package manager

import (
	"context"
	"encoding/json"
	"errors"
	"html/template"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/oschwald/geoip2-golang"

	"sock5gw/internal/config"
	"sock5gw/internal/outbound"
)

type frontProxyUpdateInput struct {
	Enabled          bool   `json:"enabled"`
	URL              string `json:"url"`
	ClearCredentials bool   `json:"clear_credentials"`
}

type frontProxyView struct {
	Enabled               bool                 `json:"enabled"`
	URL                   string               `json:"url,omitempty"`
	CredentialsConfigured bool                 `json:"credentials_configured"`
	Status                outbound.FrontStatus `json:"status"`
}

func NewAPI(m *Manager, cfg config.API, runtimeCfg *RuntimeConfig) http.Handler {
	ipGeo := newIPGeoCache(cfg.GeoIPDBPath)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = adminTemplate.Execute(w, map[string]string{"AdminKey": cfg.AdminKey})
	})
	mux.HandleFunc("POST /v1/lease", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.ClientKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		request, err := decodeOptionalDynamicLeaseRequest(r)
		if err != nil {
			writeLeaseAPIError(w, &LeaseAPIError{Code: "invalid_request", Message: err.Error(), Err: err})
			return
		}
		ip := clientIP(r, cfg.TrustProxy)
		if request != nil {
			assignment, err := m.LeaseDynamicContext(r.Context(), ip, *request, false)
			if err != nil {
				writeLeaseAPIError(w, err)
				return
			}
			writeJSON(w, assignment)
			return
		}
		writeJSON(w, m.LeaseContext(r.Context(), ip))
	})
	mux.HandleFunc("POST /v1/lease/refresh", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.ClientKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		request, err := decodeOptionalDynamicLeaseRequest(r)
		if err != nil {
			writeLeaseAPIError(w, &LeaseAPIError{Code: "invalid_request", Message: err.Error(), Err: err})
			return
		}
		ip := clientIP(r, cfg.TrustProxy)
		if request == nil {
			if current, ok := m.DynamicRequestForClient(ip); ok {
				request = &current
			}
		}
		if request != nil {
			assignment, err := m.LeaseDynamicContext(r.Context(), ip, *request, true)
			if err != nil {
				writeLeaseAPIError(w, err)
				return
			}
			writeJSON(w, assignment)
			return
		}
		writeJSON(w, m.RefreshContext(r.Context(), ip))
	})
	mux.HandleFunc("GET /v1/lease", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.ClientKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		writeJSON(w, m.Current(clientIP(r, cfg.TrustProxy)))
	})
	mux.HandleFunc("GET /v1/admin/status", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.AdminKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		writeJSON(w, m.Status())
	})
	mux.HandleFunc("GET /v1/admin/routing", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.AdminKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if runtimeCfg == nil {
			http.Error(w, "runtime config unavailable", http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, runtimeCfg.Routing())
	})
	mux.HandleFunc("PUT /v1/admin/routing", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.AdminKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if runtimeCfg == nil {
			http.Error(w, "runtime config unavailable", http.StatusServiceUnavailable)
			return
		}
		var in config.Routing
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if err := runtimeCfg.UpdateRouting(in); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, runtimeCfg.Routing())
	})
	mux.HandleFunc("GET /v1/admin/front-proxy", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.AdminKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if runtimeCfg == nil {
			http.Error(w, "runtime config unavailable", http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, newFrontProxyView(runtimeCfg.FrontProxy(), m.FrontStatus()))
	})
	mux.HandleFunc("PUT /v1/admin/front-proxy", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.AdminKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if runtimeCfg == nil {
			http.Error(w, "runtime config unavailable", http.StatusServiceUnavailable)
			return
		}
		var in frontProxyUpdateInput
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		next, err := frontProxyFromInput(runtimeCfg.FrontProxy(), in)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := runtimeCfg.UpdateFrontProxy(next); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, newFrontProxyView(runtimeCfg.FrontProxy(), m.FrontStatus()))
	})
	mux.HandleFunc("POST /v1/admin/front-proxy/test", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.AdminKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		writeJSON(w, m.TestFrontProxy(r.Context()))
	})
	mux.HandleFunc("GET /v1/admin/proxy-api", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.AdminKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if runtimeCfg == nil {
			http.Error(w, "runtime config unavailable", http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, runtimeCfg.ProxyAPI())
	})
	mux.HandleFunc("PUT /v1/admin/proxy-api", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.AdminKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if runtimeCfg == nil {
			http.Error(w, "runtime config unavailable", http.StatusServiceUnavailable)
			return
		}
		var in config.ProxyAPI
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if err := runtimeCfg.UpdateProxyAPI(in); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, runtimeCfg.ProxyAPI())
	})
	mux.HandleFunc("POST /v1/admin/proxy-api/test", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.AdminKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var in DynamicLeaseRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			writeLeaseAPIError(w, &LeaseAPIError{Code: "invalid_request", Message: "invalid json", Err: err})
			return
		}
		result, err := m.TestProxyAPI(r.Context(), in)
		if err != nil {
			writeLeaseAPIError(w, err)
			return
		}
		writeJSON(w, result)
	})
	mux.HandleFunc("POST /v1/admin/ip-geo", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.AdminKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var in struct {
			IPs []string `json:"ips"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		writeJSON(w, ipGeo.LookupMany(r.Context(), in.IPs))
	})
	mux.HandleFunc("POST /v1/admin/leases", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.AdminKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var in struct {
			ClientIP        string  `json:"client_ip"`
			Country         *string `json:"country"`
			DurationMinutes *int64  `json:"duration_minutes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		ip := strings.TrimSpace(in.ClientIP)
		if parsed := net.ParseIP(ip); parsed == nil || parsed.To4() == nil {
			http.Error(w, "valid IPv4 client_ip is required", http.StatusBadRequest)
			return
		}
		request, err := dynamicLeaseRequestFromPointers(in.Country, in.DurationMinutes)
		if err != nil {
			writeLeaseAPIError(w, &LeaseAPIError{Code: "invalid_request", Message: err.Error(), Err: err})
			return
		}
		var assignment Assignment
		if request != nil {
			assignment, err = m.LeaseDynamicContext(r.Context(), ip, *request, false)
		} else {
			assignment, err = m.AdminLeaseContext(r.Context(), ip)
		}
		if err != nil {
			writeLeaseAPIError(w, err)
			return
		}
		writeJSON(w, assignment)
	})
	mux.HandleFunc("POST /v1/admin/proxies", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.AdminKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var in ProxyInput
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		p, err := m.AddProxy(r.Context(), in)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, p)
	})
	mux.HandleFunc("POST /v1/admin/proxies/import", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.AdminKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 16*1024*1024))
		if err != nil {
			http.Error(w, "read body failed", http.StatusBadRequest)
			return
		}
		text := string(body)
		if strings.Contains(r.Header.Get("Content-Type"), "application/json") {
			var in struct {
				Text string `json:"text"`
			}
			if err := json.Unmarshal(body, &in); err != nil {
				http.Error(w, "invalid json", http.StatusBadRequest)
				return
			}
			text = in.Text
		}
		writeJSON(w, m.ImportProxies(r.Context(), text))
	})
	mux.HandleFunc("POST /v1/admin/proxies/import-url", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.AdminKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var in struct {
			URL string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		writeJSON(w, m.ImportProxiesFromURL(r.Context(), in.URL))
	})
	mux.HandleFunc("POST /v1/admin/proxies/batch/disabled", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.AdminKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var in struct {
			IDs      []string `json:"ids"`
			Disabled bool     `json:"disabled"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		writeJSON(w, m.SetProxiesDisabled(r.Context(), in.IDs, in.Disabled))
	})
	mux.HandleFunc("POST /v1/admin/proxies/batch/delete", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.AdminKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var in struct {
			IDs []string `json:"ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		writeJSON(w, m.DeleteProxies(r.Context(), in.IDs))
	})
	mux.HandleFunc("POST /v1/admin/proxies/clear", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.AdminKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		writeJSON(w, m.ClearIdleProxies(r.Context()))
	})
	mux.HandleFunc("PUT /v1/admin/proxies/{id}", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.AdminKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var in ProxyInput
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		p, err := m.UpdateProxy(r.Context(), r.PathValue("id"), in)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, p)
	})
	mux.HandleFunc("POST /v1/admin/proxies/{id}/disabled", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.AdminKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var in struct {
			Disabled bool `json:"disabled"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		p, err := m.SetProxyDisabled(r.Context(), r.PathValue("id"), in.Disabled)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, p)
	})
	mux.HandleFunc("DELETE /v1/admin/proxies/{id}", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.AdminKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if err := m.DeleteProxy(r.Context(), r.PathValue("id")); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("DELETE /v1/admin/leases/{ip}", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.AdminKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		ip := r.PathValue("ip")
		if net.ParseIP(ip) == nil {
			http.Error(w, "invalid ip", http.StatusBadRequest)
			return
		}
		m.Release(ip)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /v1/admin/leases/{ip}/refresh", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.AdminKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		ip := strings.TrimSpace(r.PathValue("ip"))
		var assignment Assignment
		var err error
		if current, ok := m.DynamicRequestForClient(ip); ok {
			assignment, err = m.LeaseDynamicContext(r.Context(), ip, current, true)
		} else {
			assignment, err = m.AdminRefreshContext(r.Context(), ip)
		}
		if err != nil {
			writeLeaseAPIError(w, err)
			return
		}
		writeJSON(w, assignment)
	})
	return mux
}

func frontProxyFromInput(current config.FrontProxy, in frontProxyUpdateInput) (config.FrontProxy, error) {
	next := current
	next.Enabled = in.Enabled
	next.FailOpen = false
	rawURL := strings.TrimSpace(in.URL)
	if rawURL == "" {
		if in.ClearCredentials {
			next.Username = ""
			next.Password = ""
		}
		if next.Enabled && strings.TrimSpace(next.Address) == "" {
			return config.FrontProxy{}, errors.New("front proxy URL is required when enabled")
		}
		if next.Enabled && strings.TrimSpace(next.Protocol) == "" {
			next.Protocol = "socks5"
		}
		return next, nil
	}

	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Opaque != "" {
		return config.FrontProxy{}, errors.New("invalid front proxy URL")
	}
	if strings.ToLower(parsed.Scheme) != "socks5" {
		return config.FrontProxy{}, errors.New("front proxy URL must use socks5://")
	}
	if parsed.Path != "" && parsed.Path != "/" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return config.FrontProxy{}, errors.New("front proxy URL must not contain a path, query, or fragment")
	}
	host, port := parsed.Hostname(), parsed.Port()
	if host == "" || port == "" {
		return config.FrontProxy{}, errors.New("front proxy URL must include host and port")
	}
	next.Protocol = "socks5"
	next.Address = net.JoinHostPort(host, port)
	switch {
	case parsed.User != nil:
		next.Username = parsed.User.Username()
		next.Password, _ = parsed.User.Password()
	case in.ClearCredentials:
		next.Username = ""
		next.Password = ""
	}
	return next, nil
}

func newFrontProxyView(front config.FrontProxy, status outbound.FrontStatus) frontProxyView {
	view := frontProxyView{
		Enabled:               front.Enabled,
		CredentialsConfigured: front.Username != "" || front.Password != "",
		Status:                status,
	}
	if strings.TrimSpace(front.Address) != "" {
		view.URL = "socks5://" + strings.TrimSpace(front.Address)
	}
	return view
}

var adminTemplate = template.Must(template.New("admin").Parse(`<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>sock5gw 管理端</title>
  <style>
    :root { color-scheme: light; --bg:#f6f7f9; --panel:#fff; --text:#172033; --muted:#667085; --line:#d8dde6; --accent:#2563eb; --bad:#b42318; --good:#067647; --warn:#b54708; }
    * { box-sizing: border-box; }
    body { margin:0; font:14px/1.45 system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; background:var(--bg); color:var(--text); }
    header { padding:18px 24px; border-bottom:1px solid var(--line); background:var(--panel); display:flex; justify-content:space-between; align-items:center; gap:16px; }
    h1 { font-size:20px; margin:0; }
    main { padding:20px 24px; display:grid; gap:18px; }
    section { background:var(--panel); border:1px solid var(--line); border-radius:8px; overflow:hidden; }
    .section-head { padding:14px 16px; border-bottom:1px solid var(--line); display:flex; justify-content:space-between; gap:12px; align-items:center; }
    h2 { font-size:16px; margin:0; }
    .stats { display:grid; grid-template-columns:repeat(5,minmax(120px,1fr)); gap:12px; }
    .stat { background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:14px; }
    .stat b { display:block; font-size:24px; margin-top:4px; }
    .muted { color:var(--muted); }
    table { width:100%; border-collapse:collapse; }
    th, td { padding:10px 12px; border-bottom:1px solid var(--line); text-align:left; vertical-align:middle; }
    th { color:var(--muted); font-weight:600; font-size:12px; background:#fafbfc; }
    tr:last-child td { border-bottom:0; }
    .pill { display:inline-flex; align-items:center; min-height:22px; padding:2px 8px; border-radius:999px; background:#eef2ff; color:#1d4ed8; font-size:12px; }
    .idle,.healthy { background:#ecfdf3; color:var(--good); }
    .active { background:#eff8ff; color:#175cd3; }
    .draining,.pending,.checking,.unknown,.half-open { background:#fffaeb; color:var(--warn); }
    .unhealthy,.blocked { background:#fef3f2; color:var(--bad); }
    .disabled { background:#f2f4f7; color:#475467; }
    form { display:grid; grid-template-columns:1fr 1.2fr 1fr 1fr auto; gap:8px; padding:14px 16px; border-top:1px solid var(--line); }
    input, textarea, select { border:1px solid var(--line); border-radius:6px; padding:8px 10px; min-width:0; font:inherit; background:#fff; color:var(--text); }
    input { height:34px; }
    input[type="checkbox"] { width:16px; height:16px; padding:0; accent-color:var(--accent); }
    textarea { width:100%; min-height:150px; resize:vertical; }
    button { height:32px; border:1px solid var(--line); background:#fff; border-radius:6px; padding:0 10px; cursor:pointer; }
    button.primary { background:var(--accent); border-color:var(--accent); color:#fff; }
    button.danger { color:var(--bad); }
    button:disabled { cursor:not-allowed; opacity:.6; }
    .token { width:320px; max-width:45vw; }
    .front-form { grid-template-columns:auto minmax(280px,1fr) auto auto; align-items:end; }
    .api-form { grid-template-columns:auto minmax(320px,1fr) 150px 150px auto; align-items:end; }
    .api-test { padding:11px 16px; border-top:1px solid var(--line); display:grid; grid-template-columns:120px 160px auto minmax(180px,1fr); gap:8px; align-items:end; }
    .front-toggle,.front-clear { min-height:34px; display:flex; align-items:center; gap:8px; white-space:nowrap; }
    .front-actions { display:flex; gap:8px; align-items:center; }
    .field { display:grid; gap:5px; color:var(--muted); font-size:12px; }
    .field input { color:var(--text); font-size:14px; }
    .config-meta { padding:11px 16px; border-top:1px solid var(--line); display:flex; flex-wrap:wrap; gap:8px 20px; align-items:center; }
    .config-meta span:last-child { margin-left:auto; }
    @media (max-width:900px) { .stats { grid-template-columns:repeat(2,1fr); } form,.front-form,.api-form,.api-test { grid-template-columns:1fr; } header { align-items:flex-start; flex-direction:column; } .token { max-width:none; width:100%; } table { font-size:12px; } .config-meta span:last-child { width:100%; margin-left:0; } }
  </style>
</head>
<body>
  <header>
    <div><h1>sock5gw 管理端</h1><div class="muted">客户端状态、出口 IP、代理池健康与启停</div></div>
    <label class="muted">Admin Token <input id="token" class="token" type="password" value="{{.AdminKey}}"></label>
  </header>
  <main>
    <div class="stats">
      <div class="stat"><span class="muted">客户端</span><b id="clientCount">0</b></div>
      <div class="stat"><span class="muted">代理总数</span><b id="proxyCount">0</b></div>
      <div class="stat"><span class="muted">空闲代理</span><b id="idleCount">0</b></div>
      <div class="stat"><span class="muted">活跃连接</span><b id="connCount">0</b></div>
      <div class="stat"><span class="muted">等待队列</span><b id="queueCount">0</b></div>
    </div>
    <section>
      <div class="section-head">
        <h2>前置代理</h2>
        <span id="frontStatus" class="pill disabled">未启用</span>
      </div>
      <form id="frontProxyForm" class="front-form">
        <label class="front-toggle"><input id="frontEnabled" type="checkbox"> 启用</label>
        <label class="field"><span>SOCKS5 链接</span><input id="frontURL" type="url" placeholder="socks5://user:password@127.0.0.1:11080" autocomplete="off" spellcheck="false"></label>
        <label class="front-clear"><input id="frontClearCredentials" type="checkbox"> 删除已保存认证</label>
        <div class="front-actions"><button id="frontTest" type="button">检测连接</button><button id="frontSave" class="primary">保存配置</button></div>
      </form>
      <div class="config-meta muted">
        <span id="frontCredentialState">认证设置：无（不发送账号密码）</span>
        <span id="frontLastCheck">最近检测：-</span>
        <span id="frontResult"></span>
      </div>
    </section>
    <section>
      <div class="section-head">
        <h2>API 代理模式</h2>
        <span id="proxyAPIStatus" class="pill disabled">未启用</span>
      </div>
      <form id="proxyAPIForm" class="api-form">
        <label class="front-toggle"><input id="proxyAPIEnabled" type="checkbox"> 启用</label>
        <label class="field"><span>供应商 API URL</span><input id="proxyAPIURL" type="url" placeholder="https://provider.example/api?num=1&amp;type=json" autocomplete="off" spellcheck="false"></label>
        <label class="field"><span>国家参数名</span><input id="proxyAPICountryParam" value="region" required></label>
        <label class="field"><span>时长参数名</span><input id="proxyAPIDurationParam" value="time" required></label>
        <button id="proxyAPISave" class="primary">保存配置</button>
      </form>
      <div class="api-test">
        <label class="field"><span>测试国家</span><input id="proxyAPITestCountry" value="Rand"></label>
        <label class="field"><span>测试时长（分钟）</span><input id="proxyAPITestDuration" type="number" min="1" step="1" value="10"></label>
        <button id="proxyAPITest" type="button">测试申请</button>
        <span id="proxyAPIResult" class="muted"></span>
      </div>
    </section>
    <section>
      <div class="section-head"><h2>在线客户端</h2><button onclick="load()">刷新</button></div>
      <table>
        <thead><tr><th>客户端 IP</th><th>状态</th><th>模式</th><th>国家</th><th>代理</th><th>出口 IP</th><th>连接数</th><th>到期时间</th><th>操作</th></tr></thead>
        <tbody id="clients"></tbody>
      </table>
      <form id="clientForm" style="grid-template-columns:1fr auto;">
        <input name="client_ip" placeholder="手动添加客户端 IP，例如 192.168.2.50" required>
        <button class="primary">添加客户端</button>
      </form>
    </section>
    <section>
      <div class="section-head"><h2>域名分流</h2><button onclick="loadRouting()">刷新配置</button></div>
      <form id="routingForm" style="grid-template-columns:120px 1fr 160px 1fr auto;">
        <label class="muted"><input id="routingEnabled" type="checkbox"> 启用</label>
        <input id="geositePath" placeholder="/etc/sock5gw/geosite.dat">
        <select id="defaultAction">
          <option value="proxy">默认代理</option>
          <option value="direct">默认直连</option>
          <option value="block">默认阻断</option>
        </select>
        <input id="routingHint" value="规则支持 geosite/domain_suffix/domain_exact/keyword/regex" readonly>
        <button class="primary">保存分流</button>
      </form>
      <div style="padding:14px 16px; border-top:1px solid var(--line); display:grid; gap:8px;">
        <textarea id="routingRules" placeholder='[
  {"type":"geosite","value":"geosite:cn","action":"direct"},
  {"type":"geosite","value":"geosite:category-ads-all","action":"block"},
  {"type":"domain_suffix","value":"google.com","action":"proxy"}
]'></textarea>
        <span id="routingResult" class="muted"></span>
      </div>
    </section>
    <section>
      <div class="section-head"><h2>代理池</h2><span class="muted">出口 IP 在分配客户端前通过 SOCKS5 检测获取</span></div>
      <div style="padding:12px 16px; border-bottom:1px solid var(--line); display:flex; flex-wrap:wrap; gap:8px; align-items:center;">
        <button onclick="batchDisable(false)">批量启用</button>
        <button onclick="batchDisable(true)">批量停用</button>
        <button class="danger" onclick="batchDelete()">批量删除</button>
        <button class="danger" onclick="clearIdleProxies()">清空未使用代理</button>
        <span class="muted" id="selectedCount">已选 0</span>
        <span style="flex:1"></span>
        <label class="muted">每页
          <select id="pageSize" onchange="setPageSize()">
            <option>25</option><option selected>50</option><option>100</option><option>200</option>
          </select>
        </label>
        <button onclick="prevPage()">上一页</button>
        <span class="muted" id="pageInfo">1 / 1</span>
        <button onclick="nextPage()">下一页</button>
      </div>
      <table>
        <thead><tr><th><input type="checkbox" id="checkPage" onchange="togglePageSelection(this.checked)"></th><th>ID</th><th>来源</th><th>国家</th><th>地址</th><th>状态</th><th>出口 IP</th><th>客户端</th><th>连接数</th><th>动态到期</th><th>健康详情</th><th>操作</th></tr></thead>
        <tbody id="proxies"></tbody>
      </table>
      <form id="proxyForm">
        <input name="id" placeholder="proxy-001" required>
        <input name="address" placeholder="1.2.3.4:1080" required>
        <input name="username" placeholder="用户名，可空">
        <input name="password" placeholder="密码，可空" type="password">
        <button class="primary">新增代理</button>
      </form>
      <div style="padding:14px 16px; border-top:1px solid var(--line); display:grid; gap:8px;">
        <textarea id="importText" placeholder="批量粘贴代理，每行一个。支持 socks5://host:port:user:pass、socks5://user:pass@host:port、host:port:user:pass"></textarea>
        <div><button class="primary" onclick="importProxies()">批量导入</button> <span id="importResult" class="muted"></span></div>
      </div>
      <div style="padding:14px 16px; border-top:1px solid var(--line); display:grid; grid-template-columns:1fr auto; gap:8px;">
        <input id="subscriptionURL" placeholder="订阅 API URL，例如 https://www.kookeey.net/pickdynamicips?...">
        <button class="primary" onclick="importSubscription()">订阅导入</button>
        <span id="subscriptionResult" class="muted"></span>
      </div>
    </section>
  </main>
  <script>
    const token = () => document.getElementById('token').value;
    const auth = () => ({ 'Authorization': 'Bearer ' + token(), 'Content-Type': 'application/json' });
    let allProxies = [];
    let selectedProxyIds = new Set();
    let proxyPage = 1;
    let ipGeo = {};
    let routingConfig = null;
    const fmtTime = v => v ? new Date(v).toLocaleString() : '-';
    function setText(el, text) { el.textContent = text == null || text === '' ? '-' : String(text); }
    function setIPText(el, ip) {
      if (!ip) { setText(el, ip); return; }
      const code = ipGeo[ip]?.country_code;
      const flag = countryFlag(code);
      el.textContent = flag ? flag + ' ' + ip : ip;
    }
    function countryFlag(code) {
      code = String(code || '').toUpperCase();
      if (!/^[A-Z]{2}$/.test(code)) return '';
      return String.fromCodePoint(...[...code].map(c => 127397 + c.charCodeAt(0)));
    }
    function appendPill(td, status) {
      const span = document.createElement('span');
      span.className = 'pill ' + String(status || '').replaceAll('_','-');
      span.textContent = status || '-';
      td.appendChild(span);
    }
    async function api(path, opts = {}) {
      const res = await fetch(path, { ...opts, headers: { ...auth(), ...(opts.headers || {}) } });
      if (!res.ok) {
        const text = (await res.text()).trim();
        try {
          const data = JSON.parse(text);
          throw new Error(data.message || data.code || text);
        } catch (err) {
          if (err instanceof SyntaxError) throw new Error(text);
          throw err;
        }
      }
      if (res.status === 204) return null;
      return res.json();
    }
    async function load() {
      const data = await api('/v1/admin/status');
      const clients = data.clients || [];
      const proxies = data.proxies || [];
      allProxies = proxies;
      await loadIPGeo(clients, proxies);
      document.getElementById('clientCount').textContent = clients.length;
      document.getElementById('proxyCount').textContent = proxies.length;
      document.getElementById('idleCount').textContent = proxies.filter(p => p.status === 'idle').length;
      document.getElementById('connCount').textContent = proxies.reduce((n,p) => n + (p.active_connections || 0), 0);
      document.getElementById('queueCount').textContent = (data.pending_new || []).length + (data.pending_refresh || []).length;
      renderFrontStatus(data.front_proxy || {});
      const clientBody = document.getElementById('clients');
      clientBody.replaceChildren();
      clients.forEach(c => {
        const tr = document.createElement('tr');
        const ip = tr.insertCell(); setText(ip, c.client_ip);
        const status = tr.insertCell(); appendPill(status, c.status);
        setText(tr.insertCell(), c.mode || 'pool');
        setText(tr.insertCell(), c.country);
        const proxy = tr.insertCell(); setText(proxy, c.proxy_id);
        const proxyAddr = document.createElement('div'); proxyAddr.className = 'muted'; proxyAddr.textContent = c.proxy_address || ''; proxy.appendChild(proxyAddr);
        setIPText(tr.insertCell(), c.exit_ip);
        setText(tr.insertCell(), c.active_connections || 0);
        setText(tr.insertCell(), fmtTime(c.expires_at));
        const actions = tr.insertCell();
        const refresh = document.createElement('button'); refresh.textContent = '刷新代理'; refresh.onclick = () => refreshLease(c.client_ip, refresh); actions.appendChild(refresh);
        actions.appendChild(document.createTextNode(' '));
        const btn = document.createElement('button'); btn.className = 'danger'; btn.textContent = '释放'; btn.onclick = () => releaseLease(c.client_ip); actions.appendChild(btn);
        clientBody.appendChild(tr);
      });
      renderProxyPage();
    }
    function renderFrontStatus(status) {
      const el = document.getElementById('frontStatus');
      const value = status.status || (status.enabled ? 'unknown' : 'disabled');
      const labels = { disabled:'未启用', unknown:'尚未验证', healthy:'连接正常', unhealthy:'连接异常', half_open:'恢复检测中' };
      el.className = 'pill ' + value.replaceAll('_', '-');
      el.textContent = labels[value] || value;
      document.getElementById('frontLastCheck').textContent = '最近检测：' + fmtTime(status.last_checked_at);
      if (status.last_error) {
        document.getElementById('frontResult').textContent = '状态详情：' + frontErrorText(status.last_error);
      }
    }
    function frontErrorText(value) {
      const messages = {
        'front proxy dial failed':'无法连接前置代理',
        'front proxy authentication failed':'前置代理认证失败',
        'front proxy handshake failed':'前置代理 SOCKS5 握手失败',
        'front proxy could not reach test target':'前置代理无法访问公网检测目标',
        'front proxy could not connect to exit':'前置代理无法连接当前出口',
        'front proxy could not connect to any candidate exit':'前置代理无法连接任何可用出口'
      };
      return messages[value] || value;
    }
    function frontTestText(data) {
      if (data.ok) return '检测通过：前置代理公网连接正常';
      const messages = {
        disabled:'前置代理未启用，无需检测',
        canceled:'检测已取消',
        inconclusive:'无法确认前置代理公网连通性，请检查后再次检测'
      };
      if (data.code === 'unhealthy') return '检测失败：' + frontErrorText(data.status?.last_error || '前置链路不可用');
      return messages[data.code] || '检测未完成';
    }
    function renderFrontConfig(data) {
      document.getElementById('frontEnabled').checked = !!data.enabled;
      document.getElementById('frontURL').value = data.url || '';
      const clear = document.getElementById('frontClearCredentials');
      clear.checked = false;
      clear.disabled = !data.credentials_configured;
      document.getElementById('frontCredentialState').textContent = data.credentials_configured ? '认证设置：已保存用户名/密码' : '认证设置：无（不发送账号密码）';
      renderFrontStatus(data.status || {});
    }
    async function loadFrontProxy() {
      const data = await api('/v1/admin/front-proxy');
      renderFrontConfig(data);
      if (data.enabled && (data.status?.status || 'unknown') === 'unknown') await testFrontProxy();
    }
    async function testFrontProxy() {
      const result = document.getElementById('frontResult');
      const button = document.getElementById('frontTest');
      button.disabled = true;
      result.textContent = '正在检测前置代理公网连通性...';
      try {
        const data = await api('/v1/admin/front-proxy/test', { method:'POST', body:'{}' });
        renderFrontStatus(data.status || {});
        result.textContent = frontTestText(data);
        return data;
      } catch (err) {
        result.textContent = '检测失败：' + err.message;
        return null;
      } finally {
        button.disabled = false;
      }
    }
    function renderProxyAPIConfig(data) {
      document.getElementById('proxyAPIEnabled').checked = !!data.enabled;
      document.getElementById('proxyAPIURL').value = data.url || '';
      document.getElementById('proxyAPICountryParam').value = data.country_param || 'region';
      document.getElementById('proxyAPIDurationParam').value = data.duration_param || 'time';
      const status = document.getElementById('proxyAPIStatus');
      status.className = 'pill ' + (data.enabled ? 'healthy' : 'disabled');
      status.textContent = data.enabled ? '已启用' : '未启用';
    }
    async function loadProxyAPI() {
      renderProxyAPIConfig(await api('/v1/admin/proxy-api'));
    }
    async function testProxyAPI() {
      if (!confirm('测试会向供应商申请 1 个代理并消耗一次额度，确认继续？')) return;
      const button = document.getElementById('proxyAPITest');
      const result = document.getElementById('proxyAPIResult');
      button.disabled = true;
      result.textContent = '正在申请并检测...';
      try {
        const data = await api('/v1/admin/proxy-api/test', {
          method:'POST',
          body:JSON.stringify({
            country:document.getElementById('proxyAPITestCountry').value.trim(),
            duration_minutes:Number(document.getElementById('proxyAPITestDuration').value)
          })
        });
        result.textContent = '检测通过：' + (data.address || '-') + (data.exit_ip ? '，出口 ' + data.exit_ip : '') + '，' + data.elapsed_ms + ' ms';
      } catch (err) {
        result.textContent = '检测失败：' + err.message;
      } finally {
        button.disabled = false;
      }
    }
    async function loadRouting() {
      routingConfig = await api('/v1/admin/routing');
      document.getElementById('routingEnabled').checked = !!routingConfig.enabled;
      document.getElementById('geositePath').value = routingConfig.geosite_path || '';
      document.getElementById('defaultAction').value = routingConfig.default_action || 'proxy';
      document.getElementById('routingRules').value = JSON.stringify(routingConfig.rules || [], null, 2);
      document.getElementById('routingResult').textContent = routingConfig.enabled ? '当前已启用' : '当前未启用';
    }
    async function loadIPGeo(clients, proxies) {
      const ips = new Set();
      clients.forEach(c => { if (c.exit_ip) ips.add(c.exit_ip); });
      proxies.forEach(p => { if (p.exit_ip) ips.add(p.exit_ip); });
      const missing = Array.from(ips).filter(ip => !ipGeo[ip]);
      if (!missing.length) return;
      try {
        ipGeo = { ...ipGeo, ...await api('/v1/admin/ip-geo', { method:'POST', body: JSON.stringify({ ips: missing }) }) };
      } catch (err) {
        console.warn('ip geo lookup failed', err);
      }
    }
    function currentPageSize() { return Number(document.getElementById('pageSize')?.value || 50); }
    function pageCount() { return Math.max(1, Math.ceil(allProxies.length / currentPageSize())); }
    function renderProxyPage() {
      proxyPage = Math.min(Math.max(1, proxyPage), pageCount());
      const size = currentPageSize();
      const start = (proxyPage - 1) * size;
      const pageItems = allProxies.slice(start, start + size);
      document.getElementById('pageInfo').textContent = proxyPage + ' / ' + pageCount();
      document.getElementById('selectedCount').textContent = '已选 ' + selectedProxyIds.size;
      document.getElementById('checkPage').checked = pageItems.length > 0 && pageItems.every(p => selectedProxyIds.has(p.id));
      const body = document.getElementById('proxies');
      body.replaceChildren();
      pageItems.forEach(p => {
        const tr = document.createElement('tr');
        const checked = tr.insertCell();
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.checked = selectedProxyIds.has(p.id);
        checkbox.onchange = () => toggleProxySelection(p.id, checkbox.checked);
        checked.appendChild(checkbox);
        setText(tr.insertCell(), p.id);
        setText(tr.insertCell(), p.source || 'pool');
        setText(tr.insertCell(), p.country);
        setText(tr.insertCell(), p.address);
        appendPill(tr.insertCell(), p.status);
        setIPText(tr.insertCell(), p.exit_ip);
        setText(tr.insertCell(), p.client_ip || p.draining_for);
        setText(tr.insertCell(), p.active_connections || 0);
        setText(tr.insertCell(), fmtTime(p.provider_expires_at));
        const detail = tr.insertCell(); detail.className = 'muted'; setText(detail, p.last_health_detail);
        const actions = tr.insertCell();
        const toggle = document.createElement('button'); toggle.textContent = p.disabled ? '启用' : '停用'; toggle.onclick = () => toggleProxy(p.id, !p.disabled); actions.appendChild(toggle);
        actions.appendChild(document.createTextNode(' '));
        const del = document.createElement('button'); del.className = 'danger'; del.textContent = '删除'; del.onclick = () => deleteProxy(p.id); actions.appendChild(del);
        body.appendChild(tr);
      });
    }
    function setPageSize() { proxyPage = 1; renderProxyPage(); }
    function prevPage() { proxyPage--; renderProxyPage(); }
    function nextPage() { proxyPage++; renderProxyPage(); }
    function toggleProxySelection(id, checked) {
      if (checked) selectedProxyIds.add(id); else selectedProxyIds.delete(id);
      renderProxyPage();
    }
    function togglePageSelection(checked) {
      const size = currentPageSize();
      const start = (proxyPage - 1) * size;
      allProxies.slice(start, start + size).forEach(p => checked ? selectedProxyIds.add(p.id) : selectedProxyIds.delete(p.id));
      renderProxyPage();
    }
    async function releaseLease(ip) { await api('/v1/admin/leases/' + encodeURIComponent(ip), { method:'DELETE' }); load(); }
    async function refreshLease(ip, btn) {
      if (btn) { btn.disabled = true; btn.textContent = '刷新中'; }
      try {
        await api('/v1/admin/leases/' + encodeURIComponent(ip) + '/refresh', { method:'POST', body:'{}' });
      } finally {
        load();
      }
    }
    async function deleteProxy(id) { await api('/v1/admin/proxies/' + encodeURIComponent(id), { method:'DELETE' }); load(); }
    async function batchDisable(disabled) {
      const ids = Array.from(selectedProxyIds);
      if (!ids.length) return alert('请先勾选代理');
      await api('/v1/admin/proxies/batch/disabled', { method:'POST', body: JSON.stringify({ ids, disabled }) });
      selectedProxyIds.clear();
      load();
    }
    async function batchDelete() {
      const ids = Array.from(selectedProxyIds);
      if (!ids.length) return alert('请先勾选代理');
      if (!confirm('确认删除选中的 '+ids.length+' 个未使用代理？使用中的代理会跳过。')) return;
      await api('/v1/admin/proxies/batch/delete', { method:'POST', body: JSON.stringify({ ids }) });
      selectedProxyIds.clear();
      load();
    }
    async function clearIdleProxies() {
      if (!confirm('确认清空所有未使用代理？正在绑定客户端或有连接的代理会跳过。')) return;
      await api('/v1/admin/proxies/clear', { method:'POST', body:'{}' });
      selectedProxyIds.clear();
      load();
    }
    async function toggleProxy(id, disabled) {
      await api('/v1/admin/proxies/' + encodeURIComponent(id) + '/disabled', { method:'POST', body: JSON.stringify({ disabled }) });
      load();
    }
    document.getElementById('proxyForm').addEventListener('submit', async e => {
      e.preventDefault();
      const fd = new FormData(e.currentTarget);
      await api('/v1/admin/proxies', { method:'POST', body: JSON.stringify(Object.fromEntries(fd.entries())) });
      e.currentTarget.reset();
      load();
    });
    document.getElementById('clientForm').addEventListener('submit', async e => {
      e.preventDefault();
      const fd = new FormData(e.currentTarget);
      await api('/v1/admin/leases', { method:'POST', body: JSON.stringify(Object.fromEntries(fd.entries())) });
      e.currentTarget.reset();
      load();
    });
    document.getElementById('routingForm').addEventListener('submit', async e => {
      e.preventDefault();
      const el = document.getElementById('routingResult');
      el.textContent = '保存中...';
      try {
        const rules = JSON.parse(document.getElementById('routingRules').value || '[]');
        const next = {
          enabled: document.getElementById('routingEnabled').checked,
          geosite_path: document.getElementById('geositePath').value.trim(),
          default_action: document.getElementById('defaultAction').value,
          rules
        };
        routingConfig = await api('/v1/admin/routing', { method:'PUT', body: JSON.stringify(next) });
        el.textContent = '已保存并热更新';
      } catch (err) {
        el.textContent = '保存失败：' + err.message;
      }
    });
    document.getElementById('frontProxyForm').addEventListener('submit', async e => {
      e.preventDefault();
      const result = document.getElementById('frontResult');
      const save = document.getElementById('frontSave');
      save.disabled = true;
      result.textContent = '保存中...';
      try {
        const next = await api('/v1/admin/front-proxy', {
          method:'PUT',
          body:JSON.stringify({
            enabled:document.getElementById('frontEnabled').checked,
            url:document.getElementById('frontURL').value.trim(),
            clear_credentials:document.getElementById('frontClearCredentials').checked
          })
        });
        renderFrontConfig(next);
        if (next.enabled) {
          result.textContent = '配置已保存，正在检测连接...';
          await testFrontProxy();
        } else {
          result.textContent = '配置已保存，前置代理已停用';
        }
        load().catch(console.error);
      } catch (err) {
        result.textContent = '保存失败：' + err.message;
      } finally {
        save.disabled = false;
      }
    });
    document.getElementById('frontTest').addEventListener('click', testFrontProxy);
    document.getElementById('proxyAPIForm').addEventListener('submit', async e => {
      e.preventDefault();
      const save = document.getElementById('proxyAPISave');
      const result = document.getElementById('proxyAPIResult');
      save.disabled = true;
      result.textContent = '保存中...';
      try {
        const next = await api('/v1/admin/proxy-api', {
          method:'PUT',
          body:JSON.stringify({
            enabled:document.getElementById('proxyAPIEnabled').checked,
            url:document.getElementById('proxyAPIURL').value.trim(),
            country_param:document.getElementById('proxyAPICountryParam').value.trim(),
            duration_param:document.getElementById('proxyAPIDurationParam').value.trim()
          })
        });
        renderProxyAPIConfig(next);
        result.textContent = '配置已保存';
      } catch (err) {
        result.textContent = '保存失败：' + err.message;
      } finally {
        save.disabled = false;
      }
    });
    document.getElementById('proxyAPITest').addEventListener('click', testProxyAPI);
    async function importProxies() {
      const text = document.getElementById('importText').value;
      const el = document.getElementById('importResult');
      el.textContent = '导入中...';
      try {
        const result = await api('/v1/admin/proxies/import', { method:'POST', body:text, headers:{'Content-Type':'text/plain'} });
        el.textContent = '导入 '+result.imported+'，跳过 '+result.skipped+(result.errors?.length ? '，错误 '+result.errors.length+': '+result.errors.slice(0,2).join(' | ') : '');
        if (!result.errors?.length) document.getElementById('importText').value = '';
        load();
      } catch (err) {
        el.textContent = '导入失败：' + err.message;
      }
    }
    async function importSubscription() {
      const url = document.getElementById('subscriptionURL').value;
      const el = document.getElementById('subscriptionResult');
      el.textContent = '订阅拉取中...';
      try {
        const result = await api('/v1/admin/proxies/import-url', { method:'POST', body: JSON.stringify({ url }) });
        el.textContent = '导入 '+result.imported+'，跳过 '+result.skipped+(result.errors?.length ? '，错误 '+result.errors.length+': '+result.errors.slice(0,2).join(' | ') : '');
        load();
      } catch (err) {
        el.textContent = '订阅导入失败：' + err.message;
      }
    }
    load().catch(err => alert(err.message));
    loadFrontProxy().catch(err => {
      document.getElementById('frontResult').textContent = '加载失败：' + err.message;
    });
    loadProxyAPI().catch(err => {
      document.getElementById('proxyAPIResult').textContent = '加载失败：' + err.message;
    });
    loadRouting().catch(console.error);
    setInterval(() => load().catch(console.error), 5000);
  </script>
</body>
</html>`))

func checkKey(r *http.Request, want string) bool {
	got := r.Header.Get("Authorization")
	got = strings.TrimPrefix(got, "Bearer ")
	return got == want
}

func clientIP(r *http.Request, trustProxy bool) string {
	if trustProxy {
		if v := r.Header.Get("X-Forwarded-For"); v != "" {
			return strings.TrimSpace(strings.Split(v, ",")[0])
		}
		if v := r.Header.Get("X-Real-IP"); v != "" {
			return strings.TrimSpace(v)
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func decodeOptionalDynamicLeaseRequest(r *http.Request) (*DynamicLeaseRequest, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 64*1024+1))
	if err != nil {
		return nil, errors.New("read request body failed")
	}
	if len(body) > 64*1024 {
		return nil, errors.New("request body exceeds 64 KiB")
	}
	if strings.TrimSpace(string(body)) == "" {
		return nil, nil
	}
	var in struct {
		Country         *string `json:"country"`
		DurationMinutes *int64  `json:"duration_minutes"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&in); err != nil {
		return nil, errors.New("invalid json")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, errors.New("invalid json")
	}
	return dynamicLeaseRequestFromPointers(in.Country, in.DurationMinutes)
}

func dynamicLeaseRequestFromPointers(country *string, durationMinutes *int64) (*DynamicLeaseRequest, error) {
	if country == nil && durationMinutes == nil {
		return nil, nil
	}
	if country == nil || durationMinutes == nil {
		return nil, errors.New("country and duration_minutes must be provided together")
	}
	request, err := NormalizeDynamicLeaseRequest(DynamicLeaseRequest{Country: *country, DurationMinutes: *durationMinutes})
	if err != nil {
		return nil, err
	}
	return &request, nil
}

func writeLeaseAPIError(w http.ResponseWriter, err error) {
	var apiErr *LeaseAPIError
	if !errors.As(err, &apiErr) {
		apiErr = &LeaseAPIError{Code: "internal_error", Message: "internal server error", Err: err}
	}
	status := http.StatusBadGateway
	switch apiErr.Code {
	case "invalid_request":
		status = http.StatusBadRequest
	case "proxy_api_disabled":
		status = http.StatusServiceUnavailable
	case "internal_error":
		status = http.StatusInternalServerError
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(apiErr)
}

type ipGeoInfo struct {
	CountryCode string `json:"country_code,omitempty"`
}

type ipGeoEntry struct {
	info      ipGeoInfo
	expiresAt time.Time
}

type ipGeoCache struct {
	mu      sync.Mutex
	db      *geoip2.Reader
	entries map[string]ipGeoEntry
}

func newIPGeoCache(path string) *ipGeoCache {
	cache := &ipGeoCache{
		entries: map[string]ipGeoEntry{},
	}
	if strings.TrimSpace(path) == "" {
		return cache
	}
	db, err := geoip2.Open(path)
	if err != nil {
		slog.Warn("geoip database unavailable", "path", path, "err", err)
		return cache
	}
	cache.db = db
	slog.Info("geoip database loaded", "path", path)
	return cache
}

func (c *ipGeoCache) LookupMany(ctx context.Context, ips []string) map[string]ipGeoInfo {
	out := map[string]ipGeoInfo{}
	for _, ip := range uniqueIPs(ips) {
		if info := c.Lookup(ctx, ip); info.CountryCode != "" {
			out[ip] = info
		}
	}
	return out
}

func (c *ipGeoCache) Lookup(ctx context.Context, ip string) ipGeoInfo {
	_ = ctx
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return ipGeoInfo{}
	}
	now := time.Now()
	c.mu.Lock()
	if entry, ok := c.entries[ip]; ok && now.Before(entry.expiresAt) {
		c.mu.Unlock()
		return entry.info
	}
	c.mu.Unlock()

	info := c.lookup(parsed)
	c.mu.Lock()
	c.entries[ip] = ipGeoEntry{info: info, expiresAt: now.Add(24 * time.Hour)}
	c.mu.Unlock()
	return info
}

func (c *ipGeoCache) lookup(ip net.IP) ipGeoInfo {
	if c.db == nil {
		return ipGeoInfo{}
	}
	record, err := c.db.Country(ip)
	if err != nil {
		return ipGeoInfo{}
	}
	if validCountryCode(record.Country.IsoCode) {
		return ipGeoInfo{CountryCode: strings.ToUpper(record.Country.IsoCode)}
	}
	return ipGeoInfo{}
}

func validCountryCode(code string) bool {
	code = strings.ToUpper(strings.TrimSpace(code))
	if len(code) != 2 {
		return false
	}
	for _, r := range code {
		if r < 'A' || r > 'Z' {
			return false
		}
	}
	return true
}

func uniqueIPs(ips []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(ips))
	for _, ip := range ips {
		ip = strings.TrimSpace(ip)
		if ip == "" || seen[ip] || net.ParseIP(ip) == nil {
			continue
		}
		seen[ip] = true
		out = append(out, ip)
	}
	return out
}
