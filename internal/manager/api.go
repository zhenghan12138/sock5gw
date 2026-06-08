package manager

import (
	"encoding/json"
	"html/template"
	"net"
	"net/http"
	"strings"

	"sock5gw/internal/config"
)

func NewAPI(m *Manager, cfg config.API) http.Handler {
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
		writeJSON(w, m.Lease(clientIP(r, cfg.TrustProxy)))
	})
	mux.HandleFunc("POST /v1/lease/refresh", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.ClientKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		writeJSON(w, m.Refresh(clientIP(r, cfg.TrustProxy)))
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
	return mux
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
    .idle { background:#ecfdf3; color:var(--good); }
    .active { background:#eff8ff; color:#175cd3; }
    .draining,.pending { background:#fffaeb; color:var(--warn); }
    .unhealthy,.blocked { background:#fef3f2; color:var(--bad); }
    .disabled { background:#f2f4f7; color:#475467; }
    form { display:grid; grid-template-columns:1fr 1.2fr 1fr 1fr auto; gap:8px; padding:14px 16px; border-top:1px solid var(--line); }
    input { height:34px; border:1px solid var(--line); border-radius:6px; padding:0 10px; min-width:0; }
    button { height:32px; border:1px solid var(--line); background:#fff; border-radius:6px; padding:0 10px; cursor:pointer; }
    button.primary { background:var(--accent); border-color:var(--accent); color:#fff; }
    button.danger { color:var(--bad); }
    .token { width:320px; max-width:45vw; }
    @media (max-width:900px) { .stats { grid-template-columns:repeat(2,1fr); } form { grid-template-columns:1fr; } header { align-items:flex-start; flex-direction:column; } .token { max-width:none; width:100%; } table { font-size:12px; } }
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
      <div class="section-head"><h2>在线客户端</h2><button onclick="load()">刷新</button></div>
      <table>
        <thead><tr><th>客户端 IP</th><th>状态</th><th>代理</th><th>出口 IP</th><th>连接数</th><th>到期时间</th><th>操作</th></tr></thead>
        <tbody id="clients"></tbody>
      </table>
    </section>
    <section>
      <div class="section-head"><h2>代理池</h2><span class="muted">出口 IP 由健康检查通过 SOCKS5 获取</span></div>
      <table>
        <thead><tr><th>ID</th><th>地址</th><th>状态</th><th>出口 IP</th><th>客户端</th><th>连接数</th><th>健康详情</th><th>操作</th></tr></thead>
        <tbody id="proxies"></tbody>
      </table>
      <form id="proxyForm">
        <input name="id" placeholder="proxy-001" required>
        <input name="address" placeholder="1.2.3.4:1080" required>
        <input name="username" placeholder="用户名，可空">
        <input name="password" placeholder="密码，可空" type="password">
        <button class="primary">新增代理</button>
      </form>
    </section>
  </main>
  <script>
    const token = () => document.getElementById('token').value;
    const auth = () => ({ 'Authorization': 'Bearer ' + token(), 'Content-Type': 'application/json' });
    const pill = s => '<span class="pill '+ String(s || '').replaceAll('_','-') +'">'+ (s || '-') +'</span>';
    const fmtTime = v => v ? new Date(v).toLocaleString() : '-';
    async function api(path, opts = {}) {
      const res = await fetch(path, { ...opts, headers: { ...auth(), ...(opts.headers || {}) } });
      if (!res.ok) throw new Error(await res.text());
      if (res.status === 204) return null;
      return res.json();
    }
    async function load() {
      const data = await api('/v1/admin/status');
      const clients = data.clients || [];
      const proxies = data.proxies || [];
      document.getElementById('clientCount').textContent = clients.length;
      document.getElementById('proxyCount').textContent = proxies.length;
      document.getElementById('idleCount').textContent = proxies.filter(p => p.status === 'idle').length;
      document.getElementById('connCount').textContent = proxies.reduce((n,p) => n + (p.active_connections || 0), 0);
      document.getElementById('queueCount').textContent = (data.pending_new || []).length + (data.pending_refresh || []).length;
      document.getElementById('clients').innerHTML = clients.map(c => '<tr>'+
        '<td>'+c.client_ip+'</td><td>'+pill(c.status)+'</td><td>'+(c.proxy_id || '-')+'<div class="muted">'+(c.proxy_address || '')+'</div></td>'+
        '<td>'+(c.exit_ip || '-')+'</td><td>'+(c.active_connections || 0)+'</td><td>'+fmtTime(c.expires_at)+'</td>'+
        '<td><button class="danger" onclick="releaseLease(\''+c.client_ip+'\')">释放</button></td></tr>').join('');
      document.getElementById('proxies').innerHTML = proxies.map(p => '<tr>'+
        '<td>'+p.id+'</td><td>'+p.address+'</td><td>'+pill(p.status)+'</td><td>'+(p.exit_ip || '-')+'</td>'+
        '<td>'+(p.client_ip || p.draining_for || '-')+'</td><td>'+(p.active_connections || 0)+'</td><td class="muted">'+(p.last_health_detail || '-')+'</td>'+
        '<td><button onclick="toggleProxy(\''+p.id+'\','+(!p.disabled)+',\''+p.address+'\',\''+(p.username || '')+'\')">'+(p.disabled ? '启用' : '停用')+'</button> '+
        '<button class="danger" onclick="deleteProxy(\''+p.id+'\')">删除</button></td></tr>').join('');
    }
    async function releaseLease(ip) { await api('/v1/admin/leases/' + encodeURIComponent(ip), { method:'DELETE' }); load(); }
    async function deleteProxy(id) { await api('/v1/admin/proxies/' + encodeURIComponent(id), { method:'DELETE' }); load(); }
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
    load().catch(err => alert(err.message));
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
