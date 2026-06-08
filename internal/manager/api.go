package manager

import (
	"encoding/json"
	"html/template"
	"io"
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
	mux.HandleFunc("POST /v1/admin/leases", func(w http.ResponseWriter, r *http.Request) {
		if !checkKey(r, cfg.AdminKey) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var in struct {
			ClientIP string `json:"client_ip"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		assignment, err := m.AdminLease(strings.TrimSpace(in.ClientIP))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
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
    .draining,.pending,.checking { background:#fffaeb; color:var(--warn); }
    .unhealthy,.blocked { background:#fef3f2; color:var(--bad); }
    .disabled { background:#f2f4f7; color:#475467; }
    form { display:grid; grid-template-columns:1fr 1.2fr 1fr 1fr auto; gap:8px; padding:14px 16px; border-top:1px solid var(--line); }
    input, textarea { border:1px solid var(--line); border-radius:6px; padding:8px 10px; min-width:0; font:inherit; }
    input { height:34px; }
    textarea { width:100%; min-height:150px; resize:vertical; }
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
      <form id="clientForm" style="grid-template-columns:1fr auto;">
        <input name="client_ip" placeholder="手动添加客户端 IP，例如 192.168.2.50" required>
        <button class="primary">添加客户端</button>
      </form>
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
        <thead><tr><th><input type="checkbox" id="checkPage" onchange="togglePageSelection(this.checked)"></th><th>ID</th><th>地址</th><th>状态</th><th>出口 IP</th><th>客户端</th><th>连接数</th><th>健康详情</th><th>操作</th></tr></thead>
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
    const fmtTime = v => v ? new Date(v).toLocaleString() : '-';
    function setText(el, text) { el.textContent = text == null || text === '' ? '-' : String(text); }
    function appendPill(td, status) {
      const span = document.createElement('span');
      span.className = 'pill ' + String(status || '').replaceAll('_','-');
      span.textContent = status || '-';
      td.appendChild(span);
    }
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
      allProxies = proxies;
      document.getElementById('clientCount').textContent = clients.length;
      document.getElementById('proxyCount').textContent = proxies.length;
      document.getElementById('idleCount').textContent = proxies.filter(p => p.status === 'idle').length;
      document.getElementById('connCount').textContent = proxies.reduce((n,p) => n + (p.active_connections || 0), 0);
      document.getElementById('queueCount').textContent = (data.pending_new || []).length + (data.pending_refresh || []).length;
      const clientBody = document.getElementById('clients');
      clientBody.replaceChildren();
      clients.forEach(c => {
        const tr = document.createElement('tr');
        const ip = tr.insertCell(); setText(ip, c.client_ip);
        const status = tr.insertCell(); appendPill(status, c.status);
        const proxy = tr.insertCell(); setText(proxy, c.proxy_id);
        const proxyAddr = document.createElement('div'); proxyAddr.className = 'muted'; proxyAddr.textContent = c.proxy_address || ''; proxy.appendChild(proxyAddr);
        setText(tr.insertCell(), c.exit_ip);
        setText(tr.insertCell(), c.active_connections || 0);
        setText(tr.insertCell(), fmtTime(c.expires_at));
        const actions = tr.insertCell();
        const btn = document.createElement('button'); btn.className = 'danger'; btn.textContent = '释放'; btn.onclick = () => releaseLease(c.client_ip); actions.appendChild(btn);
        clientBody.appendChild(tr);
      });
      renderProxyPage();
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
        setText(tr.insertCell(), p.address);
        appendPill(tr.insertCell(), p.status);
        setText(tr.insertCell(), p.exit_ip);
        setText(tr.insertCell(), p.client_ip || p.draining_for);
        setText(tr.insertCell(), p.active_connections || 0);
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
