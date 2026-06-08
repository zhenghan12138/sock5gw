package manager

import (
	"bufio"
	"context"
	"crypto/sha1"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"sock5gw/internal/config"
	"sock5gw/internal/store"
)

const (
	LeaseActive  = "active"
	LeasePending = "pending"
	LeaseBlocked = "blocked"

	ProxyIdle      = "idle"
	ProxyChecking  = "checking"
	ProxyActive    = "active"
	ProxyDraining  = "draining"
	ProxyUnhealthy = "unhealthy"
	ProxyDisabled  = "disabled"
)

var ErrNoLease = errors.New("no active lease")

type proxyProbeFunc func(context.Context, Proxy) (string, error)

type Proxy struct {
	ID               string `json:"id"`
	Address          string `json:"address"`
	Username         string `json:"-"`
	Password         string `json:"-"`
	Status           string `json:"status"`
	ClientIP         string `json:"client_ip,omitempty"`
	DrainingFor      string `json:"draining_for,omitempty"`
	ActiveConns      int    `json:"active_connections"`
	FailureCount     int    `json:"failure_count"`
	SuccessCount     int    `json:"success_count"`
	LastHealthDetail string `json:"last_health_detail,omitempty"`
	ExitIP           string `json:"exit_ip,omitempty"`
	Disabled         bool   `json:"disabled"`
}

type LeaseView struct {
	ClientIP  string    `json:"client_ip"`
	ProxyID   string    `json:"proxy_id,omitempty"`
	Status    string    `json:"status"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
	QueuedAt  time.Time `json:"queued_at,omitempty"`
}

type Assignment struct {
	ClientIP  string    `json:"client_ip"`
	ProxyID   string    `json:"proxy_id,omitempty"`
	Status    string    `json:"status"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
}

type ProxyInput struct {
	ID       string `json:"id"`
	Address  string `json:"address"`
	Username string `json:"username"`
	Password string `json:"password"`
	Disabled bool   `json:"disabled"`
}

type ClientView struct {
	ClientIP          string    `json:"client_ip"`
	Status            string    `json:"status"`
	ProxyID           string    `json:"proxy_id,omitempty"`
	ProxyAddress      string    `json:"proxy_address,omitempty"`
	ExitIP            string    `json:"exit_ip,omitempty"`
	ActiveConnections int       `json:"active_connections"`
	ExpiresAt         time.Time `json:"expires_at,omitempty"`
	Queued            bool      `json:"queued"`
}

type ImportResult struct {
	Imported int      `json:"imported"`
	Skipped  int      `json:"skipped"`
	Errors   []string `json:"errors,omitempty"`
}

type BatchResult struct {
	Updated int      `json:"updated"`
	Deleted int      `json:"deleted"`
	Skipped int      `json:"skipped"`
	Errors  []string `json:"errors,omitempty"`
}

type Manager struct {
	cfg *config.Config
	db  *store.DB

	mu          sync.Mutex
	proxies     map[string]*Proxy
	proxyOrder  []string
	leases      map[string]*LeaseView
	pendingNew  []string
	pendingRefs []string
	conns       map[string]map[net.Conn]struct{}
	fake        *FakeIPStore
	probeProxy  proxyProbeFunc
	queueActive bool
}

func New(cfg *config.Config, db *store.DB) (*Manager, error) {
	m := &Manager{
		cfg:     cfg,
		db:      db,
		proxies: map[string]*Proxy{},
		leases:  map[string]*LeaseView{},
		conns:   map[string]map[net.Conn]struct{}{},
	}
	m.probeProxy = m.defaultProbeProxy
	fake, err := NewFakeIPStore(cfg.DNS.FakeIPCIDR)
	if err != nil {
		return nil, err
	}
	m.fake = fake
	for _, p := range cfg.Proxies {
		if err := db.UpsertProxy(context.Background(), store.Proxy{
			ID:       p.ID,
			Address:  p.Address,
			Username: p.Username,
			Password: p.Password,
		}); err != nil {
			return nil, err
		}
	}
	storedProxies, err := db.ListProxies(context.Background())
	if err != nil {
		return nil, err
	}
	for _, p := range storedProxies {
		status := ProxyIdle
		if p.Disabled {
			status = ProxyDisabled
		}
		m.proxies[p.ID] = &Proxy{
			ID:       p.ID,
			Address:  p.Address,
			Username: p.Username,
			Password: p.Password,
			Status:   status,
			Disabled: p.Disabled,
		}
		m.proxyOrder = append(m.proxyOrder, p.ID)
	}
	leases, err := db.ListLeases(context.Background())
	if err != nil {
		return nil, err
	}
	now := time.Now()
	for _, l := range leases {
		p := m.proxies[l.ProxyID]
		if p == nil || l.ExpiresAt.Before(now) {
			continue
		}
		p.Status = ProxyActive
		p.ClientIP = l.ClientIP
		m.leases[l.ClientIP] = &LeaseView{
			ClientIP:  l.ClientIP,
			ProxyID:   l.ProxyID,
			Status:    LeaseActive,
			ExpiresAt: l.ExpiresAt,
		}
	}
	return m, nil
}

func (m *Manager) Start(ctx context.Context) {
	go m.expiryLoop(ctx)
	if m.cfg.HealthCheck.Enabled {
		go m.healthLoop(ctx)
	}
}

func (m *Manager) FakeIPStore() *FakeIPStore {
	return m.fake
}

func (m *Manager) Lease(clientIP string) Assignment {
	return m.ensureLease(context.Background(), clientIP, false)
}

func (m *Manager) AdminLease(clientIP string) (Assignment, error) {
	if ip := net.ParseIP(clientIP); ip == nil || ip.To4() == nil {
		return Assignment{}, errors.New("valid IPv4 client_ip is required")
	}
	return m.ensureLease(context.Background(), clientIP, false), nil
}

func (m *Manager) Refresh(clientIP string) Assignment {
	return m.ensureLease(context.Background(), clientIP, true)
}

func (m *Manager) Current(clientIP string) Assignment {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.inQueueLocked(m.pendingNew, clientIP) {
		return Assignment{ClientIP: clientIP, Status: LeasePending}
	}
	if l := m.leases[clientIP]; l != nil {
		status := l.Status
		if m.inQueueLocked(m.pendingRefs, clientIP) {
			status = LeasePending
		}
		return Assignment{ClientIP: clientIP, ProxyID: l.ProxyID, Status: status, ExpiresAt: l.ExpiresAt}
	}
	return Assignment{ClientIP: clientIP, Status: LeaseBlocked}
}

func (m *Manager) ResolveProxy(clientIP string) (*Proxy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := m.leases[clientIP]
	if l == nil || l.Status != LeaseActive || time.Now().After(l.ExpiresAt) {
		return nil, ErrNoLease
	}
	p := m.proxies[l.ProxyID]
	if p == nil || p.Status == ProxyUnhealthy {
		return nil, ErrNoLease
	}
	cp := *p
	return &cp, nil
}

func (m *Manager) RegisterConn(clientIP, proxyID string, conn net.Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.conns[clientIP] == nil {
		m.conns[clientIP] = map[net.Conn]struct{}{}
	}
	m.conns[clientIP][conn] = struct{}{}
	if p := m.proxies[proxyID]; p != nil {
		p.ActiveConns++
	}
}

func (m *Manager) UnregisterConn(clientIP, proxyID string, conn net.Conn) {
	m.mu.Lock()
	wakeQueue := false
	if set := m.conns[clientIP]; set != nil {
		delete(set, conn)
		if len(set) == 0 {
			delete(m.conns, clientIP)
		}
	}
	if p := m.proxies[proxyID]; p != nil && p.ActiveConns > 0 {
		p.ActiveConns--
		if p.Status == ProxyDraining && p.ActiveConns == 0 {
			p.Status = ProxyIdle
			p.ClientIP = ""
			p.DrainingFor = ""
			wakeQueue = true
		}
	}
	m.mu.Unlock()
	if wakeQueue {
		m.processQueueAsync()
	}
}

func (m *Manager) CloseClientConnections(clientIP string) {
	m.mu.Lock()
	var toClose []net.Conn
	for ip, set := range m.conns {
		if clientIP != "" && ip != clientIP {
			continue
		}
		for conn := range set {
			toClose = append(toClose, conn)
		}
	}
	m.mu.Unlock()
	for _, conn := range toClose {
		_ = conn.Close()
	}
}

func (m *Manager) Status() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	proxies := make([]Proxy, 0, len(m.proxies))
	for _, id := range m.proxyOrder {
		if p := m.proxies[id]; p != nil {
			proxies = append(proxies, *p)
		}
	}
	leases := make([]LeaseView, 0, len(m.leases))
	for _, l := range m.leases {
		leases = append(leases, *l)
	}
	return map[string]any{
		"proxies":         proxies,
		"clients":         m.clientViewsLocked(),
		"leases":          leases,
		"pending_new":     append([]string(nil), m.pendingNew...),
		"pending_refresh": append([]string(nil), m.pendingRefs...),
	}
}

func (m *Manager) AddProxy(ctx context.Context, in ProxyInput) (*Proxy, error) {
	if in.ID == "" {
		return nil, errors.New("id is required")
	}
	if _, _, err := net.SplitHostPort(in.Address); err != nil {
		return nil, fmt.Errorf("address: %w", err)
	}
	m.mu.Lock()
	if _, exists := m.proxies[in.ID]; exists {
		m.mu.Unlock()
		return nil, errors.New("proxy already exists")
	}
	status := ProxyIdle
	if in.Disabled {
		status = ProxyDisabled
	}
	p := &Proxy{
		ID:       in.ID,
		Address:  in.Address,
		Username: in.Username,
		Password: in.Password,
		Status:   status,
		Disabled: in.Disabled,
	}
	if err := m.db.UpsertProxy(ctx, store.Proxy{ID: in.ID, Address: in.Address, Username: in.Username, Password: in.Password, Disabled: in.Disabled}); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	m.proxies[p.ID] = p
	m.proxyOrder = append(m.proxyOrder, p.ID)
	sort.Strings(m.proxyOrder)
	cp := *p
	m.mu.Unlock()
	m.processQueueAsync()
	return &cp, nil
}

func (m *Manager) ImportProxies(ctx context.Context, text string) ImportResult {
	result := ImportResult{}
	scanner := bufio.NewScanner(strings.NewReader(text))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		in, err := parseProxyLine(line)
		if err != nil {
			result.Skipped++
			result.Errors = append(result.Errors, fmt.Sprintf("line %d: %v", lineNo, err))
			continue
		}
		if _, err := m.AddProxy(ctx, in); err != nil {
			if strings.Contains(err.Error(), "already exists") {
				result.Skipped++
				continue
			}
			result.Skipped++
			result.Errors = append(result.Errors, fmt.Sprintf("line %d: %v", lineNo, err))
			continue
		}
		result.Imported++
	}
	if err := scanner.Err(); err != nil {
		result.Errors = append(result.Errors, err.Error())
	}
	return result
}

func (m *Manager) ImportProxiesFromURL(ctx context.Context, rawURL string) ImportResult {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return ImportResult{Skipped: 1, Errors: []string{"url is required"}}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return ImportResult{Skipped: 1, Errors: []string{err.Error()}}
	}
	client := subscriptionHTTPClient()
	resp, err := client.Do(req)
	if err != nil {
		return ImportResult{Skipped: 1, Errors: []string{err.Error()}}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32*1024*1024))
	if err != nil {
		return ImportResult{Skipped: 1, Errors: []string{err.Error()}}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ImportResult{Skipped: 1, Errors: []string{fmt.Sprintf("subscription returned HTTP %d", resp.StatusCode)}}
	}
	return m.ImportProxyPayload(ctx, body)
}

func subscriptionHTTPClient() *http.Client {
	dialer := &net.Dialer{
		Timeout: 15 * time.Second,
	}
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				host, port, err := net.SplitHostPort(address)
				if err != nil || net.ParseIP(host) != nil {
					return dialer.DialContext(ctx, network, address)
				}
				ips, err := resolveAOverDoH(ctx, host)
				if err != nil {
					return nil, err
				}
				var lastErr error
				for _, ip := range ips {
					conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
					if err == nil {
						return conn, nil
					}
					lastErr = err
				}
				return nil, lastErr
			},
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 20 * time.Second,
			IdleConnTimeout:       30 * time.Second,
		},
	}
}

func resolveAOverDoH(ctx context.Context, host string) ([]net.IP, error) {
	endpoint := "https://cloudflare-dns.com/dns-query?name=" + url.QueryEscape(host) + "&type=A"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/dns-json")
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				dialer := net.Dialer{Timeout: 5 * time.Second}
				return dialer.DialContext(ctx, network, "1.1.1.1:443")
			},
			TLSClientConfig:       &tls.Config{ServerName: "cloudflare-dns.com", MinVersion: tls.VersionTLS12},
			TLSHandshakeTimeout:   5 * time.Second,
			ResponseHeaderTimeout: 5 * time.Second,
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("doh returned HTTP %d", resp.StatusCode)
	}
	var payload struct {
		Answer []struct {
			Type int    `json:"type"`
			Data string `json:"data"`
		} `json:"Answer"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1024*1024)).Decode(&payload); err != nil {
		return nil, err
	}
	var ips []net.IP
	for _, answer := range payload.Answer {
		if answer.Type != 1 {
			continue
		}
		if ip := net.ParseIP(answer.Data); ip != nil && ip.To4() != nil {
			ips = append(ips, ip)
		}
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("no A records for %s", host)
	}
	return ips, nil
}

func DialProxy(ctx context.Context, p Proxy, timeout time.Duration) (net.Conn, error) {
	dialer := net.Dialer{Timeout: timeout}
	host, port, err := net.SplitHostPort(p.Address)
	if err != nil {
		return nil, err
	}
	if net.ParseIP(host) != nil {
		return dialer.DialContext(ctx, "tcp", p.Address)
	}
	ips, err := resolveAOverDoH(ctx, host)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for _, ip := range ips {
		conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(ip.String(), port))
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("no usable A records for %s", host)
}

func (m *Manager) ImportProxyPayload(ctx context.Context, body []byte) ImportResult {
	var payload struct {
		Success bool `json:"success"`
		Data    []struct {
			IP       string `json:"ip"`
			Host     string `json:"host"`
			Port     any    `json:"port"`
			Username string `json:"username"`
			Password string `json:"password"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err == nil && len(payload.Data) > 0 {
		result := ImportResult{}
		for i, item := range payload.Data {
			host := strings.TrimSpace(item.IP)
			if host == "" {
				host = strings.TrimSpace(item.Host)
			}
			port := fmt.Sprint(item.Port)
			if f, ok := item.Port.(float64); ok {
				port = strconv.Itoa(int(f))
			}
			if host == "" || port == "" {
				result.Skipped++
				result.Errors = append(result.Errors, fmt.Sprintf("item %d: host and port are required", i+1))
				continue
			}
			in := ProxyInput{
				Address:  net.JoinHostPort(host, port),
				Username: item.Username,
				Password: item.Password,
			}
			in.ID = proxyID(in)
			if _, err := m.AddProxy(ctx, in); err != nil {
				if strings.Contains(err.Error(), "already exists") {
					result.Skipped++
					continue
				}
				result.Skipped++
				result.Errors = append(result.Errors, fmt.Sprintf("item %d: %v", i+1, err))
				continue
			}
			result.Imported++
		}
		return result
	}
	return m.ImportProxies(ctx, string(body))
}

func (m *Manager) SetProxiesDisabled(ctx context.Context, ids []string, disabled bool) BatchResult {
	result := BatchResult{}
	for _, id := range uniqueStrings(ids) {
		if _, err := m.SetProxyDisabled(ctx, id, disabled); err != nil {
			result.Skipped++
			result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", id, err))
			continue
		}
		result.Updated++
	}
	return result
}

func (m *Manager) DeleteProxies(ctx context.Context, ids []string) BatchResult {
	result := BatchResult{}
	for _, id := range uniqueStrings(ids) {
		if err := m.DeleteProxy(ctx, id); err != nil {
			result.Skipped++
			result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", id, err))
			continue
		}
		result.Deleted++
	}
	return result
}

func (m *Manager) ClearIdleProxies(ctx context.Context) BatchResult {
	m.mu.Lock()
	ids := make([]string, 0, len(m.proxyOrder))
	for _, id := range m.proxyOrder {
		p := m.proxies[id]
		if p == nil {
			continue
		}
		if p.Status == ProxyActive || p.Status == ProxyChecking || p.Status == ProxyDraining || p.ActiveConns > 0 {
			continue
		}
		ids = append(ids, id)
	}
	m.mu.Unlock()
	return m.DeleteProxies(ctx, ids)
}

func (m *Manager) proxyCheckSnapshot(id string) (Proxy, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.proxies[id]
	if p == nil || p.Disabled || p.Status == ProxyDisabled || p.Status == ProxyChecking {
		return Proxy{}, false
	}
	return *p, true
}

func (m *Manager) UpdateProxy(ctx context.Context, id string, in ProxyInput) (*Proxy, error) {
	if _, _, err := net.SplitHostPort(in.Address); err != nil {
		return nil, fmt.Errorf("address: %w", err)
	}
	m.mu.Lock()
	p := m.proxies[id]
	if p == nil {
		m.mu.Unlock()
		return nil, errors.New("proxy not found")
	}
	p.Address = in.Address
	p.Username = in.Username
	p.Password = in.Password
	p.Disabled = in.Disabled
	if in.Disabled && p.Status == ProxyIdle {
		p.Status = ProxyDisabled
	} else if p.Status == ProxyDisabled {
		p.Status = ProxyIdle
	}
	if err := m.db.UpsertProxy(ctx, store.Proxy{ID: id, Address: p.Address, Username: p.Username, Password: p.Password, Disabled: p.Disabled}); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	cp := *p
	m.mu.Unlock()
	m.processQueueAsync()
	return &cp, nil
}

func (m *Manager) SetProxyDisabled(ctx context.Context, id string, disabled bool) (*Proxy, error) {
	m.mu.Lock()
	p := m.proxies[id]
	if p == nil {
		m.mu.Unlock()
		return nil, errors.New("proxy not found")
	}
	p.Disabled = disabled
	if disabled && p.Status == ProxyIdle {
		p.Status = ProxyDisabled
	}
	if !disabled && p.Status == ProxyDisabled {
		p.Status = ProxyIdle
	}
	if err := m.db.UpsertProxy(ctx, store.Proxy{ID: p.ID, Address: p.Address, Username: p.Username, Password: p.Password, Disabled: p.Disabled}); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	cp := *p
	m.mu.Unlock()
	m.processQueueAsync()
	return &cp, nil
}

func (m *Manager) DeleteProxy(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.proxies[id]
	if p == nil {
		return errors.New("proxy not found")
	}
	if p.Status == ProxyActive || p.Status == ProxyChecking || p.Status == ProxyDraining || p.ActiveConns > 0 {
		return errors.New("proxy is in use")
	}
	if err := m.db.DeleteProxy(ctx, id); err != nil {
		return err
	}
	delete(m.proxies, id)
	m.proxyOrder = removeIP(m.proxyOrder, id)
	return nil
}

func (m *Manager) Release(clientIP string) {
	m.mu.Lock()
	m.releaseLocked(context.Background(), clientIP, true)
	m.mu.Unlock()
	m.processQueueAsync()
}

func (m *Manager) ensureLease(ctx context.Context, clientIP string, refresh bool) Assignment {
	for {
		proxy, pending := m.reserveProxyForLease(ctx, clientIP, refresh)
		if proxy == nil {
			return pending
		}
		exitIP, err := m.probeProxy(ctx, *proxy)
		m.mu.Lock()
		current := m.proxies[proxy.ID]
		if current == nil || current.Status != ProxyChecking || current.ClientIP != clientIP {
			m.mu.Unlock()
			continue
		}
		if current.Disabled {
			current.Status = ProxyDisabled
			current.ClientIP = ""
			current.DrainingFor = ""
			m.mu.Unlock()
			continue
		}
		if err != nil {
			m.markProbeFailedLocked(ctx, current, err)
			m.mu.Unlock()
			continue
		}
		assignment := m.activateCheckedProxyLocked(ctx, current, clientIP, refresh, exitIP)
		m.mu.Unlock()
		return assignment
	}
}

func (m *Manager) reserveProxyForLease(ctx context.Context, clientIP string, refresh bool) (*Proxy, Assignment) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	if l := m.leases[clientIP]; l != nil && l.Status == LeaseActive && now.After(l.ExpiresAt) {
		m.releaseLocked(ctx, clientIP, true)
	}
	if !refresh {
		if l := m.leases[clientIP]; l != nil && l.Status == LeaseActive {
			return nil, Assignment{ClientIP: clientIP, ProxyID: l.ProxyID, Status: LeaseActive, ExpiresAt: l.ExpiresAt}
		}
	}
	proxy := m.takeIdleProxyLocked()
	if proxy == nil {
		if refresh {
			m.enqueueUniqueLocked(&m.pendingRefs, clientIP)
			if l := m.leases[clientIP]; l != nil && l.Status == LeaseActive {
				m.db.AddEvent(ctx, clientIP, "", "pending", fmt.Sprintf("refresh=%v", refresh))
				return nil, Assignment{ClientIP: clientIP, ProxyID: l.ProxyID, Status: LeasePending, ExpiresAt: l.ExpiresAt}
			}
		} else {
			m.enqueueUniqueLocked(&m.pendingNew, clientIP)
		}
		m.db.AddEvent(ctx, clientIP, "", "pending", fmt.Sprintf("refresh=%v", refresh))
		return nil, Assignment{ClientIP: clientIP, Status: LeasePending}
	}
	proxy.Status = ProxyChecking
	proxy.ClientIP = clientIP
	proxy.DrainingFor = ""
	cp := *proxy
	return &cp, Assignment{}
}

func (m *Manager) activateCheckedProxyLocked(ctx context.Context, proxy *Proxy, clientIP string, refresh bool, exitIP string) Assignment {
	now := time.Now()
	expires := now.Add(m.cfg.LeaseTTL.Duration)
	if refresh {
		if old := m.leases[clientIP]; old != nil && old.ProxyID != "" {
			if oldProxy := m.proxies[old.ProxyID]; oldProxy != nil {
				if oldProxy.ActiveConns > 0 {
					oldProxy.Status = ProxyDraining
					oldProxy.ClientIP = ""
					oldProxy.DrainingFor = clientIP
				} else {
					m.markProxyIdleLocked(oldProxy)
				}
			}
		}
	}
	proxy.Status = ProxyActive
	proxy.ClientIP = clientIP
	proxy.DrainingFor = ""
	proxy.FailureCount = 0
	proxy.SuccessCount++
	proxy.LastHealthDetail = ""
	if exitIP != "" {
		proxy.ExitIP = exitIP
	}
	m.removeQueuedLocked(clientIP)
	m.leases[clientIP] = &LeaseView{ClientIP: clientIP, ProxyID: proxy.ID, Status: LeaseActive, ExpiresAt: expires}
	_ = m.db.UpsertLease(ctx, store.Lease{ClientIP: clientIP, ProxyID: proxy.ID, Status: LeaseActive, ExpiresAt: expires, UpdatedAt: now})
	m.db.AddEvent(ctx, clientIP, proxy.ID, "assigned", fmt.Sprintf("refresh=%v", refresh))
	return Assignment{ClientIP: clientIP, ProxyID: proxy.ID, Status: LeaseActive, ExpiresAt: expires}
}

func (m *Manager) markProbeFailedLocked(ctx context.Context, p *Proxy, err error) {
	p.FailureCount++
	p.SuccessCount = 0
	p.LastHealthDetail = err.Error()
	p.Status = ProxyUnhealthy
	p.ClientIP = ""
	p.DrainingFor = ""
	m.db.AddEvent(ctx, "", p.ID, "proxy_unhealthy", err.Error())
	slog.Warn("proxy failed assignment probe", "proxy_id", p.ID, "err", err)
}

func (m *Manager) processQueueAsync() {
	m.mu.Lock()
	if m.queueActive {
		m.mu.Unlock()
		return
	}
	m.queueActive = true
	m.mu.Unlock()
	go func() {
		m.processQueues(context.Background())
		m.mu.Lock()
		m.queueActive = false
		shouldRestart := m.hasQueuedLocked() && m.hasIdleProxyLocked()
		m.mu.Unlock()
		if shouldRestart {
			m.processQueueAsync()
		}
	}()
}

func (m *Manager) processQueues(ctx context.Context) {
	for {
		clientIP, refresh, ok := m.nextQueuedClient()
		if !ok {
			return
		}
		assignment := m.ensureLease(ctx, clientIP, refresh)
		if assignment.Status != LeaseActive {
			return
		}
	}
}

func (m *Manager) nextQueuedClient() (string, bool, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.hasIdleProxyLocked() {
		return "", false, false
	}
	if len(m.pendingNew) > 0 {
		clientIP := m.pendingNew[0]
		m.pendingNew = m.pendingNew[1:]
		return clientIP, false, true
	}
	if len(m.pendingRefs) > 0 {
		clientIP := m.pendingRefs[0]
		m.pendingRefs = m.pendingRefs[1:]
		return clientIP, true, true
	}
	return "", false, false
}

func (m *Manager) releaseLocked(ctx context.Context, clientIP string, closeConns bool) {
	l := m.leases[clientIP]
	if l == nil {
		return
	}
	delete(m.leases, clientIP)
	m.removeQueuedLocked(clientIP)
	if p := m.proxies[l.ProxyID]; p != nil {
		m.releaseProxyBindingLocked(p)
	}
	for _, id := range m.proxyOrder {
		p := m.proxies[id]
		if p != nil && p.DrainingFor == clientIP {
			m.releaseProxyBindingLocked(p)
		}
	}
	_ = m.db.DeleteLease(ctx, clientIP)
	m.db.AddEvent(ctx, clientIP, l.ProxyID, "released", "")
	if closeConns {
		go m.CloseClientConnections(clientIP)
	}
}

func (m *Manager) markProxyIdleLocked(p *Proxy) {
	p.ClientIP = ""
	p.DrainingFor = ""
	p.ActiveConns = 0
	if p.Status == ProxyUnhealthy {
		return
	}
	p.Status = ProxyIdle
}

func (m *Manager) releaseProxyBindingLocked(p *Proxy) {
	if p == nil {
		return
	}
	p.ClientIP = ""
	p.DrainingFor = ""
	p.ActiveConns = 0
	if p.Status != ProxyUnhealthy {
		p.Status = ProxyIdle
	}
}

func (m *Manager) takeIdleProxyLocked() *Proxy {
	for _, id := range m.proxyOrder {
		p := m.proxies[id]
		if p != nil && p.Status == ProxyIdle && !p.Disabled {
			return p
		}
	}
	return nil
}

func (m *Manager) hasIdleProxyLocked() bool {
	return m.takeIdleProxyLocked() != nil
}

func (m *Manager) hasQueuedLocked() bool {
	return len(m.pendingNew) > 0 || len(m.pendingRefs) > 0
}

func (m *Manager) defaultProbeProxy(ctx context.Context, p Proxy) (string, error) {
	timeout := m.cfg.HealthCheck.Timeout.Duration
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	if err := probeSOCKS(ctx, p, m.cfg.HealthCheck.TargetHost, m.cfg.HealthCheck.TargetPort, timeout); err != nil {
		return "", err
	}
	exitIP := probeExitIP(ctx, p, m.cfg.HealthCheck.ExitIPURL, timeout)
	return exitIP, nil
}

func (m *Manager) clientViewsLocked() []ClientView {
	seen := map[string]bool{}
	clients := make([]ClientView, 0, len(m.leases)+len(m.pendingNew))
	for _, ip := range m.pendingNew {
		seen[ip] = true
		clients = append(clients, ClientView{ClientIP: ip, Status: LeasePending, Queued: true})
	}
	for ip, l := range m.leases {
		seen[ip] = true
		view := ClientView{
			ClientIP:  ip,
			Status:    l.Status,
			ProxyID:   l.ProxyID,
			ExpiresAt: l.ExpiresAt,
			Queued:    m.inQueueLocked(m.pendingRefs, ip),
		}
		if view.Queued {
			view.Status = LeasePending
		}
		if p := m.proxies[l.ProxyID]; p != nil {
			view.ProxyAddress = p.Address
			view.ExitIP = p.ExitIP
		}
		view.ActiveConnections = m.clientConnectionCountLocked(ip)
		clients = append(clients, view)
	}
	for _, ip := range m.pendingRefs {
		if seen[ip] {
			continue
		}
		clients = append(clients, ClientView{ClientIP: ip, Status: LeasePending, Queued: true})
	}
	sort.Slice(clients, func(i, j int) bool {
		return clients[i].ClientIP < clients[j].ClientIP
	})
	return clients
}

func (m *Manager) clientConnectionCountLocked(clientIP string) int {
	total := 0
	for _, id := range m.proxyOrder {
		p := m.proxies[id]
		if p == nil {
			continue
		}
		if p.ClientIP == clientIP || p.DrainingFor == clientIP {
			total += p.ActiveConns
		}
	}
	return total
}

func parseProxyLine(line string) (ProxyInput, error) {
	if strings.Contains(line, "://") {
		return parseProxyURL(line)
	}
	parts := strings.Split(line, ":")
	if len(parts) < 2 {
		return ProxyInput{}, errors.New("expected host:port[:username:password]")
	}
	host := parts[0]
	port := parts[1]
	if host == "" || port == "" {
		return ProxyInput{}, errors.New("host and port are required")
	}
	if _, err := strconv.Atoi(port); err != nil {
		return ProxyInput{}, errors.New("invalid port")
	}
	in := ProxyInput{Address: net.JoinHostPort(host, port)}
	if len(parts) >= 3 {
		in.Username = parts[2]
	}
	if len(parts) >= 4 {
		in.Password = strings.Join(parts[3:], ":")
	}
	in.ID = proxyID(in)
	return in, nil
}

func parseProxyURL(raw string) (ProxyInput, error) {
	if strings.HasPrefix(raw, "socks5://") && strings.Count(raw, "@") == 0 {
		trimmed := strings.TrimPrefix(raw, "socks5://")
		parts := strings.Split(trimmed, ":")
		if len(parts) >= 4 {
			in := ProxyInput{
				Address:  net.JoinHostPort(parts[0], parts[1]),
				Username: parts[2],
				Password: strings.Join(parts[3:], ":"),
			}
			if _, err := strconv.Atoi(parts[1]); err != nil {
				return ProxyInput{}, errors.New("invalid port")
			}
			in.ID = proxyID(in)
			return in, nil
		}
	}
	u, err := url.Parse(raw)
	if err != nil {
		return ProxyInput{}, err
	}
	if u.Scheme != "socks5" && u.Scheme != "socks" {
		return ProxyInput{}, errors.New("only socks5 URLs are supported")
	}
	host := u.Hostname()
	port := u.Port()
	if host == "" || port == "" {
		return ProxyInput{}, errors.New("host and port are required")
	}
	if _, err := strconv.Atoi(port); err != nil {
		return ProxyInput{}, errors.New("invalid port")
	}
	in := ProxyInput{Address: net.JoinHostPort(host, port)}
	if u.User != nil {
		in.Username = u.User.Username()
		in.Password, _ = u.User.Password()
	}
	in.ID = proxyID(in)
	return in, nil
}

func proxyID(in ProxyInput) string {
	sum := sha1.Sum([]byte(in.Address + "|" + in.Username + "|" + in.Password))
	return "proxy-" + hex.EncodeToString(sum[:])[:12]
}

func uniqueStrings(values []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" || seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	return out
}

func (m *Manager) enqueueUniqueLocked(queue *[]string, clientIP string) {
	for _, ip := range *queue {
		if ip == clientIP {
			return
		}
	}
	*queue = append(*queue, clientIP)
	if l := m.leases[clientIP]; l != nil {
		l.QueuedAt = time.Now()
	}
}

func (m *Manager) inQueueLocked(queue []string, clientIP string) bool {
	for _, ip := range queue {
		if ip == clientIP {
			return true
		}
	}
	return false
}

func (m *Manager) removeQueuedLocked(clientIP string) {
	m.pendingNew = removeIP(m.pendingNew, clientIP)
	m.pendingRefs = removeIP(m.pendingRefs, clientIP)
}

func removeIP(queue []string, clientIP string) []string {
	out := queue[:0]
	for _, ip := range queue {
		if ip != clientIP {
			out = append(out, ip)
		}
	}
	return out
}

func (m *Manager) expiryLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.expireLeases(ctx)
		}
	}
}

func (m *Manager) expireLeases(ctx context.Context) {
	now := time.Now()
	var expired []string
	m.mu.Lock()
	for ip, l := range m.leases {
		if l.Status == LeaseActive && now.After(l.ExpiresAt) {
			expired = append(expired, ip)
		}
	}
	m.mu.Unlock()
	for _, ip := range expired {
		slog.Info("lease expired", "client_ip", ip)
		m.mu.Lock()
		m.releaseLocked(ctx, ip, true)
		m.mu.Unlock()
	}
}
