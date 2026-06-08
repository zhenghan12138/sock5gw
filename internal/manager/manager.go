package manager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sort"
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
	ProxyActive    = "active"
	ProxyDraining  = "draining"
	ProxyUnhealthy = "unhealthy"
	ProxyDisabled  = "disabled"
)

var ErrNoLease = errors.New("no active lease")

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
}

func New(cfg *config.Config, db *store.DB) (*Manager, error) {
	m := &Manager{
		cfg:     cfg,
		db:      db,
		proxies: map[string]*Proxy{},
		leases:  map[string]*LeaseView{},
		conns:   map[string]map[net.Conn]struct{}{},
	}
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
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ensureLeaseLocked(context.Background(), clientIP, false)
}

func (m *Manager) Refresh(clientIP string) Assignment {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ensureLeaseLocked(context.Background(), clientIP, true)
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
	defer m.mu.Unlock()
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
			m.assignQueuedLocked(context.Background())
		}
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
	defer m.mu.Unlock()
	if _, exists := m.proxies[in.ID]; exists {
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
		return nil, err
	}
	m.proxies[p.ID] = p
	m.proxyOrder = append(m.proxyOrder, p.ID)
	sort.Strings(m.proxyOrder)
	m.assignQueuedLocked(ctx)
	cp := *p
	return &cp, nil
}

func (m *Manager) UpdateProxy(ctx context.Context, id string, in ProxyInput) (*Proxy, error) {
	if _, _, err := net.SplitHostPort(in.Address); err != nil {
		return nil, fmt.Errorf("address: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.proxies[id]
	if p == nil {
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
		return nil, err
	}
	m.assignQueuedLocked(ctx)
	cp := *p
	return &cp, nil
}

func (m *Manager) SetProxyDisabled(ctx context.Context, id string, disabled bool) (*Proxy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.proxies[id]
	if p == nil {
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
		return nil, err
	}
	m.assignQueuedLocked(ctx)
	cp := *p
	return &cp, nil
}

func (m *Manager) DeleteProxy(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.proxies[id]
	if p == nil {
		return errors.New("proxy not found")
	}
	if p.Status == ProxyActive || p.Status == ProxyDraining || p.ActiveConns > 0 {
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
	defer m.mu.Unlock()
	m.releaseLocked(context.Background(), clientIP, true)
}

func (m *Manager) ensureLeaseLocked(ctx context.Context, clientIP string, refresh bool) Assignment {
	now := time.Now()
	if l := m.leases[clientIP]; l != nil && l.Status == LeaseActive && now.After(l.ExpiresAt) {
		m.releaseLocked(ctx, clientIP, true)
	}
	if !refresh {
		if l := m.leases[clientIP]; l != nil && l.Status == LeaseActive {
			return Assignment{ClientIP: clientIP, ProxyID: l.ProxyID, Status: LeaseActive, ExpiresAt: l.ExpiresAt}
		}
	}
	proxy := m.takeIdleProxyLocked()
	if proxy == nil {
		if refresh {
			m.enqueueUniqueLocked(&m.pendingRefs, clientIP)
			if l := m.leases[clientIP]; l != nil && l.Status == LeaseActive {
				return Assignment{ClientIP: clientIP, ProxyID: l.ProxyID, Status: LeasePending, ExpiresAt: l.ExpiresAt}
			}
		} else {
			m.enqueueUniqueLocked(&m.pendingNew, clientIP)
		}
		m.db.AddEvent(ctx, clientIP, "", "pending", fmt.Sprintf("refresh=%v", refresh))
		return Assignment{ClientIP: clientIP, Status: LeasePending}
	}
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
	m.leases[clientIP] = &LeaseView{ClientIP: clientIP, ProxyID: proxy.ID, Status: LeaseActive, ExpiresAt: expires}
	_ = m.db.UpsertLease(ctx, store.Lease{ClientIP: clientIP, ProxyID: proxy.ID, Status: LeaseActive, ExpiresAt: expires, UpdatedAt: now})
	m.db.AddEvent(ctx, clientIP, proxy.ID, "assigned", fmt.Sprintf("refresh=%v", refresh))
	return Assignment{ClientIP: clientIP, ProxyID: proxy.ID, Status: LeaseActive, ExpiresAt: expires}
}

func (m *Manager) assignQueuedLocked(ctx context.Context) {
	for len(m.pendingNew) > 0 {
		proxy := m.takeIdleProxyLocked()
		if proxy == nil {
			return
		}
		clientIP := m.pendingNew[0]
		m.pendingNew = m.pendingNew[1:]
		now := time.Now()
		expires := now.Add(m.cfg.LeaseTTL.Duration)
		proxy.Status = ProxyActive
		proxy.ClientIP = clientIP
		m.leases[clientIP] = &LeaseView{ClientIP: clientIP, ProxyID: proxy.ID, Status: LeaseActive, ExpiresAt: expires}
		_ = m.db.UpsertLease(ctx, store.Lease{ClientIP: clientIP, ProxyID: proxy.ID, Status: LeaseActive, ExpiresAt: expires, UpdatedAt: now})
		m.db.AddEvent(ctx, clientIP, proxy.ID, "assigned_from_queue", "")
	}
	for len(m.pendingRefs) > 0 {
		proxy := m.takeIdleProxyLocked()
		if proxy == nil {
			return
		}
		clientIP := m.pendingRefs[0]
		m.pendingRefs = m.pendingRefs[1:]
		l := m.leases[clientIP]
		if l == nil || l.Status != LeaseActive {
			m.markProxyIdleLocked(proxy)
			continue
		}
		if oldProxy := m.proxies[l.ProxyID]; oldProxy != nil {
			if oldProxy.ActiveConns > 0 {
				oldProxy.Status = ProxyDraining
				oldProxy.ClientIP = ""
				oldProxy.DrainingFor = clientIP
			} else {
				m.markProxyIdleLocked(oldProxy)
			}
		}
		now := time.Now()
		expires := now.Add(m.cfg.LeaseTTL.Duration)
		proxy.Status = ProxyActive
		proxy.ClientIP = clientIP
		m.leases[clientIP] = &LeaseView{ClientIP: clientIP, ProxyID: proxy.ID, Status: LeaseActive, ExpiresAt: expires}
		_ = m.db.UpsertLease(ctx, store.Lease{ClientIP: clientIP, ProxyID: proxy.ID, Status: LeaseActive, ExpiresAt: expires, UpdatedAt: now})
		m.db.AddEvent(ctx, clientIP, proxy.ID, "refreshed_from_queue", "")
	}
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
	m.assignQueuedLocked(ctx)
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
