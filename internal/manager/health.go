package manager

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func (m *Manager) healthLoop(ctx context.Context) {
	ticker := time.NewTicker(m.cfg.HealthCheck.Interval.Duration)
	defer ticker.Stop()
	m.checkAll(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.checkAll(ctx)
		}
	}
}

func (m *Manager) checkAll(ctx context.Context) {
	start := time.Now()
	m.mu.Lock()
	proxies := make([]Proxy, 0, len(m.proxies))
	for _, id := range m.proxyOrder {
		if p := m.proxies[id]; p != nil {
			proxies = append(proxies, *p)
		}
	}
	m.mu.Unlock()
	workers := m.cfg.HealthCheck.Concurrency
	if workers < 1 {
		workers = 1
	}
	if workers > len(proxies) && len(proxies) > 0 {
		workers = len(proxies)
	}
	jobs := make(chan Proxy)
	var checked int64
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobs {
				if ctx.Err() != nil {
					return
				}
				err := probeSOCKS(ctx, p, m.cfg.HealthCheck.TargetHost, m.cfg.HealthCheck.TargetPort, m.cfg.HealthCheck.Timeout.Duration)
				exitIP := ""
				if err == nil {
					exitIP = probeExitIP(ctx, p, m.cfg.HealthCheck.ExitIPURL, m.cfg.HealthCheck.Timeout.Duration)
				}
				m.recordHealth(ctx, p.ID, err, exitIP)
				atomic.AddInt64(&checked, 1)
			}
		}()
	}
	for _, p := range proxies {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return
		case jobs <- p:
		}
	}
	close(jobs)
	wg.Wait()
	slog.Info("health check completed", "checked", checked, "total", len(proxies), "concurrency", workers, "duration", time.Since(start).String())
}

func (m *Manager) recordHealth(ctx context.Context, proxyID string, err error, exitIP string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.proxies[proxyID]
	if p == nil {
		return
	}
	if err != nil {
		p.FailureCount++
		p.SuccessCount = 0
		p.LastHealthDetail = err.Error()
		if p.FailureCount >= m.cfg.HealthCheck.FailureThreshold && p.Status != ProxyUnhealthy {
			p.Status = ProxyUnhealthy
			m.db.AddEvent(ctx, "", proxyID, "proxy_unhealthy", err.Error())
			slog.Warn("proxy unhealthy", "proxy_id", proxyID, "err", err)
		}
		return
	}
	p.SuccessCount++
	p.FailureCount = 0
	p.LastHealthDetail = ""
	if exitIP != "" {
		p.ExitIP = exitIP
	}
	if p.Status == ProxyUnhealthy && p.SuccessCount >= m.cfg.HealthCheck.SuccessThreshold {
		p.Status = m.recoveredProxyStatusLocked(p)
		m.db.AddEvent(ctx, "", proxyID, "proxy_healthy", "")
		m.assignQueuedLocked(ctx)
	}
}

func probeExitIP(ctx context.Context, p Proxy, rawURL string, timeout time.Duration) string {
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme != "http" || u.Host == "" {
		return ""
	}
	host := u.Host
	if _, _, err := net.SplitHostPort(host); err != nil {
		host = net.JoinHostPort(host, "80")
	}
	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", p.Address)
	if err != nil {
		return ""
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))
	if err := ConnectSOCKS5(conn, host, p.Username, p.Password); err != nil {
		return ""
	}
	path := u.RequestURI()
	if path == "" {
		path = "/"
	}
	req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: %s\r\nUser-Agent: sock5gw\r\nConnection: close\r\n\r\n", path, u.Host)
	if _, err := conn.Write([]byte(req)); err != nil {
		return ""
	}
	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	if err != nil || !strings.Contains(line, "200") {
		return ""
	}
	for {
		header, err := br.ReadString('\n')
		if err != nil {
			return ""
		}
		if header == "\r\n" {
			break
		}
	}
	body, err := io.ReadAll(io.LimitReader(br, 128))
	if err != nil {
		return ""
	}
	ip := strings.TrimSpace(string(body))
	if net.ParseIP(ip) == nil {
		return ""
	}
	return ip
}

func (m *Manager) recoveredProxyStatusLocked(p *Proxy) string {
	now := time.Now()
	for ip, lease := range m.leases {
		if lease.ProxyID == p.ID && lease.Status == LeaseActive && now.Before(lease.ExpiresAt) {
			p.ClientIP = ip
			p.DrainingFor = ""
			return ProxyActive
		}
	}
	if p.DrainingFor != "" && p.ActiveConns > 0 {
		p.ClientIP = ""
		return ProxyDraining
	}
	p.ClientIP = ""
	p.DrainingFor = ""
	return ProxyIdle
}

func probeSOCKS(ctx context.Context, p Proxy, host string, port int, timeout time.Duration) error {
	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", p.Address)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))
	target := net.JoinHostPort(host, fmt.Sprint(port))
	return ConnectSOCKS5(conn, target, p.Username, p.Password)
}
