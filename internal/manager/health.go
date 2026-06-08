package manager

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var ipPattern = regexp.MustCompile(`(?m)(?:\d{1,3}\.){3}\d{1,3}|[0-9a-fA-F:]{2,}`)

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
				current, ok := m.proxyCheckSnapshot(p.ID)
				if !ok {
					continue
				}
				p = current
				err := probeSOCKS(ctx, p, m.cfg.HealthCheck.TargetHost, m.cfg.HealthCheck.TargetPort, m.cfg.HealthCheck.Timeout.Duration)
				exitIP := ""
				if err == nil {
					exitIP = probeExitIPAny(ctx, p, m.exitIPURLs(), m.cfg.HealthCheck.Timeout.Duration)
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
		go m.processQueueAsync()
	}
}

func (m *Manager) exitIPURLs() []string {
	defaults := []string{
		"https://api.ipify.org/",
		"https://icanhazip.com/",
		"https://ifconfig.me/ip",
		"http://api.ipify.org/",
	}
	seen := map[string]bool{}
	var urls []string
	add := func(raw string) {
		for _, part := range strings.Split(raw, ",") {
			part = strings.TrimSpace(part)
			if part == "" || seen[part] {
				continue
			}
			seen[part] = true
			urls = append(urls, part)
		}
	}
	add(m.cfg.HealthCheck.ExitIPURL)
	for _, raw := range defaults {
		add(raw)
	}
	return urls
}

func probeExitIPAny(ctx context.Context, p Proxy, urls []string, timeout time.Duration) string {
	for _, rawURL := range urls {
		if ip := probeExitIP(ctx, p, rawURL, timeout); ip != "" {
			return ip
		}
	}
	return ""
}

func probeExitIP(ctx context.Context, p Proxy, rawURL string, timeout time.Duration) string {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return ""
	}
	host := u.Host
	if _, _, err := net.SplitHostPort(host); err != nil {
		port := "80"
		if u.Scheme == "https" {
			port = "443"
		}
		host = net.JoinHostPort(host, port)
	}
	conn, err := DialProxy(ctx, p, timeout)
	if err != nil {
		return ""
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))
	if err := ConnectSOCKS5(conn, host, p.Username, p.Password); err != nil {
		return ""
	}
	if u.Scheme == "https" {
		tlsConn := tls.Client(conn, &tls.Config{ServerName: u.Hostname(), MinVersion: tls.VersionTLS12})
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			return ""
		}
		conn = tlsConn
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
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ""
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return ""
	}
	ip := extractIP(body)
	if net.ParseIP(ip) == nil {
		return ""
	}
	return ip
}

func extractIP(body []byte) string {
	text := strings.TrimSpace(string(body))
	if ip := net.ParseIP(text); ip != nil {
		return ip.String()
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err == nil {
		for _, key := range []string{"ip", "query", "origin"} {
			if value, ok := payload[key].(string); ok {
				if ip := firstIP(value); ip != "" {
					return ip
				}
			}
		}
	}
	return firstIP(text)
}

func firstIP(text string) string {
	for _, candidate := range ipPattern.FindAllString(text, -1) {
		if ip := net.ParseIP(strings.Trim(candidate, "[]")); ip != nil {
			return ip.String()
		}
	}
	return ""
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
	conn, err := DialProxy(ctx, p, timeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))
	target := net.JoinHostPort(host, fmt.Sprint(port))
	return ConnectSOCKS5(conn, target, p.Username, p.Password)
}
