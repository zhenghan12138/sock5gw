package manager

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
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

	"sock5gw/internal/outbound"
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
	type healthResult struct {
		proxyID string
		address string
		err     error
		exitIP  string
	}

	start := time.Now()
	m.mu.Lock()
	proxies := make([]Proxy, 0, len(m.proxies))
	for _, id := range m.proxyOrder {
		if p := m.proxies[id]; p != nil && !p.Disabled && p.Status != ProxyDisabled {
			proxies = append(proxies, *p)
		}
	}
	m.mu.Unlock()
	var preflightAmbiguous *healthResult
	if m.connector.FrontEnabled() {
		for _, proxy := range proxies {
			current, ok := m.proxyCheckSnapshot(proxy.ID)
			if !ok {
				continue
			}
			err := m.probeSOCKS(ctx, current, m.cfg.HealthCheck.TargetHost, m.cfg.HealthCheck.TargetPort, m.cfg.HealthCheck.Timeout.Duration)
			if ctx.Err() != nil {
				return
			}
			switch outbound.FailureScopeOf(err) {
			case outbound.FailureScopeShared:
				slog.Warn("health check skipped: front proxy unavailable", "err", err)
				return
			case outbound.FailureScopeAmbiguous:
				preflightAmbiguous = &healthResult{proxyID: current.ID, address: current.Address, err: err}
			}
			break
		}
	}
	roundCtx, cancelRound := context.WithCancel(ctx)
	defer cancelRound()
	workers := m.cfg.HealthCheck.Concurrency
	if workers < 1 {
		workers = 1
	}
	if workers > len(proxies) && len(proxies) > 0 {
		workers = len(proxies)
	}
	jobs := make(chan Proxy)
	var checked int64
	var frontFailureDetected atomic.Bool
	var skippedCandidate atomic.Bool
	var resultsMu sync.Mutex
	results := make([]healthResult, 0, len(proxies))
	if preflightAmbiguous != nil {
		results = append(results, *preflightAmbiguous)
		checked = 1
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobs {
				if roundCtx.Err() != nil {
					return
				}
				current, ok := m.proxyCheckSnapshot(p.ID)
				if !ok {
					skippedCandidate.Store(true)
					continue
				}
				p = current
				exitIP, err := m.probeProxy(roundCtx, p)
				if outbound.FailureScopeOf(err) == outbound.FailureScopeShared {
					if frontFailureDetected.CompareAndSwap(false, true) {
						slog.Warn("health check stopped: front proxy unavailable", "err", err)
					}
					cancelRound()
					return
				}
				if roundCtx.Err() != nil {
					return
				}
				resultsMu.Lock()
				results = append(results, healthResult{proxyID: p.ID, address: p.Address, err: err, exitIP: exitIP})
				resultsMu.Unlock()
				atomic.AddInt64(&checked, 1)
			}
		}()
	}
	for _, p := range proxies {
		if preflightAmbiguous != nil && p.ID == preflightAmbiguous.proxyID {
			continue
		}
		select {
		case <-roundCtx.Done():
			close(jobs)
			wg.Wait()
			return
		case jobs <- p:
		}
	}
	close(jobs)
	wg.Wait()
	if ctx.Err() != nil || roundCtx.Err() != nil || frontFailureDetected.Load() {
		return
	}
	type causalEvidence struct {
		address string
		token   outbound.FrontToken
	}
	var causalEvidenceSet []causalEvidence
	for _, result := range results {
		if !outbound.FrontEstablished(result.err) {
			continue
		}
		var phaseErr *outbound.PhaseError
		if errors.As(result.err, &phaseErr) && phaseErr.Token.Sequence != 0 {
			causalEvidenceSet = append(causalEvidenceSet, causalEvidence{address: result.address, token: phaseErr.Token})
		}
	}
	if evidence, ok := m.connector.RecentFrontEvidence(); ok && evidence.Sequence != 0 {
		causalEvidenceSet = append(causalEvidenceSet, causalEvidence{
			address: evidence.ExitAddress,
			token:   outbound.FrontToken{Generation: evidence.Generation, Sequence: evidence.Sequence},
		})
	}
	validatedAmbiguous := make(map[int]bool)
	ambiguousCount := 0
	for i, result := range results {
		if outbound.FailureScopeOf(result.err) != outbound.FailureScopeAmbiguous {
			continue
		}
		ambiguousCount++
		ambiguousToken, ok := outbound.AmbiguousFailureToken(result.err)
		if !ok {
			continue
		}
		for _, evidence := range causalEvidenceSet {
			if evidence.token.Generation == ambiguousToken.Generation && evidence.token.Sequence > ambiguousToken.Sequence && !sameEndpoint(result.address, evidence.address) {
				validatedAmbiguous[i] = true
				break
			}
		}
	}
	if ambiguousCount > 0 && len(validatedAmbiguous) > 0 {
		for i, result := range results {
			if outbound.FailureScopeOf(result.err) == outbound.FailureScopeAmbiguous {
				if !validatedAmbiguous[i] {
					continue
				}
				result.err = confirmedExitFailure(result.err)
			}
			if result.err == nil || outbound.FrontEstablished(result.err) || validatedAmbiguous[i] {
				m.recordHealth(ctx, result.proxyID, result.err, result.exitIP)
			}
		}
		return
	}
	if ambiguousCount > 0 {
		allAmbiguous := !skippedCandidate.Load() && ambiguousCount == len(results)
		var addresses []string
		var first outbound.FrontToken
		var last outbound.FrontToken
		for _, result := range results {
			if outbound.FailureScopeOf(result.err) != outbound.FailureScopeAmbiguous {
				allAmbiguous = false
				continue
			}
			token, ok := outbound.AmbiguousFailureToken(result.err)
			if !ok {
				allAmbiguous = false
				continue
			}
			if !ambiguousAddress(addresses, result.address) {
				addresses = append(addresses, result.address)
			}
			if first.Sequence == 0 {
				first = token
				last = token
				continue
			}
			if token.Generation != first.Generation {
				allAmbiguous = false
				continue
			}
			if token.Sequence < first.Sequence {
				first = token
			}
			if token.Sequence > last.Sequence {
				last = token
			}
		}
		if allAmbiguous && len(addresses) >= 2 && first.Sequence != 0 && last.Sequence != 0 {
			m.recordAmbiguousHealthBatch(roundCtx, first, last)
		}
		for _, result := range results {
			if result.err == nil || outbound.FrontEstablished(result.err) {
				m.recordHealth(ctx, result.proxyID, result.err, result.exitIP)
			}
		}
		return
	}
	for _, result := range results {
		m.recordHealth(ctx, result.proxyID, result.err, result.exitIP)
	}
	slog.Info("health check completed", "checked", checked, "total", len(proxies), "concurrency", workers, "duration", time.Since(start).String())
}

func (m *Manager) recordAmbiguousHealthBatch(ctx context.Context, first, last outbound.FrontToken) {
	if ctx.Err() != nil {
		return
	}
	m.connector.RecordAmbiguousBatchFailure(first, last)
}

func (m *Manager) recordHealth(ctx context.Context, proxyID string, err error, exitIP string) {
	if ctx.Err() != nil || (err != nil && outbound.IsFrontFailure(err)) {
		return
	}
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

func (m *Manager) probeExitIPAny(ctx context.Context, p Proxy, urls []string, timeout time.Duration) (string, error) {
	for _, rawURL := range urls {
		ip, err := m.probeExitIP(ctx, p, rawURL, timeout)
		if err != nil {
			if ctx.Err() != nil || outbound.FailureScopeOf(err) == outbound.FailureScopeShared {
				return "", err
			}
			continue
		}
		if ip != "" {
			return ip, nil
		}
	}
	return "", nil
}

func confirmedExitFailure(err error) error {
	var phaseErr *outbound.PhaseError
	if !errors.As(err, &phaseErr) {
		return err
	}
	return &outbound.PhaseError{
		Phase:            phaseErr.Phase,
		Scope:            outbound.FailureScopeExit,
		FrontEstablished: true,
		Token:            phaseErr.Token,
		Err:              phaseErr.Err,
	}
}

func (m *Manager) probeExitIP(ctx context.Context, p Proxy, rawURL string, timeout time.Duration) (string, error) {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return "", nil
	}
	host := u.Host
	if _, _, err := net.SplitHostPort(host); err != nil {
		port := "80"
		if u.Scheme == "https" {
			port = "443"
		}
		host = net.JoinHostPort(host, port)
	}
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	transportConn, err := m.ConnectProxy(probeCtx, p, host)
	if err != nil {
		return "", err
	}
	defer transportConn.Close()
	stopClose := context.AfterFunc(probeCtx, func() {
		_ = transportConn.Close()
	})
	defer stopClose()
	_ = transportConn.SetDeadline(time.Now().Add(timeout))
	var conn net.Conn = transportConn
	if u.Scheme == "https" {
		tlsConn := tls.Client(transportConn, &tls.Config{ServerName: u.Hostname(), MinVersion: tls.VersionTLS12})
		if err := tlsConn.HandshakeContext(probeCtx); err != nil {
			return "", nil
		}
		conn = tlsConn
	}
	path := u.RequestURI()
	if path == "" {
		path = "/"
	}
	req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: %s\r\nUser-Agent: sock5gw\r\nConnection: close\r\n\r\n", path, u.Host)
	if _, err := conn.Write([]byte(req)); err != nil {
		if probeCtx.Err() != nil {
			return "", context.Cause(probeCtx)
		}
		return "", nil
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		if probeCtx.Err() != nil {
			return "", context.Cause(probeCtx)
		}
		return "", nil
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", nil
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		if probeCtx.Err() != nil {
			return "", context.Cause(probeCtx)
		}
		return "", nil
	}
	ip := extractIP(body)
	if net.ParseIP(ip) == nil {
		return "", nil
	}
	return ip, nil
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

func (m *Manager) probeSOCKS(ctx context.Context, p Proxy, host string, port int, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	target := net.JoinHostPort(host, fmt.Sprint(port))
	conn, err := m.ConnectProxy(probeCtx, p, target)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))
	return nil
}
