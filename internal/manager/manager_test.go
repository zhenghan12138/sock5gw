package manager

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sock5gw/internal/config"
	"sock5gw/internal/outbound"
	"sock5gw/internal/store"
)

func testConfig(proxies int) *config.Config {
	cfg := &config.Config{
		DBPath:   ":memory:",
		LeaseTTL: config.Duration{Duration: time.Hour},
		API: config.API{
			ClientKey: "client",
			AdminKey:  "admin",
		},
		DNS: config.DNS{
			FakeIPCIDR: "198.18.0.0/15",
		},
		HealthCheck: config.HealthCheck{
			Timeout:    config.Duration{Duration: time.Second},
			TargetHost: "example.com",
			TargetPort: 80,
		},
	}
	for i := 0; i < proxies; i++ {
		cfg.Proxies = append(cfg.Proxies, config.ProxyConfig{
			ID:      string(rune('a' + i)),
			Address: "127.0.0.1:1080",
		})
	}
	return cfg
}

func testManagerFromConfig(t *testing.T, cfg *config.Config) *Manager {
	t.Helper()
	db, err := store.Open(cfg.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m, err := New(cfg, db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.stopQueueProcessing)
	m.probeProxy = func(context.Context, Proxy) (string, error) {
		return "203.0.113.200", nil
	}
	return m
}

func testManager(t *testing.T, proxies int) *Manager {
	t.Helper()
	return testManagerFromConfig(t, testConfig(proxies))
}

func rejectingFrontProxy(t *testing.T) (string, *atomic.Int64) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	var accepts atomic.Int64
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepts.Add(1)
			_ = conn.Close()
		}
	}()
	return listener.Addr().String(), &accepts
}

func connectRejectingFrontProxy(t *testing.T) (string, *atomic.Int64) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	var accepts atomic.Int64
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepts.Add(1)
			go rejectSOCKSConnect(conn)
		}
	}()
	return listener.Addr().String(), &accepts
}

func rejectSOCKSConnect(conn net.Conn) {
	defer conn.Close()
	greeting := make([]byte, 2)
	if _, err := io.ReadFull(conn, greeting); err != nil || greeting[0] != 5 {
		return
	}
	if _, err := io.CopyN(io.Discard, conn, int64(greeting[1])); err != nil {
		return
	}
	if _, err := conn.Write([]byte{5, 0}); err != nil {
		return
	}
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return
	}
	addressLength := 0
	switch header[3] {
	case 1:
		addressLength = 4
	case 3:
		length := []byte{0}
		if _, err := io.ReadFull(conn, length); err != nil {
			return
		}
		addressLength = int(length[0])
	case 4:
		addressLength = 16
	default:
		return
	}
	if _, err := io.CopyN(io.Discard, conn, int64(addressLength+2)); err != nil {
		return
	}
	_, _ = conn.Write([]byte{5, 5, 0, 1, 0, 0, 0, 0, 0, 0})
}

func waitForLeaseStatus(t *testing.T, m *Manager, clientIP string, status string) Assignment {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		current := m.Current(clientIP)
		if current.Status == status {
			return current
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s, current = %+v", status, current)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestLeaseUsesIdleProxyAndQueuesWhenFull(t *testing.T) {
	m := testManager(t, 1)

	first := m.Lease("192.0.2.10")
	if first.Status != LeaseActive || first.ProxyID != "a" {
		t.Fatalf("first lease = %+v", first)
	}

	second := m.Lease("192.0.2.11")
	if second.Status != LeasePending || second.ProxyID != "" {
		t.Fatalf("second lease = %+v", second)
	}

	status := m.Status()
	pending := status["pending_new"].([]string)
	if len(pending) != 1 || pending[0] != "192.0.2.11" {
		t.Fatalf("pending_new = %#v", pending)
	}
}

func TestAcquireBeforeRefreshKeepsOldProxyDrainingUntilConnectionCloses(t *testing.T) {
	m := testManager(t, 2)
	lease := m.Lease("192.0.2.10")
	if lease.ProxyID != "a" {
		t.Fatalf("lease = %+v", lease)
	}
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	acquired, err := m.AcquireProxyConn("192.0.2.10", c1)
	if err != nil {
		t.Fatal(err)
	}
	if acquired.ID != "a" || acquired.ActiveConns != 1 {
		t.Fatalf("acquired proxy = %+v", acquired)
	}

	refreshed := m.Refresh("192.0.2.10")
	if refreshed.Status != LeaseActive || refreshed.ProxyID != "b" {
		t.Fatalf("refresh = %+v", refreshed)
	}
	if old := m.proxies["a"]; old.Status != ProxyDraining || old.ActiveConns != 1 {
		t.Fatalf("old proxy while acquired = %+v", old)
	}

	m.UnregisterConn("192.0.2.10", "a", c1)
	if m.proxies["a"].Status != ProxyIdle {
		t.Fatalf("old proxy after close = %s", m.proxies["a"].Status)
	}
}

func TestRefreshBeforeAcquireUsesNewProxy(t *testing.T) {
	m := testManager(t, 2)
	lease := m.Lease("192.0.2.10")
	if lease.ProxyID != "a" {
		t.Fatalf("lease = %+v", lease)
	}
	refreshed := m.Refresh("192.0.2.10")
	if refreshed.Status != LeaseActive || refreshed.ProxyID != "b" {
		t.Fatalf("refresh = %+v", refreshed)
	}

	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	acquired, err := m.AcquireProxyConn("192.0.2.10", c1)
	if err != nil {
		t.Fatal(err)
	}
	if acquired.ID != "b" || acquired.ActiveConns != 1 {
		t.Fatalf("acquired proxy = %+v", acquired)
	}
	if old := m.proxies["a"]; old.Status != ProxyIdle || old.ActiveConns != 0 {
		t.Fatalf("old proxy after refresh = %+v", old)
	}
	m.UnregisterConn("192.0.2.10", "b", c1)
}

func TestAcquireBeforeReleaseKeepsProxyDrainingUntilConnectionCloses(t *testing.T) {
	m := testManager(t, 1)
	first := m.Lease("192.0.2.10")
	if first.Status != LeaseActive || first.ProxyID != "a" {
		t.Fatalf("first lease = %+v", first)
	}

	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	c3, c4 := net.Pipe()
	defer c3.Close()
	defer c4.Close()
	acquired, err := m.AcquireProxyConn("192.0.2.10", c1)
	if err != nil {
		t.Fatal(err)
	}
	if acquired.ID != "a" {
		t.Fatalf("acquired proxy = %+v", acquired)
	}
	acquired, err = m.AcquireProxyConn("192.0.2.10", c3)
	if err != nil {
		t.Fatal(err)
	}
	if acquired.ID != "a" || acquired.ActiveConns != 2 {
		t.Fatalf("second acquired proxy = %+v", acquired)
	}
	pending := m.Lease("192.0.2.11")
	if pending.Status != LeasePending {
		t.Fatalf("pending lease = %+v", pending)
	}

	m.Release("192.0.2.10")
	if current := m.Current("192.0.2.10"); current.Status != LeaseBlocked {
		t.Fatalf("released client = %+v", current)
	}
	if proxy := m.proxies["a"]; proxy.Status != ProxyDraining || proxy.ActiveConns != 2 {
		t.Fatalf("released proxy while acquired = %+v", proxy)
	}
	if current := m.Current("192.0.2.11"); current.Status != LeasePending {
		t.Fatalf("queued client while proxy draining = %+v", current)
	}

	m.UnregisterConn("192.0.2.10", "a", c1)
	if proxy := m.proxies["a"]; proxy.Status != ProxyDraining || proxy.ActiveConns != 1 {
		t.Fatalf("proxy after first connection closed = %+v", proxy)
	}
	m.UnregisterConn("192.0.2.10", "a", c1)
	if proxy := m.proxies["a"]; proxy.Status != ProxyDraining || proxy.ActiveConns != 1 {
		t.Fatalf("duplicate unregister changed proxy = %+v", proxy)
	}
	if current := m.Current("192.0.2.11"); current.Status != LeasePending {
		t.Fatalf("queued client before final close = %+v", current)
	}

	m.UnregisterConn("192.0.2.10", "a", c3)
	current := waitForLeaseStatus(t, m, "192.0.2.11", LeaseActive)
	if current.ProxyID != "a" {
		t.Fatalf("queued client after release = %+v", current)
	}
}

func TestReleaseBeforeAcquireRejectsStaleLease(t *testing.T) {
	m := testManager(t, 1)
	lease := m.Lease("192.0.2.10")
	if lease.Status != LeaseActive || lease.ProxyID != "a" {
		t.Fatalf("lease = %+v", lease)
	}
	m.Release("192.0.2.10")

	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	if proxy, err := m.AcquireProxyConn("192.0.2.10", c1); !errors.Is(err, ErrNoLease) {
		t.Fatalf("acquire after release = proxy %+v, err %v", proxy, err)
	}
	if proxy := m.proxies["a"]; proxy.Status != ProxyIdle || proxy.ActiveConns != 0 {
		t.Fatalf("released proxy = %+v", proxy)
	}
}

func TestRefreshPendingKeepsCurrentProxy(t *testing.T) {
	m := testManager(t, 1)
	lease := m.Lease("192.0.2.10")
	refreshed := m.Refresh("192.0.2.10")

	if refreshed.Status != LeasePending || refreshed.ProxyID != lease.ProxyID {
		t.Fatalf("refresh = %+v, lease = %+v", refreshed, lease)
	}
	current := m.Current("192.0.2.10")
	if current.Status != LeasePending || current.ProxyID != lease.ProxyID {
		t.Fatalf("current = %+v", current)
	}
}

func TestPendingNewIsObservable(t *testing.T) {
	m := testManager(t, 1)
	_ = m.Lease("192.0.2.10")
	pending := m.Lease("192.0.2.11")
	if pending.Status != LeasePending {
		t.Fatalf("pending lease = %+v", pending)
	}

	current := m.Current("192.0.2.11")
	if current.Status != LeasePending || current.ProxyID != "" {
		t.Fatalf("pending current = %+v", current)
	}
}

func TestReleaseAssignsQueuedClient(t *testing.T) {
	m := testManager(t, 1)
	first := m.Lease("192.0.2.10")
	if first.Status != LeaseActive {
		t.Fatalf("first = %+v", first)
	}
	second := m.Lease("192.0.2.11")
	if second.Status != LeasePending {
		t.Fatalf("second = %+v", second)
	}

	m.mu.Lock()
	m.releaseLocked(context.Background(), "192.0.2.10", false)
	m.mu.Unlock()
	m.processQueueAsync()

	current := waitForLeaseStatus(t, m, "192.0.2.11", LeaseActive)
	if current.Status != LeaseActive || current.ProxyID != "a" {
		t.Fatalf("queued current = %+v", current)
	}
}

func TestAddProxyAssignsQueuedClient(t *testing.T) {
	m := testManager(t, 1)
	_ = m.Lease("192.0.2.10")
	pending := m.Lease("192.0.2.11")
	if pending.Status != LeasePending {
		t.Fatalf("pending = %+v", pending)
	}

	proxy, err := m.AddProxy(context.Background(), ProxyInput{ID: "b", Address: "127.0.0.1:1081"})
	if err != nil {
		t.Fatal(err)
	}
	if proxy.ID != "b" {
		t.Fatalf("proxy = %+v", proxy)
	}
	current := waitForLeaseStatus(t, m, "192.0.2.11", LeaseActive)
	if current.Status != LeaseActive || current.ProxyID != "b" {
		t.Fatalf("current = %+v", current)
	}
}

func TestDisableIdleProxySkipsAllocation(t *testing.T) {
	m := testManager(t, 2)
	if _, err := m.SetProxyDisabled(context.Background(), "a", true); err != nil {
		t.Fatal(err)
	}
	lease := m.Lease("192.0.2.10")
	if lease.Status != LeaseActive || lease.ProxyID != "b" {
		t.Fatalf("lease = %+v", lease)
	}
}

func TestLeaseProbesProxyBeforeAssignment(t *testing.T) {
	m := testManager(t, 2)
	calls := 0
	m.probeProxy = func(_ context.Context, p Proxy) (string, error) {
		calls++
		if p.ID == "a" {
			return "", errors.New("dial failed")
		}
		return "198.51.100.88", nil
	}

	lease := m.Lease("192.0.2.10")
	if lease.Status != LeaseActive || lease.ProxyID != "b" {
		t.Fatalf("lease = %+v", lease)
	}
	if calls != 2 {
		t.Fatalf("probe calls = %d", calls)
	}
	if m.proxies["a"].Status != ProxyUnhealthy {
		t.Fatalf("bad proxy status = %s", m.proxies["a"].Status)
	}
	if m.proxies["b"].ExitIP != "198.51.100.88" {
		t.Fatalf("exit ip = %q", m.proxies["b"].ExitIP)
	}
}

func TestFrontFailureKeepsLeasePendingWithoutPollutingExit(t *testing.T) {
	frontAddress, accepts := rejectingFrontProxy(t)
	cfg := testConfig(1)
	cfg.FrontProxy = config.FrontProxy{
		Enabled:  true,
		Protocol: "socks5",
		Address:  frontAddress,
		Username: "front-user",
		Password: "front-secret",
	}
	m := testManagerFromConfig(t, cfg)
	m.probeProxy = m.defaultProbeProxy

	assignment := m.LeaseContext(context.Background(), "192.0.2.30")
	if assignment.Status != LeasePending || assignment.ProxyID != "" {
		t.Fatalf("assignment = %+v", assignment)
	}
	current := m.Current("192.0.2.30")
	if current.Status != LeasePending || current.ProxyID != "" {
		t.Fatalf("current = %+v", current)
	}
	m.mu.Lock()
	proxy := *m.proxies["a"]
	retryScheduled := m.queueRetry
	m.mu.Unlock()
	if proxy.Status != ProxyIdle || proxy.ClientIP != "" {
		t.Fatalf("proxy binding was not restored: %+v", proxy)
	}
	if proxy.FailureCount != 0 || proxy.SuccessCount != 0 || proxy.LastHealthDetail != "" {
		t.Fatalf("front failure polluted exit health: %+v", proxy)
	}
	if !retryScheduled {
		t.Fatal("front circuit did not schedule a delayed queue retry")
	}

	status, ok := m.Status()["front_proxy"].(outbound.FrontStatus)
	if !ok || status.Status != "unhealthy" || status.Address != frontAddress {
		t.Fatalf("front status = %#v", status)
	}
	encoded, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "front-user") || strings.Contains(string(encoded), "front-secret") {
		t.Fatalf("front status leaked credentials: %s", encoded)
	}

	_, healthErr := m.ConnectProxy(context.Background(), proxy, "example.com:80")
	if !outbound.IsFrontFailure(healthErr) {
		t.Fatalf("expected front failure, got %v", healthErr)
	}
	m.recordHealth(context.Background(), proxy.ID, healthErr, "")
	m.mu.Lock()
	proxy = *m.proxies["a"]
	m.mu.Unlock()
	if proxy.Status != ProxyIdle || proxy.FailureCount != 0 || proxy.LastHealthDetail != "" {
		t.Fatalf("recordHealth polluted exit health: %+v", proxy)
	}

	time.Sleep(100 * time.Millisecond)
	if got := accepts.Load(); got != 1 {
		t.Fatalf("front accepts during circuit backoff = %d, want 1", got)
	}
	m.mu.Lock()
	m.removeQueuedLocked("192.0.2.30")
	if m.queueRetry || m.queueTimer != nil {
		t.Fatal("queue retry timer was not canceled after the queue emptied")
	}
	m.mu.Unlock()
}

func TestAmbiguousFrontFailuresTryDistinctExitsBeforeOpeningCircuit(t *testing.T) {
	frontAddress, accepts := connectRejectingFrontProxy(t)
	cfg := testConfig(2)
	cfg.Proxies[1].Address = "127.0.0.1:1081"
	cfg.FrontProxy = config.FrontProxy{Enabled: true, Protocol: "socks5", Address: frontAddress}
	m := testManagerFromConfig(t, cfg)
	m.probeProxy = m.defaultProbeProxy

	assignment := m.Lease("192.0.2.33")
	if assignment.Status != LeasePending {
		t.Fatalf("assignment = %+v", assignment)
	}
	if got := accepts.Load(); got != 2 {
		t.Fatalf("front accepts = %d, want one per distinct exit", got)
	}
	m.mu.Lock()
	for _, id := range m.proxyOrder {
		proxy := m.proxies[id]
		if proxy.Status != ProxyIdle || proxy.FailureCount != 0 || proxy.LastHealthDetail != "" {
			m.mu.Unlock()
			t.Fatalf("proxy %s was polluted by ambiguous failures: %+v", id, proxy)
		}
	}
	retryScheduled := m.queueRetry && m.queueTimer != nil
	m.mu.Unlock()
	if !retryScheduled || m.connector.FrontRetryAfter() <= 0 {
		t.Fatal("ambiguous batch did not open the delayed front circuit")
	}
	time.Sleep(100 * time.Millisecond)
	if got := accepts.Load(); got != 2 {
		t.Fatalf("front accepts during batch backoff = %d, want 2", got)
	}
	m.mu.Lock()
	m.removeQueuedLocked("192.0.2.33")
	m.mu.Unlock()
}

func TestSingleAmbiguousExitUsesLocalBackoffWithoutGlobalCircuit(t *testing.T) {
	frontAddress, accepts := connectRejectingFrontProxy(t)
	cfg := testConfig(1)
	cfg.FrontProxy = config.FrontProxy{Enabled: true, Protocol: "socks5", Address: frontAddress}
	m := testManagerFromConfig(t, cfg)
	m.probeProxy = m.defaultProbeProxy

	first := m.Lease("192.0.2.38")
	second := m.Lease("192.0.2.38")
	if first.Status != LeasePending || second.Status != LeasePending {
		t.Fatalf("assignments = %+v %+v", first, second)
	}
	if got := accepts.Load(); got != 1 {
		t.Fatalf("front accepts during local backoff = %d, want 1", got)
	}
	if retryAfter := m.connector.FrontRetryAfter(); retryAfter != 0 {
		t.Fatalf("single ambiguous exit opened global circuit for %s", retryAfter)
	}
	m.mu.Lock()
	proxy := *m.proxies["a"]
	localBackoff := time.Until(m.queueBackoffUntil)
	retryScheduled := m.queueRetry && m.queueTimer != nil
	m.removeQueuedLocked("192.0.2.38")
	m.mu.Unlock()
	if proxy.Status != ProxyIdle || proxy.FailureCount != 0 {
		t.Fatalf("proxy = %+v", proxy)
	}
	if localBackoff <= 0 || !retryScheduled {
		t.Fatalf("local backoff=%s retry_scheduled=%v", localBackoff, retryScheduled)
	}
}

func TestAmbiguousLeaseFailureIsAttributedAfterDifferentExitEstablishesFront(t *testing.T) {
	cfg := testConfig(2)
	cfg.Proxies[1].Address = "127.0.0.1:1081"
	cfg.FrontProxy = config.FrontProxy{Enabled: true, Protocol: "socks5", Address: "127.0.0.1:11080"}
	m := testManagerFromConfig(t, cfg)
	calls := 0
	m.probeProxy = func(_ context.Context, proxy Proxy) (string, error) {
		calls++
		if proxy.ID == "a" {
			return "", &outbound.PhaseError{
				Phase: outbound.PhaseFrontConnectExit,
				Scope: outbound.FailureScopeAmbiguous,
				Token: outbound.FrontToken{Generation: 1, Sequence: 1},
				Err:   errors.New("connect rejected"),
			}
		}
		return "", &outbound.PhaseError{
			Phase:            outbound.PhaseExitAuth,
			Scope:            outbound.FailureScopeExit,
			FrontEstablished: true,
			Token:            outbound.FrontToken{Generation: 1, Sequence: 2},
			Err:              errors.New("exit auth failed"),
		}
	}

	assignment := m.Lease("192.0.2.34")
	if assignment.Status != LeasePending || assignment.ProxyID != "" || calls != 2 {
		t.Fatalf("assignment = %+v, calls = %d", assignment, calls)
	}
	if proxy := m.proxies["a"]; proxy.Status != ProxyUnhealthy || proxy.FailureCount != 1 {
		t.Fatalf("cross-validated proxy = %+v", proxy)
	}
}

func TestAmbiguousLeaseFailureIsNotAttributedBySameEndpointEvidence(t *testing.T) {
	cfg := testConfig(2)
	cfg.FrontProxy = config.FrontProxy{Enabled: true, Protocol: "socks5", Address: "127.0.0.1:11080"}
	m := testManagerFromConfig(t, cfg)
	m.probeProxy = func(_ context.Context, proxy Proxy) (string, error) {
		if proxy.ID == "a" {
			return "", &outbound.PhaseError{
				Phase: outbound.PhaseFrontConnectExit,
				Scope: outbound.FailureScopeAmbiguous,
				Token: outbound.FrontToken{Generation: 1, Sequence: 1},
				Err:   errors.New("ambiguous"),
			}
		}
		return "", &outbound.PhaseError{
			Phase:            outbound.PhaseExitAuth,
			Scope:            outbound.FailureScopeExit,
			FrontEstablished: true,
			Token:            outbound.FrontToken{Generation: 1, Sequence: 2},
			Err:              errors.New("exit auth failed"),
		}
	}

	assignment := m.Lease("192.0.2.42")
	if assignment.Status != LeasePending {
		t.Fatalf("assignment = %+v", assignment)
	}
	if proxy := m.proxies["a"]; proxy.Status != ProxyIdle || proxy.FailureCount != 0 {
		t.Fatalf("same-endpoint evidence attributed ambiguous proxy: %+v", proxy)
	}
	if proxy := m.proxies["b"]; proxy.Status != ProxyUnhealthy || proxy.FailureCount != 1 {
		t.Fatalf("established exit failure = %+v", proxy)
	}
}

func TestMockSuccessWithoutConnectorEvidenceDoesNotAttributeAmbiguousExit(t *testing.T) {
	cfg := testConfig(2)
	cfg.Proxies[1].Address = "127.0.0.1:1081"
	cfg.FrontProxy = config.FrontProxy{Enabled: true, Protocol: "socks5", Address: "127.0.0.1:11080"}
	m := testManagerFromConfig(t, cfg)
	m.probeProxy = func(_ context.Context, proxy Proxy) (string, error) {
		if proxy.ID == "a" {
			return "", &outbound.PhaseError{
				Phase: outbound.PhaseFrontConnectExit,
				Scope: outbound.FailureScopeAmbiguous,
				Token: outbound.FrontToken{Generation: 1, Sequence: 1},
				Err:   errors.New("ambiguous"),
			}
		}
		return "198.51.100.90", nil
	}

	assignment := m.Lease("192.0.2.43")
	if assignment.Status != LeaseActive || assignment.ProxyID != "b" {
		t.Fatalf("assignment = %+v", assignment)
	}
	if proxy := m.proxies["a"]; proxy.Status != ProxyIdle || proxy.FailureCount != 0 {
		t.Fatalf("mock success without evidence attributed ambiguous proxy: %+v", proxy)
	}
}

func TestActiveExitCrossValidatesSingleAmbiguousIdleExit(t *testing.T) {
	cfg := testConfig(2)
	cfg.Proxies[1].Address = "127.0.0.1:1081"
	cfg.FrontProxy = config.FrontProxy{Enabled: true, Protocol: "socks5", Address: "127.0.0.1:11080"}
	m := testManagerFromConfig(t, cfg)
	expires := time.Now().Add(time.Hour)
	m.mu.Lock()
	m.proxies["b"].Status = ProxyActive
	m.proxies["b"].ClientIP = "192.0.2.200"
	m.leases["192.0.2.200"] = &LeaseView{ClientIP: "192.0.2.200", ProxyID: "b", Status: LeaseActive, ExpiresAt: expires}
	m.mu.Unlock()
	var calls atomic.Int64
	m.probeProxy = func(_ context.Context, proxy Proxy) (string, error) {
		calls.Add(1)
		if proxy.ID == "a" {
			return "", &outbound.PhaseError{
				Phase: outbound.PhaseFrontConnectExit,
				Scope: outbound.FailureScopeAmbiguous,
				Token: outbound.FrontToken{Generation: 1, Sequence: 1},
				Err:   errors.New("ambiguous"),
			}
		}
		return "", &outbound.PhaseError{
			Phase:            outbound.PhaseExitAuth,
			Scope:            outbound.FailureScopeExit,
			FrontEstablished: true,
			Token:            outbound.FrontToken{Generation: 1, Sequence: 2},
			Err:              errors.New("exit auth failed"),
		}
	}

	assignment := m.Lease("192.0.2.39")
	if assignment.Status != LeasePending || calls.Load() != 2 {
		t.Fatalf("assignment = %+v, calls = %d", assignment, calls.Load())
	}
	if proxy := m.proxies["a"]; proxy.Status != ProxyUnhealthy || proxy.FailureCount != 1 {
		t.Fatalf("ambiguous idle proxy = %+v", proxy)
	}
	if proxy := m.proxies["b"]; proxy.Status != ProxyActive || proxy.ClientIP != "192.0.2.200" {
		t.Fatalf("active cross-validation proxy = %+v", proxy)
	}
	if retryAfter := m.connector.FrontRetryAfter(); retryAfter != 0 {
		t.Fatalf("active cross-validation opened front circuit for %s", retryAfter)
	}
}

func TestAmbiguousEvidenceIsAppliedPerProbe(t *testing.T) {
	probes := []ambiguousProbe{
		{
			proxyID: "a",
			address: "127.0.0.1:1080",
			token:   outbound.FrontToken{Generation: 2, Sequence: 1},
		},
		{
			proxyID: "c",
			address: "127.0.0.1:1082",
			token:   outbound.FrontToken{Generation: 2, Sequence: 3},
		},
		{
			proxyID: "d",
			address: "127.0.0.1:1083",
			token:   outbound.FrontToken{Generation: 1, Sequence: 1},
		},
	}
	evidence := outbound.FrontEvidence{
		ExitAddress: "127.0.0.1:1081",
		Generation:  2,
		Sequence:    2,
	}

	unresolved, validated := partitionAmbiguousProbesByEvidence(probes, evidence)
	if len(validated) != 1 || validated[0].proxyID != "a" {
		t.Fatalf("validated probes = %+v", validated)
	}
	if len(unresolved) != 2 || unresolved[0].proxyID != "c" || unresolved[1].proxyID != "d" {
		t.Fatalf("unresolved probes = %+v", unresolved)
	}
}

func TestRefreshContextCancellationKeepsOldLease(t *testing.T) {
	m := testManager(t, 2)
	lease := m.Lease("192.0.2.31")
	if lease.Status != LeaseActive || lease.ProxyID != "a" {
		t.Fatalf("lease = %+v", lease)
	}
	m.probeProxy = func(ctx context.Context, _ Proxy) (string, error) {
		return "", ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	refreshed := m.RefreshContext(ctx, "192.0.2.31")
	if refreshed.Status != LeaseActive || refreshed.ProxyID != lease.ProxyID {
		t.Fatalf("refresh = %+v", refreshed)
	}
	m.mu.Lock()
	oldProxy := *m.proxies["a"]
	candidate := *m.proxies["b"]
	m.mu.Unlock()
	if oldProxy.Status != ProxyActive || oldProxy.ClientIP != "192.0.2.31" {
		t.Fatalf("old proxy = %+v", oldProxy)
	}
	if candidate.Status != ProxyIdle || candidate.FailureCount != 0 || candidate.ClientIP != "" {
		t.Fatalf("candidate = %+v", candidate)
	}
}

func TestProbeCancellationDoesNotEnqueueBackgroundRetry(t *testing.T) {
	cfg := testConfig(1)
	cfg.FrontProxy = config.FrontProxy{Enabled: true, Protocol: "socks5", Address: "127.0.0.1:11080"}
	m := testManagerFromConfig(t, cfg)
	started := make(chan struct{})
	var probes atomic.Int64
	m.probeProxy = func(ctx context.Context, _ Proxy) (string, error) {
		if probes.Add(1) == 1 {
			close(started)
		}
		<-ctx.Done()
		return "", ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan Assignment, 1)
	go func() { done <- m.LeaseContext(ctx, "192.0.2.40") }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("probe did not start")
	}
	cancel()
	select {
	case assignment := <-done:
		if assignment.Status != LeasePending {
			t.Fatalf("assignment = %+v", assignment)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled probe did not return")
	}
	time.Sleep(100 * time.Millisecond)
	if got := probes.Load(); got != 1 {
		t.Fatalf("background probe count = %d, want 1", got)
	}
	m.mu.Lock()
	queued := m.hasQueuedLocked()
	proxy := *m.proxies["a"]
	m.mu.Unlock()
	if queued || proxy.Status != ProxyIdle || proxy.ClientIP != "" || proxy.FailureCount != 0 {
		t.Fatalf("canceled probe mutated manager: queued=%v proxy=%+v", queued, proxy)
	}
}

func TestCanceledAmbiguousBatchRestoresOnlyNewPendingState(t *testing.T) {
	cfg := testConfig(1)
	cfg.FrontProxy = config.FrontProxy{Enabled: true, Protocol: "socks5", Address: "127.0.0.1:11080"}
	m := testManagerFromConfig(t, cfg)
	const clientIP = "192.0.2.44"
	probe := ambiguousProbe{
		proxyID: "a",
		address: m.proxies["a"].Address,
		token:   outbound.FrontToken{Generation: 1, Sequence: 1},
		err:     errors.New("ambiguous"),
	}
	m.mu.Lock()
	m.proxies["a"].Status = ProxyChecking
	m.proxies["a"].ClientIP = clientIP
	m.enqueueUniqueLocked(&m.pendingNew, clientIP)
	m.enqueueUniqueLocked(&m.pendingRefs, clientIP)
	m.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	m.resolveAmbiguousLeaseBatch(ctx, clientIP, []ambiguousProbe{probe}, pendingSnapshot{refresh: true})

	m.mu.Lock()
	proxy := *m.proxies["a"]
	newPending := m.inQueueLocked(m.pendingNew, clientIP)
	refreshPending := m.inQueueLocked(m.pendingRefs, clientIP)
	retryScheduled := m.queueRetry || m.queueTimer != nil || !m.queueBackoffUntil.IsZero()
	m.mu.Unlock()
	if proxy.Status != ProxyIdle || proxy.ClientIP != "" || proxy.FailureCount != 0 {
		t.Fatalf("canceled ambiguous proxy = %+v", proxy)
	}
	if newPending || !refreshPending {
		t.Fatalf("pending state after cancellation: new=%v refresh=%v", newPending, refreshPending)
	}
	if retryScheduled || m.connector.FrontRetryAfter() != 0 {
		t.Fatal("canceled ambiguous batch changed retry or circuit state")
	}
}

func TestCanceledContextDoesNotProbeWhenFrontDisabled(t *testing.T) {
	m := testManager(t, 1)
	var probes atomic.Int64
	m.probeProxy = func(context.Context, Proxy) (string, error) {
		probes.Add(1)
		return "", nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assignment := m.LeaseContext(ctx, "192.0.2.41")
	if assignment.Status != LeasePending || probes.Load() != 0 {
		t.Fatalf("assignment = %+v, probes = %d", assignment, probes.Load())
	}
	m.mu.Lock()
	queued := m.hasQueuedLocked()
	m.mu.Unlock()
	if queued {
		t.Fatal("canceled context was added to the queue")
	}
}

func TestAssignmentGateWaitHonorsContextCancellation(t *testing.T) {
	cfg := testConfig(1)
	cfg.FrontProxy = config.FrontProxy{Enabled: true, Protocol: "socks5", Address: "127.0.0.1:11080"}
	m := testManagerFromConfig(t, cfg)
	var probes atomic.Int64
	m.probeProxy = func(context.Context, Proxy) (string, error) {
		probes.Add(1)
		return "198.51.100.91", nil
	}
	m.assignmentGate <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan Assignment, 1)
	go func() {
		done <- m.LeaseContext(ctx, "192.0.2.36")
	}()
	cancel()
	select {
	case assignment := <-done:
		if assignment.Status != LeasePending || assignment.ProxyID != "" {
			t.Fatalf("assignment = %+v", assignment)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled assignment gate wait did not return")
	}
	<-m.assignmentGate
	if got := probes.Load(); got != 0 {
		t.Fatalf("probe calls after gate cancellation = %d", got)
	}
	m.mu.Lock()
	queued := m.hasQueuedLocked()
	proxy := *m.proxies["a"]
	m.mu.Unlock()
	if queued || proxy.Status != ProxyIdle || proxy.ClientIP != "" {
		t.Fatalf("gate cancellation mutated manager: queued=%v proxy=%+v", queued, proxy)
	}
}

func TestCanceledRefreshAtAssignmentGateReturnsActiveLease(t *testing.T) {
	cfg := testConfig(2)
	cfg.FrontProxy = config.FrontProxy{Enabled: true, Protocol: "socks5", Address: "127.0.0.1:11080"}
	m := testManagerFromConfig(t, cfg)
	var probes atomic.Int64
	m.probeProxy = func(context.Context, Proxy) (string, error) {
		probes.Add(1)
		return "198.51.100.92", nil
	}
	lease := m.Lease("192.0.2.37")
	if lease.Status != LeaseActive {
		t.Fatalf("lease = %+v", lease)
	}
	m.assignmentGate <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	refreshed := m.RefreshContext(ctx, "192.0.2.37")
	<-m.assignmentGate
	if refreshed.Status != LeaseActive || refreshed.ProxyID != lease.ProxyID {
		t.Fatalf("refresh = %+v", refreshed)
	}
	if got := probes.Load(); got != 1 {
		t.Fatalf("probe calls = %d, want 1", got)
	}
}

func TestHealthCheckStopsAfterFrontPreflightFailure(t *testing.T) {
	frontAddress, accepts := rejectingFrontProxy(t)
	cfg := testConfig(3)
	cfg.FrontProxy = config.FrontProxy{Enabled: true, Protocol: "socks5", Address: frontAddress}
	cfg.HealthCheck.Concurrency = 3
	m := testManagerFromConfig(t, cfg)

	m.checkAll(context.Background())
	if got := accepts.Load(); got != 1 {
		t.Fatalf("front accepts = %d, want a single preflight", got)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range m.proxyOrder {
		proxy := m.proxies[id]
		if proxy.Status != ProxyIdle || proxy.FailureCount != 0 || proxy.LastHealthDetail != "" {
			t.Fatalf("proxy %s health was polluted: %+v", id, proxy)
		}
	}
}

func TestManagerContextStopsQueueRetryTimer(t *testing.T) {
	frontAddress, _ := rejectingFrontProxy(t)
	cfg := testConfig(1)
	cfg.FrontProxy = config.FrontProxy{Enabled: true, Protocol: "socks5", Address: frontAddress}
	m := testManagerFromConfig(t, cfg)
	m.probeProxy = m.defaultProbeProxy
	if assignment := m.Lease("192.0.2.32"); assignment.Status != LeasePending {
		t.Fatalf("assignment = %+v", assignment)
	}

	ctx, cancel := context.WithCancel(context.Background())
	m.Start(ctx)
	cancel()
	deadline := time.Now().Add(time.Second)
	for {
		m.mu.Lock()
		stopped := m.queueStopped
		timerCleared := !m.queueRetry && m.queueTimer == nil
		m.mu.Unlock()
		if stopped && timerCleared {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("manager context did not stop the queue retry timer")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestManagerContextCancelsActiveQueueProbe(t *testing.T) {
	m := testManager(t, 1)
	started := make(chan struct{})
	probeCanceled := make(chan struct{})
	m.probeProxy = func(ctx context.Context, _ Proxy) (string, error) {
		close(started)
		<-ctx.Done()
		close(probeCanceled)
		return "", ctx.Err()
	}
	m.mu.Lock()
	m.enqueueUniqueLocked(&m.pendingNew, "192.0.2.35")
	m.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	m.Start(ctx)
	m.processQueueAsync()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("queue probe did not start")
	}
	cancel()
	select {
	case <-probeCanceled:
	case <-time.After(time.Second):
		t.Fatal("queue probe was not canceled")
	}
	deadline := time.Now().Add(time.Second)
	for {
		m.mu.Lock()
		active := m.queueActive
		stopped := m.queueStopped
		proxy := *m.proxies["a"]
		m.mu.Unlock()
		if !active && stopped {
			if proxy.Status != ProxyIdle || proxy.ClientIP != "" || proxy.FailureCount != 0 {
				t.Fatalf("canceled queue probe polluted proxy: %+v", proxy)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("queue worker did not stop")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestHealthRoundDiscardsExitResultsAfterFrontFailure(t *testing.T) {
	m := testManager(t, 2)
	m.cfg.HealthCheck.Concurrency = 2
	m.cfg.HealthCheck.FailureThreshold = 1
	exitResultReady := make(chan struct{})
	m.probeProxy = func(_ context.Context, proxy Proxy) (string, error) {
		if proxy.ID == "a" {
			close(exitResultReady)
			return "", errors.New("exit failed")
		}
		<-exitResultReady
		return "", &outbound.PhaseError{Phase: outbound.PhaseFrontDial, Err: errors.New("front failed")}
	}

	m.checkAll(context.Background())
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range m.proxyOrder {
		proxy := m.proxies[id]
		if proxy.Status != ProxyIdle || proxy.FailureCount != 0 || proxy.LastHealthDetail != "" {
			t.Fatalf("proxy %s health was committed after front failure: %+v", id, proxy)
		}
	}
}

func TestHealthRoundDoesNotUsePrevalidationExitErrorAsFrontEvidence(t *testing.T) {
	m := testManager(t, 2)
	m.cfg.HealthCheck.Concurrency = 2
	m.cfg.HealthCheck.FailureThreshold = 1
	m.probeProxy = func(_ context.Context, proxy Proxy) (string, error) {
		if proxy.ID == "a" {
			return "", &outbound.PhaseError{
				Phase: outbound.PhaseFrontConnectExit,
				Scope: outbound.FailureScopeAmbiguous,
				Token: outbound.FrontToken{Generation: 1, Sequence: 1},
				Err:   errors.New("ambiguous"),
			}
		}
		return "", &outbound.PhaseError{
			Phase: outbound.PhaseExitDial,
			Scope: outbound.FailureScopeExit,
			Err:   errors.New("invalid exit before front connect"),
		}
	}

	m.checkAll(context.Background())
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range m.proxyOrder {
		proxy := m.proxies[id]
		if proxy.Status != ProxyIdle || proxy.FailureCount != 0 {
			t.Fatalf("proxy %s used non-causal evidence: %+v", id, proxy)
		}
	}
}

func TestHealthRoundAttributesAmbiguousFailureWithEstablishedFront(t *testing.T) {
	m := testManager(t, 2)
	m.proxies["b"].Address = "127.0.0.1:1081"
	m.cfg.HealthCheck.Concurrency = 2
	m.cfg.HealthCheck.FailureThreshold = 1
	m.probeProxy = func(_ context.Context, proxy Proxy) (string, error) {
		if proxy.ID == "a" {
			return "", &outbound.PhaseError{
				Phase: outbound.PhaseFrontConnectExit,
				Scope: outbound.FailureScopeAmbiguous,
				Token: outbound.FrontToken{Generation: 1, Sequence: 1},
				Err:   errors.New("ambiguous"),
			}
		}
		return "", &outbound.PhaseError{
			Phase:            outbound.PhaseExitAuth,
			Scope:            outbound.FailureScopeExit,
			FrontEstablished: true,
			Token:            outbound.FrontToken{Generation: 1, Sequence: 2},
			Err:              errors.New("exit auth failed"),
		}
	}

	m.checkAll(context.Background())
	if proxy := m.proxies["a"]; proxy.Status != ProxyUnhealthy || proxy.FailureCount != 1 {
		t.Fatalf("ambiguous proxy was not attributed: %+v", proxy)
	}
}

func TestHealthRoundRequiresCausalEvidenceFromDifferentEndpoint(t *testing.T) {
	tests := []struct {
		name          string
		secondAddress string
		evidenceToken outbound.FrontToken
	}{
		{
			name:          "older generation",
			secondAddress: "127.0.0.1:1081",
			evidenceToken: outbound.FrontToken{Generation: 1, Sequence: 99},
		},
		{
			name:          "same endpoint",
			secondAddress: "127.0.0.1:1080",
			evidenceToken: outbound.FrontToken{Generation: 2, Sequence: 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := testManager(t, 2)
			m.proxies["b"].Address = tt.secondAddress
			m.cfg.HealthCheck.Concurrency = 2
			m.cfg.HealthCheck.FailureThreshold = 1
			m.probeProxy = func(_ context.Context, proxy Proxy) (string, error) {
				if proxy.ID == "a" {
					return "", &outbound.PhaseError{
						Phase: outbound.PhaseFrontConnectExit,
						Scope: outbound.FailureScopeAmbiguous,
						Token: outbound.FrontToken{Generation: 2, Sequence: 1},
						Err:   errors.New("ambiguous"),
					}
				}
				return "", &outbound.PhaseError{
					Phase:            outbound.PhaseExitAuth,
					Scope:            outbound.FailureScopeExit,
					FrontEstablished: true,
					Token:            tt.evidenceToken,
					Err:              errors.New("exit auth failed"),
				}
			}

			m.checkAll(context.Background())
			if proxy := m.proxies["a"]; proxy.Status != ProxyIdle || proxy.FailureCount != 0 {
				t.Fatalf("non-causal evidence attributed ambiguous proxy: %+v", proxy)
			}
		})
	}
}

func TestCanceledHealthRoundCannotCommitAmbiguousCircuit(t *testing.T) {
	frontAddress, _ := connectRejectingFrontProxy(t)
	cfg := testConfig(2)
	cfg.Proxies[1].Address = "127.0.0.1:1081"
	cfg.FrontProxy = config.FrontProxy{Enabled: true, Protocol: "socks5", Address: frontAddress}
	m := testManagerFromConfig(t, cfg)
	tokens := make([]outbound.FrontToken, 0, len(cfg.Proxies))
	for _, proxy := range cfg.Proxies {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := m.connector.Connect(ctx, outbound.Endpoint{Address: proxy.Address}, "example.com:80")
		cancel()
		if outbound.FailureScopeOf(err) != outbound.FailureScopeAmbiguous {
			t.Fatalf("proxy %s failure = %v, want ambiguous", proxy.ID, err)
		}
		token, ok := outbound.AmbiguousFailureToken(err)
		if !ok {
			t.Fatalf("proxy %s missing ambiguous token: %v", proxy.ID, err)
		}
		tokens = append(tokens, token)
	}
	roundCtx, cancelRound := context.WithCancel(context.Background())
	cancelRound()

	m.recordAmbiguousHealthBatch(roundCtx, tokens[0], tokens[1])

	if retryAfter := m.connector.FrontRetryAfter(); retryAfter != 0 {
		t.Fatalf("canceled health round opened front circuit for %s", retryAfter)
	}
	if status := m.connector.FrontStatus(); status.Status != "unknown" {
		t.Fatalf("front status after canceled health round = %+v", status)
	}
}

func TestFrontAddressCannotBeUsedAsExit(t *testing.T) {
	const frontAddress = "127.0.0.1:11080"
	cfg := testConfig(1)
	cfg.FrontProxy = config.FrontProxy{Enabled: true, Protocol: "socks5", Address: frontAddress}
	m := testManagerFromConfig(t, cfg)

	if _, err := m.AddProxy(context.Background(), ProxyInput{ID: "b", Address: frontAddress}); err == nil {
		t.Fatal("expected AddProxy front address conflict")
	}
	if _, err := m.UpdateProxy(context.Background(), "a", ProxyInput{Address: frontAddress}); err == nil {
		t.Fatal("expected UpdateProxy front address conflict")
	}
	if got := m.proxies["a"].Address; got == frontAddress {
		t.Fatalf("failed update changed proxy address to %q", got)
	}
}

func TestNewRejectsSeedProxyAtFrontAddress(t *testing.T) {
	const frontAddress = "127.0.0.1:11080"
	cfg := testConfig(0)
	cfg.FrontProxy = config.FrontProxy{Enabled: true, Protocol: "socks5", Address: frontAddress}
	cfg.Proxies = []config.ProxyConfig{{ID: "same", Address: frontAddress}}
	db, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := New(cfg, db); err == nil {
		t.Fatal("expected seed proxy front address conflict")
	}
}

func TestIdleProxyWithActiveLeaseIsNotReassigned(t *testing.T) {
	m := testManager(t, 1)
	first := m.Lease("192.0.2.10")
	if first.Status != LeaseActive || first.ProxyID != "a" {
		t.Fatalf("first = %+v", first)
	}
	m.mu.Lock()
	m.proxies["a"].Status = ProxyIdle
	m.proxies["a"].ClientIP = ""
	m.mu.Unlock()

	second := m.Lease("192.0.2.11")
	if second.Status != LeasePending {
		t.Fatalf("second = %+v", second)
	}

	m.mu.Lock()
	m.releaseLocked(context.Background(), "192.0.2.11", false)
	if m.proxies["a"].Status != ProxyIdle {
		t.Fatalf("proxy status = %s", m.proxies["a"].Status)
	}
	m.mu.Unlock()
	current := m.Current("192.0.2.10")
	if current.Status != LeaseActive || current.ProxyID != "a" {
		t.Fatalf("current = %+v", current)
	}
}

func TestExtractIPSupportsTextAndJSON(t *testing.T) {
	cases := map[string]string{
		"198.51.100.10\n":               "198.51.100.10",
		`{"ip":"198.51.100.11"}`:        "198.51.100.11",
		`{"query":"198.51.100.12"}`:     "198.51.100.12",
		`{"origin":"198.51.100.13, x"}`: "198.51.100.13",
	}
	for body, want := range cases {
		if got := extractIP([]byte(body)); got != want {
			t.Fatalf("extractIP(%q) = %q, want %q", body, got, want)
		}
	}
}

func TestParseKookeeyProxyLine(t *testing.T) {
	in, err := parseProxyLine("socks5://mobile.kookeey.info:1086:example-user:example-password")
	if err != nil {
		t.Fatal(err)
	}
	if in.Address != "mobile.kookeey.info:1086" {
		t.Fatalf("address = %q", in.Address)
	}
	if in.Username != "example-user" {
		t.Fatalf("username = %q", in.Username)
	}
	if in.Password != "example-password" {
		t.Fatalf("password = %q", in.Password)
	}
	if in.ID == "" {
		t.Fatal("empty id")
	}
}

func TestImportProxiesSkipsDuplicates(t *testing.T) {
	m := testManager(t, 0)
	result := m.ImportProxies(context.Background(), "127.0.0.1:1080:u:p\n127.0.0.1:1080:u:p\n")
	if result.Imported != 1 || result.Skipped != 1 {
		t.Fatalf("result = %+v", result)
	}
}

func TestAdminLeaseRequiresIPv4AndAssignsProxy(t *testing.T) {
	m := testManager(t, 1)
	if _, err := m.AdminLease("not-an-ip"); err == nil {
		t.Fatal("expected invalid ip error")
	}
	assignment, err := m.AdminLease("192.0.2.20")
	if err != nil {
		t.Fatal(err)
	}
	if assignment.Status != LeaseActive || assignment.ProxyID != "a" {
		t.Fatalf("assignment = %+v", assignment)
	}
}

func TestAdminRefreshRequiresIPv4AndRefreshesProxy(t *testing.T) {
	m := testManager(t, 2)
	if _, err := m.AdminRefresh("not-an-ip"); err == nil {
		t.Fatal("expected invalid ip error")
	}
	lease := m.Lease("192.0.2.20")
	if lease.ProxyID != "a" {
		t.Fatalf("lease = %+v", lease)
	}
	refreshed, err := m.AdminRefresh("192.0.2.20")
	if err != nil {
		t.Fatal(err)
	}
	if refreshed.Status != LeaseActive || refreshed.ProxyID != "b" {
		t.Fatalf("refresh = %+v", refreshed)
	}
	if m.proxies["b"].ExitIP != "203.0.113.200" {
		t.Fatalf("exit ip = %q", m.proxies["b"].ExitIP)
	}
}

func TestImportProxyPayloadKookeeyJSON(t *testing.T) {
	m := testManager(t, 0)
	payload := []byte(`{"success":true,"data":[{"username":"u","password":"p","ip":"mobile.kookeey.info","port":1086}]}`)
	result := m.ImportProxyPayload(context.Background(), payload)
	if result.Imported != 1 || result.Skipped != 0 {
		t.Fatalf("result = %+v", result)
	}
	status := m.Status()
	proxies := status["proxies"].([]Proxy)
	if len(proxies) != 1 {
		t.Fatalf("proxies = %+v", proxies)
	}
	if proxies[0].Address != "mobile.kookeey.info:1086" {
		t.Fatalf("address = %q", proxies[0].Address)
	}
}

func TestBatchDisableAndDelete(t *testing.T) {
	m := testManager(t, 2)
	disabled := m.SetProxiesDisabled(context.Background(), []string{"a", "b"}, true)
	if disabled.Updated != 2 || disabled.Skipped != 0 {
		t.Fatalf("disabled = %+v", disabled)
	}
	if m.proxies["a"].Status != ProxyDisabled || m.proxies["b"].Status != ProxyDisabled {
		t.Fatalf("statuses = %s %s", m.proxies["a"].Status, m.proxies["b"].Status)
	}
	deleted := m.DeleteProxies(context.Background(), []string{"a", "b"})
	if deleted.Deleted != 2 || deleted.Skipped != 0 {
		t.Fatalf("deleted = %+v", deleted)
	}
}

func TestClearIdleProxiesSkipsActive(t *testing.T) {
	m := testManager(t, 2)
	lease := m.Lease("192.0.2.10")
	if lease.ProxyID != "a" {
		t.Fatalf("lease = %+v", lease)
	}
	result := m.ClearIdleProxies(context.Background())
	if result.Deleted != 1 || result.Skipped != 0 {
		t.Fatalf("result = %+v", result)
	}
	status := m.Status()
	proxies := status["proxies"].([]Proxy)
	if len(proxies) != 1 || proxies[0].ID != "a" {
		t.Fatalf("proxies = %+v", proxies)
	}
}
