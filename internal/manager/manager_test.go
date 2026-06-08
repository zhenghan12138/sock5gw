package manager

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"sock5gw/internal/config"
	"sock5gw/internal/store"
)

func testManager(t *testing.T, proxies int) *Manager {
	t.Helper()
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
	}
	for i := 0; i < proxies; i++ {
		cfg.Proxies = append(cfg.Proxies, config.ProxyConfig{
			ID:      string(rune('a' + i)),
			Address: "127.0.0.1:1080",
		})
	}
	db, err := store.Open(cfg.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m, err := New(cfg, db)
	if err != nil {
		t.Fatal(err)
	}
	m.probeProxy = func(context.Context, Proxy) (string, error) {
		return "203.0.113.200", nil
	}
	return m
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

func TestRefreshKeepsOldProxyDrainingUntilConnectionCloses(t *testing.T) {
	m := testManager(t, 2)
	lease := m.Lease("192.0.2.10")
	if lease.ProxyID != "a" {
		t.Fatalf("lease = %+v", lease)
	}
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	m.RegisterConn("192.0.2.10", "a", c1)

	refreshed := m.Refresh("192.0.2.10")
	if refreshed.Status != LeaseActive || refreshed.ProxyID != "b" {
		t.Fatalf("refresh = %+v", refreshed)
	}
	if m.proxies["a"].Status != ProxyDraining {
		t.Fatalf("old proxy status = %s", m.proxies["a"].Status)
	}

	m.UnregisterConn("192.0.2.10", "a", c1)
	if m.proxies["a"].Status != ProxyIdle {
		t.Fatalf("old proxy after close = %s", m.proxies["a"].Status)
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
	in, err := parseProxyLine("socks5://mobile.kookeey.info:1086:4423363-07c57c6f:06c79d64-global-97891462")
	if err != nil {
		t.Fatal(err)
	}
	if in.Address != "mobile.kookeey.info:1086" {
		t.Fatalf("address = %q", in.Address)
	}
	if in.Username != "4423363-07c57c6f" {
		t.Fatalf("username = %q", in.Username)
	}
	if in.Password != "06c79d64-global-97891462" {
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
