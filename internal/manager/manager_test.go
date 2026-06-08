package manager

import (
	"context"
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
	return m
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

	current := m.Current("192.0.2.11")
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
	current := m.Current("192.0.2.11")
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
