package manager

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"sock5gw/internal/config"
	"sock5gw/internal/providerapi"
	"sock5gw/internal/store"
)

type proxyAPIOutcome struct {
	endpoint providerapi.Endpoint
	err      error
}

type fakeProxyAPI struct {
	mu       sync.Mutex
	outcomes []proxyAPIOutcome
	requests []DynamicLeaseRequest
	delay    time.Duration
}

func (f *fakeProxyAPI) Acquire(ctx context.Context, country string, durationMinutes int64) (providerapi.Endpoint, error) {
	f.mu.Lock()
	index := len(f.requests)
	f.requests = append(f.requests, DynamicLeaseRequest{Country: country, DurationMinutes: durationMinutes})
	var outcome proxyAPIOutcome
	if len(f.outcomes) > 0 {
		outcome = f.outcomes[min(index, len(f.outcomes)-1)]
	}
	delay := f.delay
	f.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return providerapi.Endpoint{}, ctx.Err()
		}
	}
	return outcome.endpoint, outcome.err
}

func (f *fakeProxyAPI) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

func dynamicTestConfig(path string) *config.Config {
	cfg := testConfig(0)
	cfg.DBPath = path
	cfg.ProxyAPI = config.ProxyAPI{
		Enabled:       true,
		URL:           "https://provider.example/api?num=1&type=json",
		CountryParam:  "region",
		DurationParam: "time",
	}
	return cfg
}

func setFakeProxyAPI(m *Manager, fake proxyAPIAcquirer) {
	m.proxyAPIMu.Lock()
	m.proxyAPIClient = fake
	m.proxyAPIMu.Unlock()
}

func TestDynamicLeaseIsIdempotentAndIsolatedFromPool(t *testing.T) {
	m := testManagerFromConfig(t, dynamicTestConfig(":memory:"))
	fake := &fakeProxyAPI{outcomes: []proxyAPIOutcome{
		{endpoint: providerapi.Endpoint{Address: "198.51.100.10:1080"}},
		{endpoint: providerapi.Endpoint{Address: "198.51.100.11:1080"}},
	}}
	setFakeProxyAPI(m, fake)

	request := DynamicLeaseRequest{Country: "us", DurationMinutes: 10}
	first, err := m.LeaseDynamicContext(context.Background(), "192.0.2.10", request, false)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != LeaseActive || first.Mode != ProxySourceAPI || first.Country != "US" {
		t.Fatalf("first lease = %+v", first)
	}
	second, err := m.LeaseDynamicContext(context.Background(), "192.0.2.10", request, false)
	if err != nil {
		t.Fatal(err)
	}
	if second.ProxyID != first.ProxyID || fake.callCount() != 1 {
		t.Fatalf("idempotent lease = %+v, calls = %d", second, fake.callCount())
	}
	if pooled := m.Lease("192.0.2.11"); pooled.Status != LeasePending {
		t.Fatalf("dynamic proxy entered regular pool: %+v", pooled)
	}

	replaced, err := m.LeaseDynamicContext(context.Background(), "192.0.2.10", DynamicLeaseRequest{Country: "JP", DurationMinutes: 20}, false)
	if err != nil {
		t.Fatal(err)
	}
	if replaced.ProxyID == first.ProxyID || replaced.Country != "JP" || fake.callCount() != 2 {
		t.Fatalf("replacement = %+v, calls = %d", replaced, fake.callCount())
	}
	if _, exists := m.proxies[first.ProxyID]; exists {
		t.Fatalf("old dynamic proxy %q was not removed", first.ProxyID)
	}
	m.Release("192.0.2.10")
	if _, exists := m.proxies[replaced.ProxyID]; exists {
		t.Fatalf("released dynamic proxy %q was not removed", replaced.ProxyID)
	}
}

func TestDynamicLeaseFailurePreservesCurrentLease(t *testing.T) {
	m := testManagerFromConfig(t, dynamicTestConfig(":memory:"))
	fake := &fakeProxyAPI{outcomes: []proxyAPIOutcome{
		{endpoint: providerapi.Endpoint{Address: "198.51.100.20:1080"}},
		{err: errors.New("provider unavailable")},
	}}
	setFakeProxyAPI(m, fake)
	first, err := m.LeaseDynamicContext(context.Background(), "192.0.2.20", DynamicLeaseRequest{Country: "US", DurationMinutes: 10}, false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = m.LeaseDynamicContext(context.Background(), "192.0.2.20", DynamicLeaseRequest{Country: "JP", DurationMinutes: 10}, false)
	var apiErr *LeaseAPIError
	if !errors.As(err, &apiErr) || apiErr.Code != "proxy_api_request_failed" {
		t.Fatalf("error = %#v", err)
	}
	if apiErr.CurrentLease == nil || apiErr.CurrentLease.ProxyID != first.ProxyID {
		t.Fatalf("current lease in error = %+v", apiErr.CurrentLease)
	}
	if current := m.Current("192.0.2.20"); current.ProxyID != first.ProxyID || current.Status != LeaseActive {
		t.Fatalf("current lease changed after failure: %+v", current)
	}
}

func TestDynamicRefreshDrainsThenDeletesOldProxy(t *testing.T) {
	m := testManagerFromConfig(t, dynamicTestConfig(":memory:"))
	fake := &fakeProxyAPI{outcomes: []proxyAPIOutcome{
		{endpoint: providerapi.Endpoint{Address: "198.51.100.30:1080"}},
		{endpoint: providerapi.Endpoint{Address: "198.51.100.31:1080"}},
	}}
	setFakeProxyAPI(m, fake)
	request := DynamicLeaseRequest{Country: "US", DurationMinutes: 10}
	first, err := m.LeaseDynamicContext(context.Background(), "192.0.2.30", request, false)
	if err != nil {
		t.Fatal(err)
	}
	client, peer := net.Pipe()
	defer client.Close()
	defer peer.Close()
	if _, err := m.AcquireProxyConn("192.0.2.30", client); err != nil {
		t.Fatal(err)
	}
	if _, err := m.LeaseDynamicContext(context.Background(), "192.0.2.30", request, true); err != nil {
		t.Fatal(err)
	}
	if old := m.proxies[first.ProxyID]; old == nil || old.Status != ProxyDraining {
		t.Fatalf("old proxy = %+v", old)
	}
	m.UnregisterConn("192.0.2.30", first.ProxyID, client)
	if _, exists := m.proxies[first.ProxyID]; exists {
		t.Fatal("drained dynamic proxy was not removed")
	}
	stored, err := m.db.ListDynamicProxies(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(stored) != 1 {
		t.Fatalf("stored dynamic proxies = %+v", stored)
	}
}

func TestConcurrentDynamicLeaseOnlyCallsProviderOnce(t *testing.T) {
	m := testManagerFromConfig(t, dynamicTestConfig(":memory:"))
	fake := &fakeProxyAPI{
		outcomes: []proxyAPIOutcome{{endpoint: providerapi.Endpoint{Address: "198.51.100.40:1080"}}},
		delay:    20 * time.Millisecond,
	}
	setFakeProxyAPI(m, fake)
	request := DynamicLeaseRequest{Country: "US", DurationMinutes: 10}
	results := make(chan Assignment, 2)
	errorsCh := make(chan error, 2)
	for range 2 {
		go func() {
			assignment, err := m.LeaseDynamicContext(context.Background(), "192.0.2.40", request, false)
			results <- assignment
			errorsCh <- err
		}()
	}
	first, second := <-results, <-results
	if err := <-errorsCh; err != nil {
		t.Fatal(err)
	}
	if err := <-errorsCh; err != nil {
		t.Fatal(err)
	}
	if first.ProxyID == "" || first.ProxyID != second.ProxyID || fake.callCount() != 1 {
		t.Fatalf("results = %+v / %+v, calls = %d", first, second, fake.callCount())
	}
}

func TestDynamicLeaseRestoresAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manager.db")
	cfg := dynamicTestConfig(path)
	db, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	m, err := New(cfg, db)
	if err != nil {
		t.Fatal(err)
	}
	m.probeProxy = func(context.Context, Proxy) (string, error) { return "203.0.113.50", nil }
	setFakeProxyAPI(m, &fakeProxyAPI{outcomes: []proxyAPIOutcome{{endpoint: providerapi.Endpoint{Address: "198.51.100.50:1080"}}}})
	created, err := m.LeaseDynamicContext(context.Background(), "192.0.2.50", DynamicLeaseRequest{Country: "US", DurationMinutes: 10}, false)
	if err != nil {
		t.Fatal(err)
	}
	m.stopQueueProcessing()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	restored, err := New(cfg, db)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.stopQueueProcessing()
	current := restored.Current("192.0.2.50")
	if current.ProxyID != created.ProxyID || current.Mode != ProxySourceAPI || current.Country != "US" {
		t.Fatalf("restored lease = %+v", current)
	}
}
