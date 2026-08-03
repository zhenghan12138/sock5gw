package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestActivateDynamicLeasePersistsAndDeletesMetadata(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	now := time.Now().Truncate(time.Second)
	proxy := Proxy{ID: "api-test", Address: "198.51.100.10:1080"}
	dynamic := DynamicProxy{
		ProxyID:           proxy.ID,
		Country:           "US",
		DurationMinutes:   10,
		ProviderExpiresAt: now.Add(10 * time.Minute),
	}
	lease := Lease{
		ClientIP:  "192.0.2.10",
		ProxyID:   proxy.ID,
		Status:    "active",
		ExpiresAt: dynamic.ProviderExpiresAt,
		UpdatedAt: now,
	}
	if err := db.ActivateDynamicLease(context.Background(), proxy, dynamic, lease, ""); err != nil {
		t.Fatal(err)
	}
	proxies, err := db.ListDynamicProxies(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(proxies) != 1 || proxies[0].ProxyID != proxy.ID || proxies[0].Country != "US" {
		t.Fatalf("dynamic proxies = %+v", proxies)
	}
	leases, err := db.ListLeases(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(leases) != 1 || leases[0].ProxyID != proxy.ID {
		t.Fatalf("leases = %+v", leases)
	}
	if err := db.DeleteLeaseAndProxy(context.Background(), lease.ClientIP, proxy.ID); err != nil {
		t.Fatal(err)
	}
	proxies, err = db.ListDynamicProxies(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(proxies) != 0 {
		t.Fatalf("dynamic proxies after delete = %+v", proxies)
	}
}
