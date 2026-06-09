package manager

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"sock5gw/internal/config"
	"sock5gw/internal/routing"
)

func TestRuntimeConfigUpdateRoutingWritesConfigAndAppliesMatcher(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	initial := map[string]any{
		"db_path":   ":memory:",
		"lease_ttl": "24h",
		"api": map[string]any{
			"client_key": "client",
			"admin_key":  "admin",
		},
		"dns": map[string]any{
			"fake_ip_cidr": "198.18.0.0/15",
		},
	}
	data, err := json.Marshal(initial)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Routing: config.Routing{Enabled: false, DefaultAction: routing.ActionProxy}}
	applied := false
	rt := NewRuntimeConfig(path, cfg, func(m *routing.Matcher) {
		applied = true
		if got := m.ActionFor("example.cn:443"); got != routing.ActionDirect {
			t.Fatalf("matcher action = %q", got)
		}
	})

	next := config.Routing{
		Enabled:       true,
		DefaultAction: routing.ActionProxy,
		Rules: []config.RoutingRule{
			{Type: "domain_suffix", Value: "example.cn", Action: routing.ActionDirect},
		},
	}
	if err := rt.UpdateRouting(next); err != nil {
		t.Fatal(err)
	}
	if !applied {
		t.Fatal("matcher was not applied")
	}
	var saved map[string]any
	if err := json.Unmarshal(mustReadFile(t, path), &saved); err != nil {
		t.Fatal(err)
	}
	routingRaw := saved["routing"].(map[string]any)
	if routingRaw["enabled"] != true || routingRaw["default_action"] != routing.ActionProxy {
		t.Fatalf("routing = %#v", routingRaw)
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
