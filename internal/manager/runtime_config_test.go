package manager

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
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
	}, nil)

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

func TestRuntimeConfigUpdateFrontProxyPreservesModeAndSecrets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	initial := map[string]any{
		"api": map[string]any{
			"client_key": "client",
			"admin_key":  "admin",
		},
	}
	data, err := json.Marshal(initial)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{}
	var applied config.FrontProxy
	rt := NewRuntimeConfig(path, cfg, nil, func(next config.FrontProxy, persist func() error) error {
		if err := persist(); err != nil {
			return err
		}
		applied = next
		return nil
	})
	next := config.FrontProxy{
		Enabled:  true,
		Protocol: "socks5",
		Address:  "127.0.0.1:11080",
		Username: "front-user",
		Password: "front-secret",
	}
	if err := rt.UpdateFrontProxy(next); err != nil {
		t.Fatal(err)
	}
	if applied != next || rt.FrontProxy() != next {
		t.Fatalf("front proxy was not applied: applied=%+v current=%+v", applied, rt.FrontProxy())
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0600 {
		t.Fatalf("config mode = %o, want 600", got)
	}
	saved := string(mustReadFile(t, path))
	if !strings.Contains(saved, `"password": "front-secret"`) {
		t.Fatalf("saved config does not contain updated credentials: %s", saved)
	}
}

func TestRuntimeConfigDoesNotCommitFrontProxyWhenApplyFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"front_proxy":{"enabled":false}}`), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{}
	rt := NewRuntimeConfig(path, cfg, nil, func(config.FrontProxy, func() error) error {
		return errors.New("conflict")
	})
	next := config.FrontProxy{Enabled: true, Protocol: "socks5", Address: "127.0.0.1:11080"}
	if err := rt.UpdateFrontProxy(next); err == nil {
		t.Fatal("expected apply failure")
	}
	if rt.FrontProxy().Enabled {
		t.Fatal("failed update changed runtime front proxy")
	}
	if strings.Contains(string(mustReadFile(t, path)), "127.0.0.1:11080") {
		t.Fatal("failed update changed persisted config")
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
