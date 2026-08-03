package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadFrontProxy(t *testing.T) {
	tests := []struct {
		name     string
		front    string
		enabled  bool
		protocol string
	}{
		{
			name: "legacy config without front proxy",
		},
		{
			name:     "enabled front proxy gets default protocol",
			front:    `,"front_proxy":{"enabled":true,"address":"127.0.0.1:11080"}`,
			enabled:  true,
			protocol: "socks5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			data := `{"api":{"client_key":"client-key","admin_key":"admin-key"}` + tt.front + `}`
			if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
				t.Fatalf("write config: %v", err)
			}

			cfg, err := Load(path)
			if err != nil {
				t.Fatalf("load config: %v", err)
			}
			if cfg.FrontProxy.Enabled != tt.enabled {
				t.Fatalf("front proxy enabled = %v, want %v", cfg.FrontProxy.Enabled, tt.enabled)
			}
			if cfg.FrontProxy.Protocol != tt.protocol {
				t.Fatalf("front proxy protocol = %q, want %q", cfg.FrontProxy.Protocol, tt.protocol)
			}
		})
	}
}

func TestFrontProxyDefaults(t *testing.T) {
	cfg := validTestConfig()
	cfg.FrontProxy = FrontProxy{
		Enabled: true,
		Address: "127.0.0.1:11080",
	}

	cfg.setDefaults()
	if cfg.FrontProxy.Protocol != "socks5" {
		t.Fatalf("front proxy protocol = %q, want socks5", cfg.FrontProxy.Protocol)
	}
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate front proxy: %v", err)
	}
}

func TestFrontProxyValidation(t *testing.T) {
	tests := []struct {
		name  string
		front FrontProxy
		want  string
	}{
		{
			name: "valid hostname",
			front: FrontProxy{
				Enabled:  true,
				Protocol: "socks5",
				Address:  "localhost:11080",
			},
		},
		{
			name: "valid ipv6",
			front: FrontProxy{
				Enabled:  true,
				Protocol: "socks5",
				Address:  "[::1]:11080",
			},
		},
		{
			name: "credentials at byte limit",
			front: FrontProxy{
				Enabled:  true,
				Protocol: "socks5",
				Address:  "127.0.0.1:11080",
				Username: strings.Repeat("界", 85),
				Password: strings.Repeat("p", 255),
			},
		},
		{
			name: "unsupported protocol",
			front: FrontProxy{
				Enabled:  true,
				Protocol: "http",
				Address:  "127.0.0.1:11080",
			},
			want: "front_proxy.protocol must be socks5",
		},
		{
			name: "missing address",
			front: FrontProxy{
				Enabled:  true,
				Protocol: "socks5",
			},
			want: "front_proxy.address",
		},
		{
			name: "missing host",
			front: FrontProxy{
				Enabled:  true,
				Protocol: "socks5",
				Address:  ":11080",
			},
			want: "front_proxy.address host is required",
		},
		{
			name: "host contains whitespace",
			front: FrontProxy{
				Enabled:  true,
				Protocol: "socks5",
				Address:  "bad host:11080",
			},
			want: "front_proxy.address host is required",
		},
		{
			name: "port is not numeric",
			front: FrontProxy{
				Enabled:  true,
				Protocol: "socks5",
				Address:  "localhost:http",
			},
			want: "front_proxy.address port must be an integer",
		},
		{
			name: "port is zero",
			front: FrontProxy{
				Enabled:  true,
				Protocol: "socks5",
				Address:  "localhost:0",
			},
			want: "front_proxy.address port must be an integer",
		},
		{
			name: "port is too large",
			front: FrontProxy{
				Enabled:  true,
				Protocol: "socks5",
				Address:  "localhost:65536",
			},
			want: "front_proxy.address port must be an integer",
		},
		{
			name: "username is too long in bytes",
			front: FrontProxy{
				Enabled:  true,
				Protocol: "socks5",
				Address:  "127.0.0.1:11080",
				Username: strings.Repeat("界", 86),
			},
			want: "front_proxy.username must not exceed 255 bytes",
		},
		{
			name: "password is too long",
			front: FrontProxy{
				Enabled:  true,
				Protocol: "socks5",
				Address:  "127.0.0.1:11080",
				Password: strings.Repeat("p", 256),
			},
			want: "front_proxy.password must not exceed 255 bytes",
		},
		{
			name: "fail open is rejected while enabled",
			front: FrontProxy{
				Enabled:  true,
				Protocol: "socks5",
				Address:  "127.0.0.1:11080",
				FailOpen: true,
			},
			want: "front_proxy.fail_open must be false",
		},
		{
			name: "fail open is rejected while disabled",
			front: FrontProxy{
				FailOpen: true,
			},
			want: "front_proxy.fail_open must be false",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validTestConfig()
			cfg.FrontProxy = tt.front

			err := cfg.validate()
			if tt.want == "" {
				if err != nil {
					t.Fatalf("validate front proxy: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("validate front proxy error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestDisabledFrontProxyPreservesCompatibility(t *testing.T) {
	cfg := validTestConfig()
	cfg.FrontProxy = FrontProxy{
		Protocol: "unused-protocol",
		Address:  "not-an-address",
		Username: strings.Repeat("u", 256),
		Password: strings.Repeat("p", 256),
	}

	cfg.setDefaults()
	if cfg.FrontProxy.Protocol != "unused-protocol" {
		t.Fatalf("disabled front proxy protocol changed to %q", cfg.FrontProxy.Protocol)
	}
	if err := cfg.validate(); err != nil {
		t.Fatalf("disabled front proxy should be ignored: %v", err)
	}
}

func TestProxyAPIDefaultsAndValidation(t *testing.T) {
	cfg := validTestConfig()
	cfg.ProxyAPI = ProxyAPI{
		Enabled: true,
		URL:     "https://white.1024proxy.com/white/api?num=1&type=json",
	}
	cfg.setDefaults()
	if cfg.ProxyAPI.CountryParam != "region" || cfg.ProxyAPI.DurationParam != "time" {
		t.Fatalf("proxy API defaults = %+v", cfg.ProxyAPI)
	}
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}

	cfg.ProxyAPI.URL = "http://provider.example/api"
	if err := cfg.validate(); err == nil || !strings.Contains(err.Error(), "https://") {
		t.Fatalf("unsafe URL error = %v", err)
	}
	cfg.ProxyAPI.URL = "https://provider.example/api"
	cfg.ProxyAPI.DurationParam = cfg.ProxyAPI.CountryParam
	if err := cfg.validate(); err == nil || !strings.Contains(err.Error(), "must differ") {
		t.Fatalf("duplicate mapping error = %v", err)
	}
}

func validTestConfig() *Config {
	cfg := &Config{
		API: API{
			ClientKey: "client-key",
			AdminKey:  "admin-key",
		},
	}
	cfg.setDefaults()
	return cfg
}
