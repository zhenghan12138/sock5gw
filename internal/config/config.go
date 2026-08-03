package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DBPath      string        `json:"db_path"`
	LeaseTTL    Duration      `json:"lease_ttl"`
	HealthCheck HealthCheck   `json:"health_check"`
	API         API           `json:"api"`
	Gateway     Gateway       `json:"gateway"`
	FrontProxy  FrontProxy    `json:"front_proxy"`
	ProxyAPI    ProxyAPI      `json:"proxy_api"`
	DNS         DNS           `json:"dns"`
	Routing     Routing       `json:"routing"`
	Proxies     []ProxyConfig `json:"proxies"`
}

type FrontProxy struct {
	Enabled  bool   `json:"enabled"`
	Protocol string `json:"protocol"`
	Address  string `json:"address"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	FailOpen bool   `json:"fail_open"`
}

type ProxyAPI struct {
	Enabled       bool   `json:"enabled"`
	URL           string `json:"url"`
	CountryParam  string `json:"country_param"`
	DurationParam string `json:"duration_param"`
}

type API struct {
	Listen      string `json:"listen"`
	ClientKey   string `json:"client_key"`
	AdminKey    string `json:"admin_key"`
	TrustProxy  bool   `json:"trust_proxy_headers"`
	GeoIPDBPath string `json:"geoip_db_path"`
}

type Gateway struct {
	Listen           string   `json:"listen"`
	TransparentProxy bool     `json:"transparent_proxy"`
	BlockedPorts     []string `json:"blocked_ports"`
	DialTimeout      Duration `json:"dial_timeout"`
	IdleTimeout      Duration `json:"idle_timeout"`
}

type DNS struct {
	Listen      string   `json:"listen"`
	FakeIPCIDR  string   `json:"fake_ip_cidr"`
	Upstream    string   `json:"upstream"`
	BlockedQTyp []string `json:"blocked_qtypes"`
}

type Routing struct {
	Enabled       bool          `json:"enabled"`
	GeositePath   string        `json:"geosite_path"`
	DefaultAction string        `json:"default_action"`
	DirectDomains []string      `json:"direct_domains"`
	ProxyDomains  []string      `json:"proxy_domains"`
	BlockDomains  []string      `json:"block_domains"`
	Rules         []RoutingRule `json:"rules"`
}

type RoutingRule struct {
	Type   string `json:"type"`
	Value  string `json:"value"`
	Action string `json:"action"`
}

type HealthCheck struct {
	Enabled          bool     `json:"enabled"`
	Interval         Duration `json:"interval"`
	Timeout          Duration `json:"timeout"`
	Concurrency      int      `json:"concurrency"`
	TargetHost       string   `json:"target_host"`
	TargetPort       int      `json:"target_port"`
	ExitIPURL        string   `json:"exit_ip_url"`
	FailureThreshold int      `json:"failure_threshold"`
	SuccessThreshold int      `json:"success_threshold"`
}

type ProxyConfig struct {
	ID       string `json:"id"`
	Address  string `json:"address"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

type Duration struct {
	time.Duration
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	cfg.setDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (d *Duration) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		v, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		d.Duration = v
		return nil
	}
	var n int64
	if err := json.Unmarshal(data, &n); err != nil {
		return err
	}
	d.Duration = time.Duration(n) * time.Second
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

func (cfg *Config) setDefaults() {
	if cfg.DBPath == "" {
		cfg.DBPath = "sock5gw.db"
	}
	if cfg.LeaseTTL.Duration == 0 {
		cfg.LeaseTTL.Duration = 24 * time.Hour
	}
	if cfg.API.Listen == "" {
		cfg.API.Listen = "127.0.0.1:8080"
	}
	if cfg.Gateway.Listen == "" {
		cfg.Gateway.Listen = "0.0.0.0:15001"
	}
	if cfg.Gateway.DialTimeout.Duration == 0 {
		cfg.Gateway.DialTimeout.Duration = 10 * time.Second
	}
	if cfg.Gateway.IdleTimeout.Duration == 0 {
		cfg.Gateway.IdleTimeout.Duration = 2 * time.Minute
	}
	if cfg.FrontProxy.Enabled && cfg.FrontProxy.Protocol == "" {
		cfg.FrontProxy.Protocol = "socks5"
	}
	if cfg.ProxyAPI.CountryParam == "" {
		cfg.ProxyAPI.CountryParam = "region"
	}
	if cfg.ProxyAPI.DurationParam == "" {
		cfg.ProxyAPI.DurationParam = "time"
	}
	if cfg.DNS.Listen == "" {
		cfg.DNS.Listen = "0.0.0.0:5353"
	}
	if cfg.DNS.FakeIPCIDR == "" {
		cfg.DNS.FakeIPCIDR = "198.18.0.0/15"
	}
	if cfg.DNS.Upstream == "" {
		cfg.DNS.Upstream = "1.1.1.1:53"
	}
	if cfg.Routing.DefaultAction == "" {
		cfg.Routing.DefaultAction = "proxy"
	}
	if cfg.HealthCheck.Interval.Duration == 0 {
		cfg.HealthCheck.Interval.Duration = 30 * time.Second
	}
	if cfg.HealthCheck.Timeout.Duration == 0 {
		cfg.HealthCheck.Timeout.Duration = 5 * time.Second
	}
	if cfg.HealthCheck.Concurrency == 0 {
		cfg.HealthCheck.Concurrency = 50
	}
	if cfg.HealthCheck.TargetHost == "" {
		cfg.HealthCheck.TargetHost = "example.com"
	}
	if cfg.HealthCheck.TargetPort == 0 {
		cfg.HealthCheck.TargetPort = 80
	}
	if cfg.HealthCheck.ExitIPURL == "" {
		cfg.HealthCheck.ExitIPURL = "http://api.ipify.org/"
	}
	if cfg.HealthCheck.FailureThreshold == 0 {
		cfg.HealthCheck.FailureThreshold = 3
	}
	if cfg.HealthCheck.SuccessThreshold == 0 {
		cfg.HealthCheck.SuccessThreshold = 2
	}
}

func (cfg *Config) validate() error {
	if cfg.API.ClientKey == "" {
		return errors.New("api.client_key is required")
	}
	if cfg.API.AdminKey == "" {
		return errors.New("api.admin_key is required")
	}
	if _, _, err := net.ParseCIDR(cfg.DNS.FakeIPCIDR); err != nil {
		return fmt.Errorf("dns.fake_ip_cidr: %w", err)
	}
	if err := validateFrontProxy(cfg.FrontProxy); err != nil {
		return err
	}
	if err := ValidateProxyAPI(cfg.ProxyAPI); err != nil {
		return err
	}
	if err := validateRoutingAction("routing.default_action", cfg.Routing.DefaultAction); err != nil {
		return err
	}
	for i, rule := range cfg.Routing.Rules {
		if err := validateRoutingAction(fmt.Sprintf("routing.rules[%d].action", i), rule.Action); err != nil {
			return err
		}
	}
	seen := map[string]bool{}
	for _, p := range cfg.Proxies {
		if p.ID == "" {
			return errors.New("proxy id is required")
		}
		if seen[p.ID] {
			return fmt.Errorf("duplicate proxy id %q", p.ID)
		}
		seen[p.ID] = true
		if _, _, err := net.SplitHostPort(p.Address); err != nil {
			return fmt.Errorf("proxy %s address: %w", p.ID, err)
		}
	}
	return nil
}

func ValidateProxyAPI(proxyAPI ProxyAPI) error {
	proxyAPI.URL = strings.TrimSpace(proxyAPI.URL)
	proxyAPI.CountryParam = strings.TrimSpace(proxyAPI.CountryParam)
	proxyAPI.DurationParam = strings.TrimSpace(proxyAPI.DurationParam)
	if proxyAPI.CountryParam == "" {
		return errors.New("proxy_api.country_param is required")
	}
	if proxyAPI.DurationParam == "" {
		return errors.New("proxy_api.duration_param is required")
	}
	if proxyAPI.CountryParam == proxyAPI.DurationParam {
		return errors.New("proxy_api country and duration parameters must differ")
	}
	if strings.ContainsAny(proxyAPI.CountryParam+proxyAPI.DurationParam, "\r\n") {
		return errors.New("proxy_api parameter names must not contain newlines")
	}
	if proxyAPI.URL == "" {
		if proxyAPI.Enabled {
			return errors.New("proxy_api.url is required when enabled")
		}
		return nil
	}
	parsed, err := url.Parse(proxyAPI.URL)
	if err != nil || parsed.Opaque != "" || parsed.Hostname() == "" {
		return errors.New("proxy_api.url must be a valid absolute URL")
	}
	if strings.ToLower(parsed.Scheme) != "https" {
		return errors.New("proxy_api.url must use https://")
	}
	if parsed.User != nil {
		return errors.New("proxy_api.url must not contain user info")
	}
	if parsed.Fragment != "" {
		return errors.New("proxy_api.url must not contain a fragment")
	}
	return nil
}

func validateFrontProxy(front FrontProxy) error {
	if front.FailOpen {
		return errors.New("front_proxy.fail_open must be false")
	}
	if !front.Enabled {
		return nil
	}
	if front.Protocol != "socks5" {
		return errors.New("front_proxy.protocol must be socks5")
	}
	host, portText, err := net.SplitHostPort(front.Address)
	if err != nil {
		return fmt.Errorf("front_proxy.address: %w", err)
	}
	if host == "" || strings.ContainsAny(host, " \t\r\n") {
		return errors.New("front_proxy.address host is required and must not contain whitespace")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return errors.New("front_proxy.address port must be an integer between 1 and 65535")
	}
	if len(front.Username) > 255 {
		return errors.New("front_proxy.username must not exceed 255 bytes")
	}
	if len(front.Password) > 255 {
		return errors.New("front_proxy.password must not exceed 255 bytes")
	}
	return nil
}

func validateRoutingAction(name string, action string) error {
	switch action {
	case "", "direct", "proxy", "block":
		return nil
	default:
		return fmt.Errorf("%s must be direct, proxy, or block", name)
	}
}
