package providerapi

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	xproxy "golang.org/x/net/proxy"

	"sock5gw/internal/config"
)

const (
	requestTimeout  = 15 * time.Second
	maxResponseBody = 64 * 1024
)

type ErrorKind string

const (
	ErrorRequest  ErrorKind = "request"
	ErrorResponse ErrorKind = "response"
)

type Error struct {
	Kind ErrorKind
	Err  error
}

func (e *Error) Error() string {
	if e == nil || e.Err == nil {
		return "proxy API failed"
	}
	return e.Err.Error()
}

func (e *Error) Unwrap() error { return e.Err }

func ErrorKindOf(err error) ErrorKind {
	var apiErr *Error
	if errors.As(err, &apiErr) {
		return apiErr.Kind
	}
	return ErrorRequest
}

type Endpoint struct {
	Address  string
	Username string
	Password string
}

type Client struct {
	cfg        config.ProxyAPI
	httpClient *http.Client
}

func New(cfg config.ProxyAPI, frontProxy ...config.FrontProxy) (*Client, error) {
	cfg = normalizedConfig(cfg)
	if err := config.ValidateProxyAPI(cfg); err != nil {
		return nil, err
	}
	var front config.FrontProxy
	if len(frontProxy) > 0 {
		front = frontProxy[0]
	}
	httpClient, err := newHTTPClient(front)
	if err != nil {
		return nil, err
	}
	return &Client{cfg: cfg, httpClient: httpClient}, nil
}

func normalizedConfig(cfg config.ProxyAPI) config.ProxyAPI {
	cfg.URL = strings.TrimSpace(cfg.URL)
	cfg.CountryParam = strings.TrimSpace(cfg.CountryParam)
	cfg.DurationParam = strings.TrimSpace(cfg.DurationParam)
	if cfg.CountryParam == "" {
		cfg.CountryParam = "region"
	}
	if cfg.DurationParam == "" {
		cfg.DurationParam = "time"
	}
	return cfg
}

func (c *Client) Acquire(ctx context.Context, country string, durationMinutes int64) (Endpoint, error) {
	if c == nil || !c.cfg.Enabled {
		return Endpoint{}, &Error{Kind: ErrorRequest, Err: errors.New("proxy API is disabled")}
	}
	parsed, err := url.Parse(c.cfg.URL)
	if err != nil {
		return Endpoint{}, &Error{Kind: ErrorRequest, Err: errors.New("invalid proxy API URL")}
	}
	query := parsed.Query()
	query.Set(c.cfg.CountryParam, country)
	query.Set(c.cfg.DurationParam, strconv.FormatInt(durationMinutes, 10))
	parsed.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return Endpoint{}, &Error{Kind: ErrorRequest, Err: fmt.Errorf("create proxy API request: %w", err)}
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return Endpoint{}, &Error{Kind: ErrorRequest, Err: errors.New("proxy API request failed")}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody+1))
	if err != nil {
		return Endpoint{}, &Error{Kind: ErrorResponse, Err: fmt.Errorf("read proxy API response: %w", err)}
	}
	if len(body) > maxResponseBody {
		return Endpoint{}, &Error{Kind: ErrorResponse, Err: errors.New("proxy API response exceeds 64 KiB")}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Endpoint{}, &Error{Kind: ErrorRequest, Err: fmt.Errorf("proxy API returned HTTP %d", resp.StatusCode)}
	}
	endpoint, err := ParseResponse(body)
	if err != nil {
		return Endpoint{}, &Error{Kind: ErrorResponse, Err: err}
	}
	return endpoint, nil
}

func ParseResponse(body []byte) (Endpoint, error) {
	text := strings.TrimSpace(string(body))
	body = []byte(text)
	if len(body) == 0 {
		return Endpoint{}, errors.New("proxy API returned an empty response")
	}
	if body[0] == '[' || body[0] == '{' || body[0] == '"' {
		endpoints, err := parseJSONResponse(body)
		if err != nil {
			return Endpoint{}, err
		}
		return exactlyOne(endpoints)
	}
	endpoints := parseTextResponse(text)
	if len(endpoints) == 0 {
		return Endpoint{}, fmt.Errorf("proxy API rejected the request: %s", sanitizeProviderMessage(text))
	}
	return exactlyOne(endpoints)
}

func parseJSONResponse(body []byte) ([]Endpoint, error) {
	var value any
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return endpointsFromJSON(value)
}

func endpointsFromJSON(value any) ([]Endpoint, error) {
	switch typed := value.(type) {
	case []any:
		var out []Endpoint
		for _, item := range typed {
			endpoints, err := endpointsFromJSON(item)
			if err != nil {
				return nil, err
			}
			out = append(out, endpoints...)
		}
		return out, nil
	case map[string]any:
		if data, ok := typed["data"]; ok {
			endpoints, err := endpointsFromJSON(data)
			if err == nil && len(endpoints) > 0 {
				return endpoints, nil
			}
			if message := providerErrorMessage(typed); message != "" {
				return nil, fmt.Errorf("proxy API rejected the request: %s", message)
			}
			return endpoints, err
		}
		endpoint, err := endpointFromObject(typed)
		if err != nil {
			if message := providerErrorMessage(typed); message != "" {
				return nil, fmt.Errorf("proxy API rejected the request: %s", message)
			}
			return nil, err
		}
		return []Endpoint{endpoint}, nil
	case string:
		endpoints := parseTextResponse(typed)
		if len(endpoints) == 0 {
			return nil, errors.New("proxy API JSON string does not contain an endpoint")
		}
		return endpoints, nil
	default:
		return nil, errors.New("proxy API JSON must contain an endpoint object or array")
	}
}

func providerErrorMessage(object map[string]any) string {
	for _, key := range []string{"message", "msg", "error", "reason"} {
		message := sanitizeProviderMessage(stringValue(object[key]))
		if message != "" {
			return message
		}
	}
	return ""
}

func sanitizeProviderMessage(message string) string {
	message = strings.TrimSpace(message)
	message = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, message)
	if len(message) > 200 {
		message = message[:200]
	}
	return message
}

func endpointFromObject(value any) (Endpoint, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return Endpoint{}, errors.New("proxy API endpoint must be an object")
	}
	host := stringValue(object["host"])
	if host == "" {
		host = stringValue(object["ip"])
	}
	port := stringValue(object["port"])
	return validateEndpoint(host, port, stringValue(object["username"]), stringValue(object["password"]))
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		return ""
	}
}

func parseTextResponse(text string) []Endpoint {
	var endpoints []Endpoint
	scanner := bufio.NewScanner(strings.NewReader(text))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		endpoint, err := parseTextEndpoint(line)
		if err == nil {
			endpoints = append(endpoints, endpoint)
		}
	}
	return endpoints
}

func parseTextEndpoint(raw string) (Endpoint, error) {
	if strings.Contains(raw, "://") {
		parsed, err := url.Parse(raw)
		if err != nil || (parsed.Scheme != "socks5" && parsed.Scheme != "socks") {
			return Endpoint{}, errors.New("unsupported proxy URL")
		}
		username, password := "", ""
		if parsed.User != nil {
			username = parsed.User.Username()
			password, _ = parsed.User.Password()
		}
		return validateEndpoint(parsed.Hostname(), parsed.Port(), username, password)
	}
	parts := strings.Split(raw, ":")
	if len(parts) < 2 {
		return Endpoint{}, errors.New("expected host:port[:username:password]")
	}
	username, password := "", ""
	if len(parts) >= 3 {
		username = parts[2]
	}
	if len(parts) >= 4 {
		password = strings.Join(parts[3:], ":")
	}
	return validateEndpoint(parts[0], parts[1], username, password)
}

func validateEndpoint(host, port, username, password string) (Endpoint, error) {
	host = strings.TrimSpace(host)
	port = strings.TrimSpace(port)
	if host == "" || strings.ContainsAny(host, " \t\r\n") {
		return Endpoint{}, errors.New("proxy API endpoint host is invalid")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return Endpoint{}, errors.New("proxy API endpoint port is invalid")
	}
	if len(username) > 255 || len(password) > 255 {
		return Endpoint{}, errors.New("proxy API endpoint credentials are too long")
	}
	return Endpoint{
		Address:  net.JoinHostPort(host, strconv.Itoa(portNumber)),
		Username: username,
		Password: password,
	}, nil
}

func exactlyOne(endpoints []Endpoint) (Endpoint, error) {
	if len(endpoints) != 1 {
		return Endpoint{}, fmt.Errorf("proxy API must return exactly one endpoint, got %d", len(endpoints))
	}
	return endpoints[0], nil
}

func newHTTPClient(front config.FrontProxy) (*http.Client, error) {
	directDialer := &net.Dialer{Timeout: requestTimeout}
	dialContext := directDialer.DialContext
	if front.Enabled {
		front.Protocol = strings.ToLower(strings.TrimSpace(front.Protocol))
		front.Address = strings.TrimSpace(front.Address)
		if front.Protocol == "" {
			front.Protocol = "socks5"
		}
		if front.Protocol != "socks5" {
			return nil, fmt.Errorf("unsupported front proxy protocol %q", front.Protocol)
		}
		if _, _, err := net.SplitHostPort(front.Address); err != nil {
			return nil, fmt.Errorf("front proxy address: %w", err)
		}
		var auth *xproxy.Auth
		if front.Username != "" || front.Password != "" {
			auth = &xproxy.Auth{User: front.Username, Password: front.Password}
		}
		socksDialer, err := xproxy.SOCKS5("tcp", front.Address, auth, directDialer)
		if err != nil {
			return nil, fmt.Errorf("create front proxy dialer: %w", err)
		}
		contextDialer, ok := socksDialer.(xproxy.ContextDialer)
		if !ok {
			return nil, errors.New("front proxy dialer does not support context cancellation")
		}
		dialContext = contextDialer.DialContext
	}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			ips, err := resolvePublicIPs(ctx, host)
			if err != nil {
				return nil, err
			}
			var lastErr error
			for _, ip := range ips {
				conn, err := dialContext(ctx, network, net.JoinHostPort(ip.String(), port))
				if err == nil {
					return conn, nil
				}
				lastErr = err
			}
			return nil, lastErr
		},
		TLSHandshakeTimeout: requestTimeout,
	}
	return &http.Client{
		Timeout:   requestTimeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return errors.New("too many proxy API redirects")
			}
			if req.URL.Scheme != "https" || req.URL.Hostname() == "" || req.URL.User != nil {
				return errors.New("proxy API redirect must use a public https URL")
			}
			return nil
		},
	}, nil
}

func resolvePublicIPs(ctx context.Context, host string) ([]net.IP, error) {
	if parsed := net.ParseIP(host); parsed != nil {
		if !isPublicIP(parsed) {
			return nil, errors.New("proxy API URL resolved to a non-public address")
		}
		return []net.IP{parsed}, nil
	}
	addresses, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	var out []net.IP
	for _, address := range addresses {
		if isPublicIP(address.IP) {
			out = append(out, address.IP)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("proxy API URL did not resolve to a public address")
	}
	return out, nil
}

func isPublicIP(ip net.IP) bool {
	return ip != nil && ip.IsGlobalUnicast() && !ip.IsPrivate() && !ip.IsLoopback() &&
		!ip.IsLinkLocalUnicast() && !ip.IsUnspecified()
}
