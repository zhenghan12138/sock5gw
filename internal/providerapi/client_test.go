package providerapi

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"sock5gw/internal/config"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestAcquireMapsConfiguredQueryParameters(t *testing.T) {
	var query url.Values
	client := &Client{
		cfg: normalizedConfig(config.ProxyAPI{
			Enabled:       true,
			URL:           "https://provider.example/api?region=Rand&time=5&num=1&type=json",
			CountryParam:  "region",
			DurationParam: "time",
		}),
		httpClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			query = request.URL.Query()
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`[{"host":"198.51.100.10","port":"1080"}]`)),
				Header:     make(http.Header),
			}, nil
		})},
	}
	endpoint, err := client.Acquire(context.Background(), "US", 10)
	if err != nil {
		t.Fatal(err)
	}
	if endpoint.Address != "198.51.100.10:1080" {
		t.Fatalf("endpoint = %+v", endpoint)
	}
	if query.Get("region") != "US" || query.Get("time") != "10" || query.Get("num") != "1" {
		t.Fatalf("query = %#v", query)
	}
}

func TestParseResponseFormats(t *testing.T) {
	tests := []struct {
		name string
		body string
		want Endpoint
	}{
		{"top-level array", `[{"host":"198.51.100.10","port":"1080"}]`, Endpoint{Address: "198.51.100.10:1080"}},
		{"top-level object", `{"ip":"198.51.100.11","port":1081,"username":"u","password":"p"}`, Endpoint{Address: "198.51.100.11:1081", Username: "u", Password: "p"}},
		{"data array", `{"data":[{"host":"proxy.example","port":"1082"}]}`, Endpoint{Address: "proxy.example:1082"}},
		{"data string", `{"data":"198.51.100.14:1085"}`, Endpoint{Address: "198.51.100.14:1085"}},
		{"plain text", `198.51.100.12:1083:user:secret`, Endpoint{Address: "198.51.100.12:1083", Username: "user", Password: "secret"}},
		{"socks URL", `socks5://user:secret@198.51.100.13:1084`, Endpoint{Address: "198.51.100.13:1084", Username: "user", Password: "secret"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ParseResponse([]byte(test.body))
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("endpoint = %+v, want %+v", got, test.want)
			}
		})
	}
}

func TestParseResponseRequiresExactlyOneEndpoint(t *testing.T) {
	for _, body := range []string{
		`[]`,
		`[{"host":"198.51.100.10","port":1080},{"host":"198.51.100.11","port":1080}]`,
		`not-a-proxy`,
	} {
		if _, err := ParseResponse([]byte(body)); err == nil {
			t.Fatalf("expected %q to fail", body)
		}
	}
}

func TestParseResponseReturnsProviderErrorMessage(t *testing.T) {
	_, err := ParseResponse([]byte(`{"code":401,"msg":"IP is not whitelisted"}`))
	if err == nil || !strings.Contains(err.Error(), "IP is not whitelisted") {
		t.Fatalf("error = %v", err)
	}
}

func TestParseResponseReturnsPlainTextProviderError(t *testing.T) {
	_, err := ParseResponse([]byte(`203.0.113.1 not added to whitelist`))
	if err == nil || !strings.Contains(err.Error(), "not added to whitelist") {
		t.Fatalf("error = %v", err)
	}
}

func TestNewRoutesRequestsThroughFrontProxy(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	targets := make(chan string, 1)
	serverErrors := make(chan error, 1)
	go serveSingleSOCKS5(listener, targets, serverErrors)

	client, err := New(config.ProxyAPI{}, config.FrontProxy{
		Enabled:  true,
		Protocol: "socks5",
		Address:  listener.Addr().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	transport, ok := client.httpClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T", client.httpClient.Transport)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := transport.DialContext(ctx, "tcp", "198.51.100.10:443")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	select {
	case target := <-targets:
		if target != "198.51.100.10:443" {
			t.Fatalf("SOCKS5 target = %q", target)
		}
	case err := <-serverErrors:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func serveSingleSOCKS5(listener net.Listener, targets chan<- string, errors chan<- error) {
	conn, err := listener.Accept()
	if err != nil {
		errors <- err
		return
	}
	defer conn.Close()
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil {
		errors <- err
		return
	}
	methods := make([]byte, int(header[1]))
	if _, err := io.ReadFull(conn, methods); err != nil {
		errors <- err
		return
	}
	if header[0] != 5 {
		errors <- fmt.Errorf("SOCKS version = %d", header[0])
		return
	}
	if _, err := conn.Write([]byte{5, 0}); err != nil {
		errors <- err
		return
	}
	request := make([]byte, 4)
	if _, err := io.ReadFull(conn, request); err != nil {
		errors <- err
		return
	}
	var host string
	switch request[3] {
	case 1:
		address := make([]byte, net.IPv4len)
		if _, err := io.ReadFull(conn, address); err != nil {
			errors <- err
			return
		}
		host = net.IP(address).String()
	case 4:
		address := make([]byte, net.IPv6len)
		if _, err := io.ReadFull(conn, address); err != nil {
			errors <- err
			return
		}
		host = net.IP(address).String()
	default:
		errors <- fmt.Errorf("SOCKS address type = %d", request[3])
		return
	}
	portBytes := make([]byte, 2)
	if _, err := io.ReadFull(conn, portBytes); err != nil {
		errors <- err
		return
	}
	if _, err := conn.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 0}); err != nil {
		errors <- err
		return
	}
	targets <- net.JoinHostPort(host, fmt.Sprint(binary.BigEndian.Uint16(portBytes)))
}

func TestNewRejectsUnsafeURL(t *testing.T) {
	for _, rawURL := range []string{
		"http://provider.example/api",
		"https://user:secret@provider.example/api",
		"https://provider.example/api#fragment",
	} {
		_, err := New(config.ProxyAPI{Enabled: true, URL: rawURL})
		if err == nil {
			t.Fatalf("expected %q to fail", rawURL)
		}
	}
}
