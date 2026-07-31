package gateway

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"sock5gw/internal/config"
	"sock5gw/internal/manager"
	"sock5gw/internal/routing"
)

type gatewayManagerStub struct {
	proxy      manager.Proxy
	upstream   net.Conn
	connect    chan gatewayConnectCall
	acquire    chan string
	unregister chan string
	fake       *manager.FakeIPStore
	acquired   atomic.Bool
}

type gatewayConnectCall struct {
	proxy       manager.Proxy
	target      string
	hasDeadline bool
}

func (m *gatewayManagerStub) AcquireProxyConn(clientIP string, _ net.Conn) (*manager.Proxy, error) {
	m.acquired.Store(true)
	m.acquire <- clientIP + "/" + m.proxy.ID
	proxy := m.proxy
	return &proxy, nil
}

func (m *gatewayManagerStub) ConnectProxy(ctx context.Context, proxy manager.Proxy, target string) (net.Conn, error) {
	_, hasDeadline := ctx.Deadline()
	m.connect <- gatewayConnectCall{proxy: proxy, target: target, hasDeadline: hasDeadline}
	if !m.acquired.Load() {
		return nil, errors.New("connector called before connection acquisition")
	}
	if m.upstream == nil {
		return nil, errors.New("unexpected connector call")
	}
	return m.upstream, nil
}

func (m *gatewayManagerStub) UnregisterConn(clientIP, proxyID string, _ net.Conn) {
	m.unregister <- clientIP + "/" + proxyID
}

func (m *gatewayManagerStub) FakeIPStore() *manager.FakeIPStore { return m.fake }

type addressedConn struct {
	net.Conn
	local  net.Addr
	remote net.Addr
}

func (c addressedConn) LocalAddr() net.Addr  { return c.local }
func (c addressedConn) RemoteAddr() net.Addr { return c.remote }

func TestHandleProxyUsesManagerConnectorAndRelays(t *testing.T) {
	client, gatewayClient := net.Pipe()
	upstream, exit := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = gatewayClient.Close()
		_ = upstream.Close()
		_ = exit.Close()
	})

	stub := &gatewayManagerStub{
		proxy: manager.Proxy{
			ID:       "leased-exit",
			Address:  "exit.example:1080",
			Username: "exit-user",
			Password: "exit-password",
		},
		upstream:   upstream,
		connect:    make(chan gatewayConnectCall, 1),
		acquire:    make(chan string, 1),
		unregister: make(chan string, 1),
	}
	gateway := New(config.Gateway{
		DialTimeout: config.Duration{Duration: time.Second},
		IdleTimeout: config.Duration{Duration: time.Second},
	}, config.DNS{}, stub, nil)

	done := make(chan struct{})
	go func() {
		gateway.handleProxy(context.Background(), gatewayClient, "192.0.2.10", "target.example:443")
		close(done)
	}()

	select {
	case acquired := <-stub.acquire:
		if acquired != "192.0.2.10/leased-exit" {
			t.Fatalf("acquired = %q", acquired)
		}
	case <-time.After(time.Second):
		t.Fatal("connection was not acquired")
	}

	select {
	case call := <-stub.connect:
		if call.target != "target.example:443" || call.proxy != stub.proxy || !call.hasDeadline {
			t.Fatalf("connector call = %+v", call)
		}
	case <-time.After(time.Second):
		t.Fatal("manager connector was not called")
	}

	request := []byte("through-chain")
	if _, err := client.Write(request); err != nil {
		t.Fatal(err)
	}
	received := make([]byte, len(request))
	if _, err := io.ReadFull(exit, received); err != nil {
		t.Fatal(err)
	}
	if string(received) != string(request) {
		t.Fatalf("relayed request = %q", received)
	}

	response := []byte("from-exit")
	go func() { _, _ = exit.Write(response) }()
	received = make([]byte, len(response))
	if _, err := io.ReadFull(client, received); err != nil {
		t.Fatal(err)
	}
	if string(received) != string(response) {
		t.Fatalf("relayed response = %q", received)
	}

	_ = client.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("proxy handler did not stop after client close")
	}
	select {
	case unregistered := <-stub.unregister:
		if unregistered != "192.0.2.10/leased-exit" {
			t.Fatalf("unregistered = %q", unregistered)
		}
	case <-time.After(time.Second):
		t.Fatal("connection was not unregistered")
	}
}

func TestHandleProxyReleasesAcquiredConnectionWhenDialFails(t *testing.T) {
	client, gatewayClient := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = gatewayClient.Close()
	})

	stub := &gatewayManagerStub{
		proxy: manager.Proxy{
			ID:      "leased-exit",
			Address: "exit.example:1080",
		},
		connect:    make(chan gatewayConnectCall, 1),
		acquire:    make(chan string, 1),
		unregister: make(chan string, 1),
	}
	gateway := New(config.Gateway{
		DialTimeout: config.Duration{Duration: time.Second},
		IdleTimeout: config.Duration{Duration: time.Second},
	}, config.DNS{}, stub, nil)

	done := make(chan struct{})
	go func() {
		gateway.handleProxy(context.Background(), gatewayClient, "192.0.2.12", "target.example:443")
		close(done)
	}()

	select {
	case acquired := <-stub.acquire:
		if acquired != "192.0.2.12/leased-exit" {
			t.Fatalf("acquired = %q", acquired)
		}
	case <-time.After(time.Second):
		t.Fatal("connection was not acquired before dial")
	}
	select {
	case call := <-stub.connect:
		if call.proxy != stub.proxy || call.target != "target.example:443" || !call.hasDeadline {
			t.Fatalf("connector call = %+v", call)
		}
	case <-time.After(time.Second):
		t.Fatal("manager connector was not called")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("proxy handler did not stop after dial failure")
	}
	select {
	case unregistered := <-stub.unregister:
		if unregistered != "192.0.2.12/leased-exit" {
			t.Fatalf("unregistered = %q", unregistered)
		}
	case <-time.After(time.Second):
		t.Fatal("failed dial did not release the acquired connection")
	}
}

func TestDirectRouteBypassesManagerConnector(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	request := []byte("direct-request")
	response := []byte("direct-response")
	serverDone := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverDone <- acceptErr
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		got := make([]byte, len(request))
		if _, readErr := io.ReadFull(conn, got); readErr != nil {
			serverDone <- readErr
			return
		}
		if string(got) != string(request) {
			serverDone <- errors.New("unexpected direct request")
			return
		}
		_, writeErr := conn.Write(response)
		serverDone <- writeErr
	}()

	fake, err := manager.NewFakeIPStore("198.18.0.0/15")
	if err != nil {
		t.Fatal(err)
	}
	stub := &gatewayManagerStub{
		connect:    make(chan gatewayConnectCall, 1),
		acquire:    make(chan string, 1),
		unregister: make(chan string, 1),
		fake:       fake,
	}
	router, err := routing.New(config.Routing{Enabled: true, DefaultAction: routing.ActionDirect})
	if err != nil {
		t.Fatal(err)
	}
	gateway := New(config.Gateway{
		DialTimeout: config.Duration{Duration: time.Second},
		IdleTimeout: config.Duration{Duration: time.Second},
	}, config.DNS{}, stub, router)

	client, gatewayClient := net.Pipe()
	defer client.Close()
	_ = client.SetDeadline(time.Now().Add(time.Second))
	wrapped := addressedConn{
		Conn:   gatewayClient,
		local:  listener.Addr(),
		remote: &net.TCPAddr{IP: net.ParseIP("192.0.2.11"), Port: 12345},
	}
	done := make(chan struct{})
	go func() {
		gateway.handle(context.Background(), wrapped)
		close(done)
	}()

	if _, err := client.Write(request); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(response))
	if _, err := io.ReadFull(client, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(response) {
		t.Fatalf("direct response = %q", got)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	_ = client.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("direct handler did not stop")
	}
	select {
	case call := <-stub.connect:
		t.Fatalf("direct route called manager connector: %+v", call)
	default:
	}
	select {
	case acquired := <-stub.acquire:
		t.Fatalf("direct route acquired proxy connection %q", acquired)
	default:
	}
}
