package manager

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"sock5gw/internal/outbound"
)

func TestHealthRoundKeepsSuccessAlongsideUnresolvedAmbiguousFailure(t *testing.T) {
	m := testManager(t, 2)
	m.cfg.HealthCheck.Concurrency = 1
	m.probeProxy = func(_ context.Context, proxy Proxy) (string, error) {
		if proxy.ID == "a" {
			return "", &outbound.PhaseError{
				Phase: outbound.PhaseFrontConnectExit,
				Scope: outbound.FailureScopeAmbiguous,
				Token: outbound.FrontToken{Generation: 1, Sequence: 1},
				Err:   errors.New("ambiguous"),
			}
		}
		return "203.0.113.42", nil
	}

	m.checkAll(context.Background())

	m.mu.Lock()
	defer m.mu.Unlock()
	if proxy := m.proxies["a"]; proxy.Status != ProxyIdle || proxy.FailureCount != 0 || proxy.SuccessCount != 0 {
		t.Fatalf("ambiguous proxy was mutated: %+v", proxy)
	}
	if proxy := m.proxies["b"]; proxy.Status != ProxyIdle || proxy.SuccessCount != 1 || proxy.ExitIP != "203.0.113.42" {
		t.Fatalf("confirmed success was discarded: %+v", proxy)
	}
}

func TestAmbiguousFrontFailureStopsAfterTwoDistinctExits(t *testing.T) {
	frontAddress, accepts := connectRejectingFrontProxy(t)
	cfg := testConfig(5)
	for index := range cfg.Proxies {
		cfg.Proxies[index].Address = net.JoinHostPort("127.0.0.1", strconv.Itoa(1080+index))
	}
	cfg.FrontProxy.Enabled = true
	cfg.FrontProxy.Protocol = "socks5"
	cfg.FrontProxy.Address = frontAddress
	m := testManagerFromConfig(t, cfg)
	m.probeProxy = m.defaultProbeProxy

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	assignment := m.LeaseContext(ctx, "192.0.2.99")
	if assignment.Status != LeasePending {
		t.Fatalf("assignment = %+v, want pending", assignment)
	}
	if got := accepts.Load(); got != 2 {
		t.Fatalf("front attempts = %d, want 2", got)
	}
}

func TestExitIPProbeHonorsCancellationAfterSOCKSConnect(t *testing.T) {
	address, requestReady := hangingSOCKS5HTTPProxy(t)
	m := testManager(t, 0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		_, err := m.probeExitIP(ctx, Proxy{Address: address}, "http://example.com/ip", 30*time.Second)
		done <- err
	}()

	select {
	case <-requestReady:
	case <-time.After(time.Second):
		t.Fatal("exit-IP probe did not send its HTTP request")
	}
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("probe error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("exit-IP probe did not stop after cancellation")
	}
}

func hangingSOCKS5HTTPProxy(t *testing.T) (string, <-chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	requestReady := make(chan struct{})

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		greeting := make([]byte, 2)
		if _, err := io.ReadFull(reader, greeting); err != nil || greeting[0] != 5 {
			return
		}
		if _, err := io.CopyN(io.Discard, reader, int64(greeting[1])); err != nil {
			return
		}
		if _, err := conn.Write([]byte{5, 0}); err != nil {
			return
		}
		header := make([]byte, 4)
		if _, err := io.ReadFull(reader, header); err != nil {
			return
		}
		addressLength := 0
		switch header[3] {
		case 1:
			addressLength = 4
		case 3:
			length := []byte{0}
			if _, err := io.ReadFull(reader, length); err != nil {
				return
			}
			addressLength = int(length[0])
		case 4:
			addressLength = 16
		default:
			return
		}
		if _, err := io.CopyN(io.Discard, reader, int64(addressLength+2)); err != nil {
			return
		}
		if _, err := conn.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, 0, 80}); err != nil {
			return
		}
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			if line == "\r\n" {
				close(requestReady)
				_, _ = io.Copy(io.Discard, reader)
				return
			}
		}
	}()

	return listener.Addr().String(), requestReady
}
