package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"sock5gw/internal/config"
)

func TestConnectorDoubleSOCKS5(t *testing.T) {
	exit := startTestSOCKSServer(t, socksServerOptions{
		username: "exit-user",
		password: "exit-password",
		echo:     true,
	})
	front := startTestSOCKSServer(t, socksServerOptions{
		username: "front-user",
		password: "front-password",
		forward:  exit.address,
	})
	connector, err := New(config.FrontProxy{
		Enabled:  true,
		Protocol: "socks5",
		Address:  front.address,
		Username: "front-user",
		Password: "front-password",
	}, "")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := connector.Connect(ctx, Endpoint{
		Address:  "exit.proxy.example:1080",
		Username: "exit-user",
		Password: "exit-password",
	}, "service.example:443")
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	wantPayload := []byte("through-two-socks-hops")
	if err := writeFull(conn, wantPayload); err != nil {
		t.Fatalf("write tunnel payload: %v", err)
	}
	gotPayload := make([]byte, len(wantPayload))
	if _, err := io.ReadFull(conn, gotPayload); err != nil {
		t.Fatalf("read tunnel payload: %v", err)
	}
	if string(gotPayload) != string(wantPayload) {
		t.Fatalf("payload = %q, want %q", gotPayload, wantPayload)
	}
	assertTarget(t, front.targets, "exit.proxy.example:1080")
	assertTarget(t, exit.targets, "service.example:443")

	status := connector.FrontStatus()
	if status.Status != "healthy" || !status.Enabled {
		t.Fatalf("front status = %+v", status)
	}
	encoded, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "front-user") || strings.Contains(string(encoded), "front-password") {
		t.Fatalf("status leaked credentials: %s", encoded)
	}
}

func TestConnectorDisabledUsesSingleSOCKS5Hop(t *testing.T) {
	exit := startTestSOCKSServer(t, socksServerOptions{echo: true})
	connector, err := New(config.FrontProxy{}, "")
	if err != nil {
		t.Fatal(err)
	}
	if connector.FrontEnabled() {
		t.Fatal("front unexpectedly enabled")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := connector.Connect(ctx, Endpoint{Address: exit.address}, "unchanged.example:8443")
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()
	assertTarget(t, exit.targets, "unchanged.example:8443")

	if err := writeFull(conn, []byte("ok")); err != nil {
		t.Fatal(err)
	}
	response := make([]byte, 2)
	if _, err := io.ReadFull(conn, response); err != nil {
		t.Fatal(err)
	}
	if string(response) != "ok" {
		t.Fatalf("echo = %q", response)
	}
	if status := connector.FrontStatus(); status.Status != "disabled" || status.Enabled {
		t.Fatalf("disabled status = %+v", status)
	}
}

func TestFrontAuthenticationFailureIsSharedAndSanitized(t *testing.T) {
	front := startTestSOCKSServer(t, socksServerOptions{
		username: "expected-user",
		password: "expected-password",
		authFail: true,
	})
	connector, err := New(config.FrontProxy{
		Enabled:  true,
		Protocol: "socks5",
		Address:  front.address,
		Username: "secret-user",
		Password: "secret-password",
	}, "")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = connector.Connect(ctx, Endpoint{Address: "exit.example:1080"}, "target.example:80")
	assertPhase(t, err, PhaseFrontAuth)
	if !IsFrontFailure(err) {
		t.Fatalf("IsFrontFailure(%v) = false", err)
	}
	if scope := FailureScopeOf(err); scope != FailureScopeShared {
		t.Fatalf("front auth scope = %q, want shared", scope)
	}
	if connector.FrontRetryAfter() <= 0 {
		t.Fatal("front circuit did not expose a retry delay")
	}

	status := connector.FrontStatus()
	if status.Status != "unhealthy" || status.LastError != "front proxy authentication failed" {
		t.Fatalf("front status = %+v", status)
	}
	encoded, marshalErr := json.Marshal(status)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	for _, secret := range []string{"secret-user", "secret-password", "expected-user", "expected-password"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("status leaked %q: %s", secret, encoded)
		}
	}

	_, err = connector.Connect(ctx, Endpoint{Address: "another-exit.example:1080"}, "target.example:80")
	assertPhase(t, err, PhaseFrontCircuit)
}

func TestFrontCircuitAllowsOnlyOneHalfOpenProbe(t *testing.T) {
	unused := unusedTCPAddress(t)
	connector, err := New(config.FrontProxy{Enabled: true, Protocol: "socks5", Address: unused}, "")
	if err != nil {
		t.Fatal(err)
	}
	connector.circuitBackoff = 50 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = connector.Connect(ctx, Endpoint{Address: "exit.example:1080"}, "target.example:80")
	assertPhase(t, err, PhaseFrontDial)
	if !IsFrontFailure(err) {
		t.Fatalf("dial failure was not identified as shared: %v", err)
	}
	_, err = connector.Connect(ctx, Endpoint{Address: "exit.example:1080"}, "target.example:80")
	assertPhase(t, err, PhaseFrontCircuit)

	greetingSeen := make(chan struct{}, 1)
	releaseGreeting := make(chan struct{})
	front := startTestSOCKSServer(t, socksServerOptions{
		connectReply:    0x05,
		greetingSeen:    greetingSeen,
		releaseGreeting: releaseGreeting,
	})
	connector.mu.Lock()
	connector.front.Address = front.address
	connector.openUntil = time.Now().Add(-time.Millisecond)
	connector.status.CircuitOpenUntil = connector.openUntil
	connector.mu.Unlock()

	probeResult := make(chan error, 1)
	go func() {
		_, connectErr := connector.Connect(ctx, Endpoint{Address: "exit.example:1080"}, "target.example:80")
		probeResult <- connectErr
	}()
	select {
	case <-greetingSeen:
	case <-time.After(time.Second):
		t.Fatal("half-open probe did not reach the front")
	}
	if status := connector.FrontStatus(); status.Status != "half_open" {
		t.Fatalf("status during recovery probe = %+v", status)
	}
	if retryAfter := connector.FrontRetryAfter(); retryAfter != halfOpenRetryDelay {
		t.Fatalf("half-open retry delay = %s, want %s", retryAfter, halfOpenRetryDelay)
	}
	_, err = connector.Connect(ctx, Endpoint{Address: "exit.example:1080"}, "target.example:80")
	assertPhase(t, err, PhaseFrontCircuit)
	close(releaseGreeting)
	var ambiguousToken FrontToken
	select {
	case err := <-probeResult:
		assertPhase(t, err, PhaseFrontConnectExit)
		if !IsFrontFailure(err) {
			t.Fatalf("front CONNECT-to-exit refusal was not identified as shared: %v", err)
		}
		if !IsAmbiguousFrontFailure(err) || !IsAmbiguousFailure(err) {
			t.Fatalf("front CONNECT-to-exit refusal was not identified as ambiguous: %v", err)
		}
		if scope := FailureScopeOf(err); scope != FailureScopeAmbiguous {
			t.Fatalf("front CONNECT-to-exit scope = %q", scope)
		}
		var ok bool
		ambiguousToken, ok = AmbiguousFailureToken(err)
		if !ok {
			t.Fatalf("ambiguous error has no causal token: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("half-open probe did not finish")
	}
	if status := connector.FrontStatus(); status.Status != "unknown" || status.LastError != "front proxy could not connect to exit" {
		t.Fatalf("status after front CONNECT refusal = %+v", status)
	}
	if connector.FrontRetryAfter() != 0 {
		t.Fatal("one ambiguous failure must allow immediate exit rotation")
	}
	if !connector.RecordAmbiguousBatchFailure(ambiguousToken, ambiguousToken) {
		t.Fatal("current ambiguous batch token did not open circuit")
	}
	if connector.RecordAmbiguousBatchFailure(ambiguousToken, ambiguousToken) {
		t.Fatal("ambiguous batch token was consumed more than once")
	}
	if status := connector.FrontStatus(); status.Status != "unhealthy" || status.LastError != "front proxy could not connect to any candidate exit" {
		t.Fatalf("status after exhausted ambiguous batch = %+v", status)
	}
	if connector.FrontRetryAfter() <= 0 {
		t.Fatal("exhausted ambiguous batch did not open the circuit")
	}
}

func TestCanceledHalfOpenProbeRearmsCircuit(t *testing.T) {
	exit := startTestSOCKSServer(t, socksServerOptions{authFail: true})
	greetingSeen := make(chan struct{}, 1)
	releaseGreeting := make(chan struct{})
	front := startTestSOCKSServer(t, socksServerOptions{
		greetingSeen:    greetingSeen,
		releaseGreeting: releaseGreeting,
		forward:         exit.address,
	})
	connector, err := New(config.FrontProxy{Enabled: true, Protocol: "socks5", Address: front.address}, "")
	if err != nil {
		t.Fatal(err)
	}
	connector.circuitBackoff = 50 * time.Millisecond
	connector.mu.Lock()
	connector.generation = 1
	connector.openUntil = time.Now().Add(-time.Millisecond)
	connector.status.Status = "unhealthy"
	connector.status.LastError = "front proxy dial failed"
	connector.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, connectErr := connector.Connect(ctx, Endpoint{Address: "exit.example:1080"}, "target.example:80")
		result <- connectErr
	}()
	select {
	case <-greetingSeen:
	case <-time.After(time.Second):
		t.Fatal("half-open probe did not reach front")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
		if IsFrontFailure(err) {
			t.Fatalf("caller cancellation identified as shared failure: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled half-open probe did not finish")
	}
	close(releaseGreeting)
	status := connector.FrontStatus()
	if status.Status != "unhealthy" {
		t.Fatalf("status after canceled recovery probe = %+v", status)
	}
	if connector.FrontRetryAfter() <= 0 {
		t.Fatal("canceled recovery probe did not rearm circuit")
	}
	time.Sleep(70 * time.Millisecond)
	retryCtx, retryCancel := context.WithTimeout(context.Background(), time.Second)
	defer retryCancel()
	_, err = connector.Connect(retryCtx, Endpoint{
		Address:  "exit.example:1080",
		Username: "exit-user",
		Password: "exit-password",
	}, "target.example:80")
	assertPhase(t, err, PhaseExitAuth)
	if IsFrontFailure(err) {
		t.Fatalf("retry after canceled recovery probe remained a front failure: %v", err)
	}
	if status := connector.FrontStatus(); status.Status != "healthy" {
		t.Fatalf("status after successful retry handshake = %+v", status)
	}
}

func TestCanceledExitGreetingDoesNotRecordFrontStateAndClosesConnection(t *testing.T) {
	exitGreetingSeen := make(chan struct{}, 1)
	releaseExitGreeting := make(chan struct{})
	exitConnectionDone := make(chan struct{}, 1)
	exit := startTestSOCKSServer(t, socksServerOptions{
		greetingSeen:    exitGreetingSeen,
		releaseGreeting: releaseExitGreeting,
		connectionDone:  exitConnectionDone,
	})
	front := startTestSOCKSServer(t, socksServerOptions{forward: exit.address})
	connector, err := New(config.FrontProxy{Enabled: true, Protocol: "socks5", Address: front.address}, "")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, connectErr := connector.Connect(ctx, Endpoint{Address: "exit.example:1080"}, "target.example:443")
		result <- connectErr
	}()
	select {
	case <-exitGreetingSeen:
	case <-time.After(time.Second):
		t.Fatal("second-level SOCKS5 greeting was not sent")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled second-level greeting did not unblock")
	}
	if status := connector.FrontStatus(); status.Status != "unknown" || status.LastError != "" || !status.LastCheckedAt.IsZero() {
		t.Fatalf("cancellation changed front status: %+v", status)
	}
	if evidence, ok := connector.RecentFrontEvidence(); ok {
		t.Fatalf("cancellation recorded front evidence: %+v", evidence)
	}

	close(releaseExitGreeting)
	select {
	case <-exitConnectionDone:
	case <-time.After(time.Second):
		t.Fatal("canceled connector did not close the tunneled exit connection")
	}
}

func TestConnectSOCKS5ValidatesProtocolAndHonorsContext(t *testing.T) {
	t.Run("partial writes", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()
		targets := make(chan string, 1)
		go serveTestSOCKSConn(server, socksServerOptions{}, targets)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := ConnectSOCKS5(ctx, &shortWriteConn{Conn: client, max: 1}, "domain.example:443", "", ""); err != nil {
			t.Fatalf("ConnectSOCKS5: %v", err)
		}
		assertTarget(t, targets, "domain.example:443")
	})

	t.Run("invalid response version", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()
		go serveTestSOCKSConn(server, socksServerOptions{responseVersion: 0x04}, make(chan string, 1))
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := ConnectSOCKS5(ctx, client, "domain.example:443", "", "")
		if err == nil || !strings.Contains(err.Error(), "version") {
			t.Fatalf("error = %v, want version validation", err)
		}
	})

	t.Run("cancellation", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- ConnectSOCKS5(ctx, client, "domain.example:443", "", "")
		}()
		time.Sleep(10 * time.Millisecond)
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v, want context.Canceled", err)
			}
		case <-time.After(time.Second):
			t.Fatal("ConnectSOCKS5 did not unblock on cancellation")
		}
	})
}

func TestConnectorValidatesPortsBeforeDialing(t *testing.T) {
	connector, err := New(config.FrontProxy{}, "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = connector.Connect(context.Background(), Endpoint{Address: "127.0.0.1:1"}, "example.com:70000")
	assertPhase(t, err, PhaseExitConnect)
	if scope := FailureScopeOf(err); scope != FailureScopeExit {
		t.Fatalf("invalid target scope = %q, want exit", scope)
	}
	if IsFrontFailure(err) {
		t.Fatalf("invalid target identified as front failure: %v", err)
	}
	if FrontEstablished(err) {
		t.Fatalf("input validation claimed front establishment: %v", err)
	}
}

func TestAmbiguousCompletionCannotOverrideNewerCrossValidation(t *testing.T) {
	connector, err := New(config.FrontProxy{Enabled: true, Protocol: "socks5", Address: "127.0.0.1:1080"}, "")
	if err != nil {
		t.Fatal(err)
	}
	ambiguousAttempt, err := connector.beginFrontAttempt()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	successAttempt, err := connector.beginFrontAttempt()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := connector.recordFrontSuccess(successAttempt, "working-exit.example:1080"); !ok {
		t.Fatal("success evidence was not recorded")
	}
	if _, ok := connector.recordFrontAmbiguous(ambiguousAttempt); ok {
		t.Fatal("older ambiguous result was accepted after cross-validation")
	}
	if status := connector.FrontStatus(); status.Status != "healthy" || status.LastError != "" {
		t.Fatalf("stale ambiguity overrode cross-validation: %+v", status)
	}
}

func TestAmbiguousBatchTokenCannotOverrideNewerSuccess(t *testing.T) {
	connector, err := New(config.FrontProxy{Enabled: true, Protocol: "socks5", Address: "127.0.0.1:1080"}, "")
	if err != nil {
		t.Fatal(err)
	}
	ambiguousAttempt, err := connector.beginFrontAttempt()
	if err != nil {
		t.Fatal(err)
	}
	ambiguousToken, ok := connector.recordFrontAmbiguous(ambiguousAttempt)
	if !ok {
		t.Fatal("ambiguous token was not recorded")
	}
	successAttempt, err := connector.beginFrontAttempt()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := connector.recordFrontSuccess(successAttempt, "working-exit.example:1080"); !ok {
		t.Fatal("success evidence was not recorded")
	}
	if connector.RecordAmbiguousBatchFailure(ambiguousToken, ambiguousToken) {
		t.Fatal("stale ambiguous batch token overrode newer success")
	}
	evidence, ok := connector.RecentFrontEvidence()
	if !ok || evidence.ExitAddress != "working-exit.example:1080" || evidence.Sequence <= ambiguousToken.Sequence || evidence.At.IsZero() {
		t.Fatalf("recent front evidence = %+v, ok=%v", evidence, ok)
	}
	if status := connector.FrontStatus(); status.Status != "healthy" {
		t.Fatalf("status after rejected stale batch = %+v", status)
	}
}

func TestBatchEvidenceBetweenAmbiguitiesPreventsCircuit(t *testing.T) {
	connector, err := New(config.FrontProxy{Enabled: true, Protocol: "socks5", Address: "127.0.0.1:1080"}, "")
	if err != nil {
		t.Fatal(err)
	}
	firstAttempt, err := connector.beginFrontAttempt()
	if err != nil {
		t.Fatal(err)
	}
	firstToken, ok := connector.recordFrontAmbiguous(firstAttempt)
	if !ok {
		t.Fatal("first ambiguity was not recorded")
	}
	successAttempt, err := connector.beginFrontAttempt()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := connector.recordFrontSuccess(successAttempt, "cross-validation.example:1080"); !ok {
		t.Fatal("cross-validation evidence was not recorded")
	}
	lastAttempt, err := connector.beginFrontAttempt()
	if err != nil {
		t.Fatal(err)
	}
	lastToken, ok := connector.recordFrontAmbiguous(lastAttempt)
	if !ok {
		t.Fatal("last ambiguity was not recorded")
	}
	if connector.RecordAmbiguousBatchFailure(firstToken, lastToken) {
		t.Fatal("batch circuit opened despite intervening cross-validation evidence")
	}
	if connector.FrontRetryAfter() != 0 {
		t.Fatal("rejected batch unexpectedly opened circuit")
	}
	evidence, ok := connector.RecentFrontEvidence()
	if !ok || evidence.Sequence <= firstToken.Sequence || evidence.Sequence >= lastToken.Sequence {
		t.Fatalf("intervening evidence = %+v, ok=%v", evidence, ok)
	}
}

func TestFrontConnectSuccessThenExitGreetingEOFIsAmbiguous(t *testing.T) {
	front := startTestSOCKSServer(t, socksServerOptions{})
	connector, err := New(config.FrontProxy{Enabled: true, Protocol: "socks5", Address: front.address}, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = connector.Connect(ctx, Endpoint{Address: "unverified-exit.example:1080"}, "target.example:443")
	assertPhase(t, err, PhaseFrontConnectExit)
	assertTarget(t, front.targets, "unverified-exit.example:1080")
	if scope := FailureScopeOf(err); scope != FailureScopeAmbiguous {
		t.Fatalf("exit greeting EOF scope = %q, want ambiguous", scope)
	}
	if FrontEstablished(err) {
		t.Fatalf("exit greeting EOF claimed front establishment: %v", err)
	}
	if _, ok := AmbiguousFailureToken(err); !ok {
		t.Fatalf("exit greeting EOF lacks ambiguous evidence token: %v", err)
	}
	if evidence, ok := connector.RecentFrontEvidence(); ok {
		t.Fatalf("exit greeting EOF recorded success evidence: %+v", evidence)
	}
	if status := connector.FrontStatus(); status.Status != "unknown" {
		t.Fatalf("front status after exit greeting EOF = %+v", status)
	}
}

func TestExitGreetingTimeoutBeforeEvidenceIsAmbiguous(t *testing.T) {
	exitGreetingSeen := make(chan struct{}, 1)
	releaseExitGreeting := make(chan struct{})
	exit := startTestSOCKSServer(t, socksServerOptions{
		greetingSeen:    exitGreetingSeen,
		releaseGreeting: releaseExitGreeting,
	})
	front := startTestSOCKSServer(t, socksServerOptions{forward: exit.address})
	connector, err := New(config.FrontProxy{Enabled: true, Protocol: "socks5", Address: front.address}, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, connectErr := connector.Connect(ctx, Endpoint{Address: "slow-exit.example:1080"}, "target.example:443")
		result <- connectErr
	}()
	select {
	case <-exitGreetingSeen:
	case <-time.After(time.Second):
		t.Fatal("second-level SOCKS5 greeting was not sent")
	}
	select {
	case err = <-result:
	case <-time.After(time.Second):
		t.Fatal("second-level greeting timeout did not unblock")
	}
	close(releaseExitGreeting)
	assertPhase(t, err, PhaseFrontConnectExit)
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("error = %v, want a timeout", err)
	}
	if scope := FailureScopeOf(err); scope != FailureScopeAmbiguous {
		t.Fatalf("exit greeting timeout scope = %q, want ambiguous", scope)
	}
	if FrontEstablished(err) {
		t.Fatalf("exit greeting timeout claimed front establishment: %v", err)
	}
	if evidence, ok := connector.RecentFrontEvidence(); ok {
		t.Fatalf("exit greeting timeout recorded success evidence: %+v", evidence)
	}
}

func TestInvalidExitGreetingBeforeEvidenceIsAmbiguous(t *testing.T) {
	exit := startTestSOCKSServer(t, socksServerOptions{responseVersion: 0x04})
	front := startTestSOCKSServer(t, socksServerOptions{forward: exit.address})
	connector, err := New(config.FrontProxy{Enabled: true, Protocol: "socks5", Address: front.address}, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = connector.Connect(ctx, Endpoint{Address: "reachable-exit.example:1080"}, "target.example:443")
	assertPhase(t, err, PhaseFrontConnectExit)
	if FrontEstablished(err) || IsFrontEstablished(err) {
		t.Fatalf("invalid exit greeting claimed front establishment evidence: %v", err)
	}
	if scope := FailureScopeOf(err); scope != FailureScopeAmbiguous {
		t.Fatalf("invalid exit greeting scope = %q, want ambiguous", scope)
	}
	if _, ok := AmbiguousFailureToken(err); !ok {
		t.Fatalf("invalid exit greeting lacks ambiguous evidence token: %v", err)
	}
	if evidence, ok := connector.RecentFrontEvidence(); ok {
		t.Fatalf("invalid exit greeting recorded success evidence: %+v", evidence)
	}
	if status := connector.FrontStatus(); status.Status != "unknown" {
		t.Fatalf("front status after invalid exit greeting = %+v", status)
	}
}

func TestExitAuthFailureAfterValidGreetingCarriesEvidence(t *testing.T) {
	exit := startTestSOCKSServer(t, socksServerOptions{authFail: true})
	front := startTestSOCKSServer(t, socksServerOptions{forward: exit.address})
	connector, err := New(config.FrontProxy{Enabled: true, Protocol: "socks5", Address: front.address}, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = connector.Connect(ctx, Endpoint{
		Address:  "reachable-exit.example:1080",
		Username: "exit-user",
		Password: "exit-password",
	}, "target.example:443")
	assertPhase(t, err, PhaseExitAuth)
	if !FrontEstablished(err) || !IsFrontEstablished(err) {
		t.Fatalf("exit auth error lacks front establishment evidence: %v", err)
	}
	if scope := FailureScopeOf(err); scope != FailureScopeExit {
		t.Fatalf("exit auth scope = %q, want exit", scope)
	}
	var phaseErr *PhaseError
	if !errors.As(err, &phaseErr) || phaseErr.Token.Sequence == 0 {
		t.Fatalf("exit auth error lacks evidence token: %v", err)
	}
	evidence, ok := connector.RecentFrontEvidence()
	if !ok || evidence.ExitAddress != "reachable-exit.example:1080" || evidence.Generation != phaseErr.Token.Generation || evidence.Sequence != phaseErr.Token.Sequence || evidence.At.IsZero() {
		t.Fatalf("recent front evidence = %+v, token=%+v, ok=%v", evidence, phaseErr.Token, ok)
	}
	if status := connector.FrontStatus(); status.Status != "healthy" {
		t.Fatalf("front status after evidenced exit auth failure = %+v", status)
	}
}

func TestExitConnectFailureAfterValidGreetingCarriesEvidence(t *testing.T) {
	exit := startTestSOCKSServer(t, socksServerOptions{connectReply: 0x05})
	front := startTestSOCKSServer(t, socksServerOptions{forward: exit.address})
	connector, err := New(config.FrontProxy{Enabled: true, Protocol: "socks5", Address: front.address}, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = connector.Connect(ctx, Endpoint{Address: "reachable-exit.example:1080"}, "target.example:443")
	assertPhase(t, err, PhaseExitConnect)
	if !FrontEstablished(err) || FailureScopeOf(err) != FailureScopeExit {
		t.Fatalf("evidenced exit CONNECT error = %v, scope=%q", err, FailureScopeOf(err))
	}
	var phaseErr *PhaseError
	if !errors.As(err, &phaseErr) || phaseErr.Token.Sequence == 0 {
		t.Fatalf("exit CONNECT error lacks evidence token: %v", err)
	}
	evidence, ok := connector.RecentFrontEvidence()
	if !ok || evidence.ExitAddress != "reachable-exit.example:1080" || evidence.Generation != phaseErr.Token.Generation || evidence.Sequence != phaseErr.Token.Sequence {
		t.Fatalf("recent front evidence = %+v, token=%+v, ok=%v", evidence, phaseErr.Token, ok)
	}
}

func TestFrontStatusOmitsUnsetTimes(t *testing.T) {
	connector, err := New(config.FrontProxy{Enabled: true, Protocol: "socks5", Address: "127.0.0.1:1080"}, "")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(connector.FrontStatus())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "last_checked_at") || strings.Contains(string(encoded), "circuit_open_until") {
		t.Fatalf("zero times should be omitted: %s", encoded)
	}
}

type socksServerOptions struct {
	username        string
	password        string
	authFail        bool
	connectReply    byte
	responseVersion byte
	forward         string
	echo            bool
	greetingSeen    chan<- struct{}
	releaseGreeting <-chan struct{}
	connectionDone  chan<- struct{}
}

type testSOCKSServer struct {
	address string
	targets <-chan string
}

func startTestSOCKSServer(t *testing.T, options socksServerOptions) testSOCKSServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	targets := make(chan string, 16)
	var connections sync.Map
	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			connections.Store(conn, struct{}{})
			go func() {
				defer connections.Delete(conn)
				serveTestSOCKSConn(conn, options, targets)
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		connections.Range(func(key, _ any) bool {
			_ = key.(net.Conn).Close()
			return true
		})
	})
	return testSOCKSServer{address: listener.Addr().String(), targets: targets}
}

func serveTestSOCKSConn(conn net.Conn, options socksServerOptions, targets chan<- string) {
	defer conn.Close()
	defer func() {
		if options.connectionDone != nil {
			select {
			case options.connectionDone <- struct{}{}:
			default:
			}
		}
	}()
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil {
		return
	}
	methods := make([]byte, int(header[1]))
	if _, err := io.ReadFull(conn, methods); err != nil {
		return
	}
	if options.greetingSeen != nil {
		select {
		case options.greetingSeen <- struct{}{}:
		default:
		}
	}
	if options.releaseGreeting != nil {
		<-options.releaseGreeting
	}
	version := options.responseVersion
	if version == 0 {
		version = 0x05
	}
	needsAuth := options.username != "" || options.password != "" || options.authFail
	method := byte(0x00)
	if needsAuth {
		method = 0x02
	}
	if err := writeFull(conn, []byte{version, method}); err != nil || version != 0x05 {
		return
	}
	if needsAuth {
		username, password, err := readTestAuth(conn)
		if err != nil {
			return
		}
		failed := options.authFail || username != options.username || password != options.password
		status := byte(0x00)
		if failed {
			status = 0x01
		}
		if err := writeFull(conn, []byte{0x01, status}); err != nil || failed {
			return
		}
	}

	target, err := readTestConnectTarget(conn)
	if err != nil {
		return
	}
	targets <- target
	reply := options.connectReply
	var upstream net.Conn
	if reply == 0 && options.forward != "" {
		upstream, err = net.Dial("tcp", options.forward)
		if err != nil {
			reply = 0x05
		}
	}
	if err := writeFull(conn, []byte{0x05, reply, 0x00, 0x01, 0, 0, 0, 0, 0, 0}); err != nil || reply != 0 {
		if upstream != nil {
			_ = upstream.Close()
		}
		return
	}
	if upstream != nil {
		defer upstream.Close()
		go func() {
			_, _ = io.Copy(upstream, conn)
			_ = upstream.Close()
		}()
		_, _ = io.Copy(conn, upstream)
		return
	}
	if options.echo {
		_, _ = io.Copy(conn, conn)
	}
}

func readTestAuth(conn net.Conn) (string, string, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil {
		return "", "", err
	}
	if header[0] != 0x01 {
		return "", "", fmt.Errorf("auth version %d", header[0])
	}
	username := make([]byte, int(header[1]))
	if _, err := io.ReadFull(conn, username); err != nil {
		return "", "", err
	}
	length := []byte{0}
	if _, err := io.ReadFull(conn, length); err != nil {
		return "", "", err
	}
	password := make([]byte, int(length[0]))
	if _, err := io.ReadFull(conn, password); err != nil {
		return "", "", err
	}
	return string(username), string(password), nil
}

func readTestConnectTarget(conn net.Conn) (string, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return "", err
	}
	if header[0] != 0x05 || header[1] != 0x01 || header[2] != 0x00 {
		return "", fmt.Errorf("invalid CONNECT header %v", header)
	}
	var host string
	switch header[3] {
	case 0x01:
		value := make([]byte, net.IPv4len)
		if _, err := io.ReadFull(conn, value); err != nil {
			return "", err
		}
		host = net.IP(value).String()
	case 0x03:
		length := []byte{0}
		if _, err := io.ReadFull(conn, length); err != nil {
			return "", err
		}
		value := make([]byte, int(length[0]))
		if _, err := io.ReadFull(conn, value); err != nil {
			return "", err
		}
		host = string(value)
	case 0x04:
		value := make([]byte, net.IPv6len)
		if _, err := io.ReadFull(conn, value); err != nil {
			return "", err
		}
		host = net.IP(value).String()
	default:
		return "", fmt.Errorf("address type %d", header[3])
	}
	portBytes := make([]byte, 2)
	if _, err := io.ReadFull(conn, portBytes); err != nil {
		return "", err
	}
	port := int(portBytes[0])<<8 | int(portBytes[1])
	return net.JoinHostPort(host, strconv.Itoa(port)), nil
}

func assertTarget(t *testing.T, targets <-chan string, expected string) {
	t.Helper()
	select {
	case actual := <-targets:
		if actual != expected {
			t.Fatalf("SOCKS5 target = %q, want %q", actual, expected)
		}
	case <-time.After(time.Second):
		t.Fatalf("SOCKS5 server did not receive target %q", expected)
	}
}

func assertPhase(t *testing.T, err error, expected string) {
	t.Helper()
	var phaseErr *PhaseError
	if !errors.As(err, &phaseErr) {
		t.Fatalf("error = %v, want PhaseError %q", err, expected)
	}
	if phaseErr.Phase != expected {
		t.Fatalf("phase = %q, want %q (error: %v)", phaseErr.Phase, expected, err)
	}
}

func unusedTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

type shortWriteConn struct {
	net.Conn
	max int
}

func (c *shortWriteConn) Write(data []byte) (int, error) {
	if len(data) > c.max {
		data = data[:c.max]
	}
	return c.Conn.Write(data)
}
