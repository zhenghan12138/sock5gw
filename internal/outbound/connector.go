package outbound

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"sock5gw/internal/config"
)

const (
	defaultCircuitBackoff = 5 * time.Second
	halfOpenRetryDelay    = 100 * time.Millisecond
)

const (
	PhaseFrontDial        = "front_dial"
	PhaseFrontCircuit     = "front_circuit"
	PhaseFrontHandshake   = "front_handshake"
	PhaseFrontAuth        = "front_auth"
	PhaseFrontConnectExit = "front_connect_exit"
	PhaseExitDial         = "exit_dial"
	PhaseExitHandshake    = "exit_handshake"
	PhaseExitAuth         = "exit_auth"
	PhaseExitConnect      = "exit_connect_target"
)

// FailureScope expresses which ownership boundary should react to a failure.
// Ambiguous failures are treated as front failures for pool protection, while
// allowing the manager to rotate exits and seek cross-validation.
type FailureScope string

const (
	FailureScopeUnknown   FailureScope = ""
	FailureScopeShared    FailureScope = "shared"
	FailureScopeAmbiguous FailureScope = "ambiguous"
	FailureScopeExit      FailureScope = "exit"
)

// FrontToken identifies one ordered front-path resolution within a circuit
// generation. It lets callers reject stale batch decisions without relying on
// timing windows.
type FrontToken struct {
	Generation uint64
	Sequence   uint64
}

// FrontEvidence records that a valid SOCKS5 method selection response was
// received from an exit through the front. It contains no credentials.
type FrontEvidence struct {
	ExitAddress string
	Generation  uint64
	Sequence    uint64
	At          time.Time
}

// Endpoint describes a SOCKS5 exit. Credentials are never included in status
// output or generated error messages.
type Endpoint struct {
	Address  string
	Username string
	Password string
}

// FrontStatus is a credential-free snapshot of the shared front proxy.
type FrontStatus struct {
	Enabled          bool      `json:"enabled"`
	Protocol         string    `json:"protocol,omitempty"`
	Address          string    `json:"address,omitempty"`
	Status           string    `json:"status"`
	LastError        string    `json:"last_error,omitempty"`
	LastCheckedAt    time.Time `json:"last_checked_at,omitempty,omitzero"`
	CircuitOpenUntil time.Time `json:"circuit_open_until,omitempty,omitzero"`
}

// PhaseError identifies which hop failed without losing the underlying error.
type PhaseError struct {
	Phase            string
	Scope            FailureScope
	FrontEstablished bool
	Token            FrontToken
	Err              error
}

func (e *PhaseError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Err == nil {
		return e.Phase
	}
	return e.Phase + ": " + e.Err.Error()
}

func (e *PhaseError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func newPhaseError(phase string, err error) *PhaseError {
	return &PhaseError{Phase: phase, Scope: scopeForPhase(phase), Err: err}
}

func newEstablishedPhaseError(phase string, token FrontToken, err error) *PhaseError {
	return &PhaseError{
		Phase:            phase,
		Scope:            scopeForPhase(phase),
		FrontEstablished: true,
		Token:            token,
		Err:              err,
	}
}

func scopeForPhase(phase string) FailureScope {
	switch phase {
	case PhaseFrontDial, PhaseFrontCircuit, PhaseFrontHandshake, PhaseFrontAuth:
		return FailureScopeShared
	case PhaseFrontConnectExit:
		return FailureScopeAmbiguous
	case PhaseExitDial, PhaseExitHandshake, PhaseExitAuth, PhaseExitConnect:
		return FailureScopeExit
	default:
		return FailureScopeUnknown
	}
}

// Connector establishes a tunnel through an optional shared SOCKS5 front and
// an individual SOCKS5 exit.
type Connector struct {
	front       config.FrontProxy
	dnsUpstream string

	mu               sync.Mutex
	status           FrontStatus
	openUntil        time.Time
	halfOpenInFlight bool
	generation       uint64
	resolutionSeq    uint64
	lastResolution   frontResolution
	recentEvidence   FrontEvidence
	circuitBackoff   time.Duration
}

type frontResolution uint8

const (
	frontResolutionNone frontResolution = iota
	frontResolutionShared
	frontResolutionAmbiguous
	frontResolutionEstablished
)

type frontAttempt struct {
	generation      uint64
	halfOpen        bool
	startedSequence uint64
}

// New constructs a connector. Timeouts are supplied by each Connect context.
func New(front config.FrontProxy, dnsUpstream string) (*Connector, error) {
	front.Protocol = strings.ToLower(strings.TrimSpace(front.Protocol))
	front.Address = strings.TrimSpace(front.Address)
	dnsUpstream = strings.TrimSpace(dnsUpstream)

	if front.FailOpen {
		return nil, errors.New("front proxy fail-open mode is not supported")
	}
	if front.Enabled {
		if front.Protocol == "" {
			front.Protocol = "socks5"
		}
		if front.Protocol != "socks5" {
			return nil, fmt.Errorf("unsupported front proxy protocol %q", front.Protocol)
		}
		if err := validateAddress(front.Address); err != nil {
			return nil, fmt.Errorf("front proxy address: %w", err)
		}
		if err := validateCredentials(front.Username, front.Password); err != nil {
			return nil, fmt.Errorf("front proxy credentials: %w", err)
		}
	}
	if dnsUpstream != "" {
		var err error
		dnsUpstream, err = normalizeDNSUpstream(dnsUpstream)
		if err != nil {
			return nil, err
		}
	}

	status := FrontStatus{Enabled: front.Enabled, Status: "disabled"}
	if front.Enabled {
		status.Protocol = front.Protocol
		status.Address = front.Address
		status.Status = "unknown"
	}
	return &Connector{
		front:          front,
		dnsUpstream:    dnsUpstream,
		status:         status,
		circuitBackoff: defaultCircuitBackoff,
	}, nil
}

func (c *Connector) FrontEnabled() bool {
	return c != nil && c.front.Enabled
}

func (c *Connector) FrontStatus() FrontStatus {
	if c == nil {
		return FrontStatus{Status: "disabled"}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	status := c.status
	if c.front.Enabled && c.halfOpenInFlight {
		status.Status = "half_open"
	}
	return status
}

func (c *Connector) FrontRetryAfter() time.Duration {
	if c == nil || !c.front.Enabled {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.halfOpenInFlight {
		return halfOpenRetryDelay
	}
	retryAfter := time.Until(c.openUntil)
	if retryAfter < 0 {
		return 0
	}
	return retryAfter
}

// IsFrontFailure reports failures attributed to the shared front path. The
// front's CONNECT-to-exit phase is treated conservatively as shared so a dead
// front transport cannot mark every exit unhealthy.
func IsFrontFailure(err error) bool {
	switch FailureScopeOf(err) {
	case FailureScopeShared, FailureScopeAmbiguous:
		return true
	default:
		return false
	}
}

// FailureScopeOf returns the explicit failure scope. It also infers scope from
// Phase for PhaseError values created by older callers.
func FailureScopeOf(err error) FailureScope {
	var phaseErr *PhaseError
	if !errors.As(err, &phaseErr) {
		return FailureScopeUnknown
	}
	if phaseErr.Scope != FailureScopeUnknown {
		return phaseErr.Scope
	}
	return scopeForPhase(phaseErr.Phase)
}

// IsAmbiguousFrontFailure reports a front-to-exit failure that requires pool
// rotation before it can be attributed to either the shared front or one exit.
func IsAmbiguousFrontFailure(err error) bool {
	return FailureScopeOf(err) == FailureScopeAmbiguous
}

// IsAmbiguousFailure is a concise alias used by manager policy code.
func IsAmbiguousFailure(err error) bool {
	return IsAmbiguousFrontFailure(err)
}

// AmbiguousFailureToken returns the causal token attached to an unresolved
// front-to-exit failure. A false result means newer evidence already resolved
// that attempt or the error is not ambiguous.
func AmbiguousFailureToken(err error) (FrontToken, bool) {
	var phaseErr *PhaseError
	if !errors.As(err, &phaseErr) || FailureScopeOf(err) != FailureScopeAmbiguous || phaseErr.Token.Sequence == 0 {
		return FrontToken{}, false
	}
	return phaseErr.Token, true
}

// FrontEstablished reports whether this operation received a valid SOCKS5
// method selection response from the exit before failing later in that hop.
func FrontEstablished(err error) bool {
	var phaseErr *PhaseError
	return errors.As(err, &phaseErr) && phaseErr.FrontEstablished
}

func IsFrontEstablished(err error) bool {
	return FrontEstablished(err)
}

// RecentFrontEvidence returns cross-validation evidence from the current
// circuit generation. Evidence from a prior shared failure is not returned.
func (c *Connector) RecentFrontEvidence() (FrontEvidence, bool) {
	if c == nil || !c.front.Enabled {
		return FrontEvidence{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.recentEvidence.Sequence == 0 || c.recentEvidence.Generation != c.generation {
		return FrontEvidence{}, false
	}
	return c.recentEvidence, true
}

// Connect returns a TCP connection after completing every required SOCKS5
// handshake. The caller owns the returned connection.
func (c *Connector) Connect(ctx context.Context, exit Endpoint, target string) (net.Conn, error) {
	if c == nil {
		return nil, errors.New("nil outbound connector")
	}
	if ctx == nil {
		return nil, errors.New("nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateAddress(exit.Address); err != nil {
		return nil, newPhaseError(PhaseExitDial, err)
	}
	if err := validateCredentials(exit.Username, exit.Password); err != nil {
		return nil, newPhaseError(PhaseExitAuth, err)
	}
	if err := validateAddress(target); err != nil {
		return nil, newPhaseError(PhaseExitConnect, err)
	}

	if !c.front.Enabled {
		return c.connectDirect(ctx, exit, target)
	}
	return c.connectThroughFront(ctx, exit, target)
}

func (c *Connector) connectDirect(ctx context.Context, exit Endpoint, target string) (net.Conn, error) {
	conn, err := c.dialAddress(ctx, exit.Address)
	if err != nil {
		if ctxErr := ctx.Err(); errors.Is(ctxErr, context.Canceled) {
			return nil, ctxErr
		}
		return nil, newPhaseError(PhaseExitDial, contextCause(ctx, err))
	}
	cleanup, err := bindConnContext(ctx, conn)
	if err != nil {
		_ = conn.Close()
		if ctxErr := ctx.Err(); errors.Is(ctxErr, context.Canceled) {
			return nil, ctxErr
		}
		return nil, newPhaseError(PhaseExitHandshake, contextCause(ctx, err))
	}
	succeeded := false
	defer func() {
		cleanup()
		if !succeeded {
			_ = conn.Close()
		}
	}()

	client := socks5Client{conn: conn}
	if err := client.handshake(exit.Username, exit.Password); err != nil {
		if ctxErr := ctx.Err(); errors.Is(ctxErr, context.Canceled) {
			return nil, ctxErr
		}
		return nil, newPhaseError(exitHandshakePhase(err), contextCause(ctx, err))
	}
	if err := client.connect(target); err != nil {
		if ctxErr := ctx.Err(); errors.Is(ctxErr, context.Canceled) {
			return nil, ctxErr
		}
		return nil, newPhaseError(PhaseExitConnect, contextCause(ctx, err))
	}
	succeeded = true
	return conn, nil
}

func (c *Connector) connectThroughFront(ctx context.Context, exit Endpoint, target string) (net.Conn, error) {
	attempt, err := c.beginFrontAttempt()
	if err != nil {
		return nil, err
	}
	attemptResolved := false
	defer func() {
		if !attemptResolved {
			c.abandonFrontAttempt(attempt)
		}
	}()
	conn, err := c.dialAddress(ctx, c.front.Address)
	if err != nil {
		if ctxErr := ctx.Err(); errors.Is(ctxErr, context.Canceled) {
			return nil, ctxErr
		}
		phaseErr := newPhaseError(PhaseFrontDial, contextCause(ctx, err))
		c.recordFrontFailure(attempt, phaseErr.Phase)
		attemptResolved = true
		return nil, phaseErr
	}
	cleanup, err := bindConnContext(ctx, conn)
	if err != nil {
		_ = conn.Close()
		if ctxErr := ctx.Err(); errors.Is(ctxErr, context.Canceled) {
			return nil, ctxErr
		}
		phaseErr := newPhaseError(PhaseFrontHandshake, contextCause(ctx, err))
		c.recordFrontFailure(attempt, phaseErr.Phase)
		attemptResolved = true
		return nil, phaseErr
	}
	succeeded := false
	defer func() {
		cleanup()
		if !succeeded {
			_ = conn.Close()
		}
	}()

	frontClient := socks5Client{conn: conn}
	if err := frontClient.handshake(c.front.Username, c.front.Password); err != nil {
		if ctxErr := ctx.Err(); errors.Is(ctxErr, context.Canceled) {
			return nil, ctxErr
		}
		phase := frontHandshakePhase(err)
		phaseErr := newPhaseError(phase, contextCause(ctx, err))
		c.recordFrontFailure(attempt, phase)
		attemptResolved = true
		return nil, phaseErr
	}
	if err := frontClient.connect(exit.Address); err != nil {
		if ctxErr := ctx.Err(); errors.Is(ctxErr, context.Canceled) {
			return nil, ctxErr
		}
		phaseErr := newPhaseError(PhaseFrontConnectExit, contextCause(ctx, err))
		if token, current := c.recordFrontAmbiguous(attempt); current {
			phaseErr.Token = token
		}
		attemptResolved = true
		return nil, phaseErr
	}
	frontToken := FrontToken{}
	frontEstablished := false
	exitClient := socks5Client{conn: conn}
	if err := exitClient.handshakeWithMethodSelection(exit.Username, exit.Password, func() {
		frontEstablished = true
		frontToken, _ = c.recordFrontSuccess(attempt, exit.Address)
		attemptResolved = true
	}); err != nil {
		if ctxErr := ctx.Err(); errors.Is(ctxErr, context.Canceled) {
			return nil, ctxErr
		}
		if !frontEstablished {
			phaseErr := newPhaseError(PhaseFrontConnectExit, contextCause(ctx, err))
			if token, current := c.recordFrontAmbiguous(attempt); current {
				phaseErr.Token = token
			}
			attemptResolved = true
			return nil, phaseErr
		}
		return nil, newEstablishedPhaseError(exitHandshakePhase(err), frontToken, contextCause(ctx, err))
	}
	if err := exitClient.connect(target); err != nil {
		if ctxErr := ctx.Err(); errors.Is(ctxErr, context.Canceled) {
			return nil, ctxErr
		}
		return nil, newEstablishedPhaseError(PhaseExitConnect, frontToken, contextCause(ctx, err))
	}
	succeeded = true
	return conn, nil
}

func (c *Connector) beginFrontAttempt() (frontAttempt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if now.Before(c.openUntil) {
		return frontAttempt{}, newPhaseError(PhaseFrontCircuit, errors.New("front proxy circuit is open"))
	}
	if !c.openUntil.IsZero() {
		if c.halfOpenInFlight {
			return frontAttempt{}, newPhaseError(PhaseFrontCircuit, errors.New("front proxy recovery probe is already running"))
		}
		c.halfOpenInFlight = true
		c.status.Status = "half_open"
		return frontAttempt{generation: c.generation, halfOpen: true, startedSequence: c.resolutionSeq}, nil
	}
	return frontAttempt{generation: c.generation, startedSequence: c.resolutionSeq}, nil
}

func (c *Connector) recordFrontFailure(attempt frontAttempt, phase string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Ignore completion from an attempt that predates a newer failure.
	if attempt.generation != c.generation {
		return
	}
	now := time.Now()
	c.generation++
	c.resolutionSeq++
	c.lastResolution = frontResolutionShared
	c.openUntil = now.Add(c.circuitBackoff)
	c.halfOpenInFlight = false
	c.status.Status = "unhealthy"
	c.status.LastCheckedAt = now
	c.status.CircuitOpenUntil = c.openUntil
	c.status.LastError = publicFrontError(phase)
}

func (c *Connector) recordFrontSuccess(attempt frontAttempt, exitAddress string) (FrontToken, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// A stale success must not close a circuit opened by another attempt.
	if attempt.generation != c.generation {
		return FrontToken{}, false
	}
	now := time.Now()
	c.resolutionSeq++
	token := FrontToken{Generation: c.generation, Sequence: c.resolutionSeq}
	c.lastResolution = frontResolutionEstablished
	c.recentEvidence = FrontEvidence{
		ExitAddress: exitAddress,
		Generation:  token.Generation,
		Sequence:    token.Sequence,
		At:          now,
	}
	c.openUntil = time.Time{}
	c.halfOpenInFlight = false
	c.status.Status = "healthy"
	c.status.LastCheckedAt = now
	c.status.CircuitOpenUntil = time.Time{}
	c.status.LastError = ""
	return token, true
}

func (c *Connector) recordFrontAmbiguous(attempt frontAttempt) (FrontToken, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if attempt.generation != c.generation {
		return FrontToken{}, false
	}
	if c.recentEvidence.Generation == attempt.generation && c.recentEvidence.Sequence > attempt.startedSequence {
		return FrontToken{}, false
	}
	now := time.Now()
	c.resolutionSeq++
	token := FrontToken{Generation: c.generation, Sequence: c.resolutionSeq}
	c.lastResolution = frontResolutionAmbiguous
	c.openUntil = time.Time{}
	c.halfOpenInFlight = false
	c.status.Status = "unknown"
	c.status.LastCheckedAt = now
	c.status.CircuitOpenUntil = time.Time{}
	c.status.LastError = publicFrontError(PhaseFrontConnectExit)
	return token, true
}

// RecordAmbiguousBatchFailure opens the shared circuit after the manager has
// exhausted distinct exits without obtaining a cross-validating success. It
// is a no-op if another concurrent attempt has already resolved the ambiguity.
func (c *Connector) RecordAmbiguousBatchFailure(first, last FrontToken) bool {
	if c == nil || !c.front.Enabled {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if first.Sequence == 0 || last.Sequence == 0 || first.Generation != last.Generation || first.Generation != c.generation || first.Sequence > last.Sequence || last.Sequence != c.resolutionSeq || c.lastResolution != frontResolutionAmbiguous {
		return false
	}
	if c.recentEvidence.Generation == c.generation && c.recentEvidence.Sequence > first.Sequence {
		return false
	}
	now := time.Now()
	c.generation++
	c.resolutionSeq++
	c.lastResolution = frontResolutionShared
	c.openUntil = now.Add(c.circuitBackoff)
	c.halfOpenInFlight = false
	c.status.Status = "unhealthy"
	c.status.LastCheckedAt = now
	c.status.CircuitOpenUntil = c.openUntil
	c.status.LastError = "front proxy could not connect to any candidate exit"
	return true
}

func (c *Connector) abandonFrontAttempt(attempt frontAttempt) {
	if !attempt.halfOpen {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if attempt.generation != c.generation || !c.halfOpenInFlight {
		return
	}
	c.halfOpenInFlight = false
	c.openUntil = time.Now().Add(c.circuitBackoff)
	c.status.Status = "unhealthy"
	c.status.CircuitOpenUntil = c.openUntil
}

func publicFrontError(phase string) string {
	switch phase {
	case PhaseFrontDial:
		return "front proxy dial failed"
	case PhaseFrontAuth:
		return "front proxy authentication failed"
	case PhaseFrontHandshake:
		return "front proxy handshake failed"
	case PhaseFrontConnectExit:
		return "front proxy could not connect to exit"
	default:
		return "front proxy failed"
	}
}

func frontHandshakePhase(err error) string {
	if isSOCKSAuthError(err) {
		return PhaseFrontAuth
	}
	return PhaseFrontHandshake
}

func exitHandshakePhase(err error) string {
	if isSOCKSAuthError(err) {
		return PhaseExitAuth
	}
	return PhaseExitHandshake
}

func (c *Connector) dialAddress(ctx context.Context, address string) (net.Conn, error) {
	host, port, err := splitAddress(address)
	if err != nil {
		return nil, err
	}
	dialer := net.Dialer{}
	if net.ParseIP(host) != nil || c.dnsUpstream == "" {
		return dialer.DialContext(ctx, "tcp", address)
	}

	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(resolveCtx context.Context, network, _ string) (net.Conn, error) {
			return dialer.DialContext(resolveCtx, network, c.dnsUpstream)
		},
	}
	addresses, err := resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", host, err)
	}
	var lastErr error
	for _, resolved := range addresses {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ip4 := resolved.IP.To4()
		if ip4 == nil {
			continue
		}
		conn, dialErr := dialer.DialContext(ctx, "tcp", net.JoinHostPort(ip4.String(), port))
		if dialErr == nil {
			return conn, nil
		}
		lastErr = dialErr
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("no usable addresses for %s", host)
}

func normalizeDNSUpstream(address string) (string, error) {
	if _, _, err := net.SplitHostPort(address); err == nil {
		if err := validateAddress(address); err != nil {
			return "", fmt.Errorf("dns upstream: %w", err)
		}
		return address, nil
	}
	if net.ParseIP(address) != nil {
		address = net.JoinHostPort(address, "53")
		return address, nil
	}
	if strings.Contains(address, ":") {
		return "", fmt.Errorf("dns upstream: invalid address %q", address)
	}
	address = net.JoinHostPort(address, "53")
	if err := validateAddress(address); err != nil {
		return "", fmt.Errorf("dns upstream: %w", err)
	}
	return address, nil
}

func validateAddress(address string) error {
	_, _, err := splitAddress(address)
	return err
}

func splitAddress(address string) (string, string, error) {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return "", "", err
	}
	if host == "" {
		return "", "", errors.New("host is required")
	}
	if strings.ContainsAny(host, "\x00 \t\r\n") {
		return "", "", errors.New("host contains invalid characters")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", "", errors.New("port must be an integer between 1 and 65535")
	}
	return host, portText, nil
}

func validateCredentials(username, password string) error {
	if len(username) > 255 || len(password) > 255 {
		return errors.New("SOCKS5 credentials must not exceed 255 bytes")
	}
	return nil
}

func contextCause(ctx context.Context, fallback error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return fallback
}
