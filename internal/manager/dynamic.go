package manager

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"sock5gw/internal/config"
	"sock5gw/internal/providerapi"
	"sock5gw/internal/store"
)

type proxyAPIAcquirer interface {
	Acquire(context.Context, string, int64) (providerapi.Endpoint, error)
}

type dynamicGate struct {
	token chan struct{}
	refs  int
}

type DynamicLeaseRequest struct {
	Country         string `json:"country"`
	DurationMinutes int64  `json:"duration_minutes"`
}

type LeaseAPIError struct {
	Code         string      `json:"code"`
	Message      string      `json:"message"`
	CurrentLease *Assignment `json:"current_lease,omitempty"`
	Err          error       `json:"-"`
}

func (e *LeaseAPIError) Error() string {
	if e == nil {
		return "dynamic lease failed"
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return e.Message
}

func (e *LeaseAPIError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type ProxyAPITestResult struct {
	OK        bool   `json:"ok"`
	Code      string `json:"code"`
	Address   string `json:"address,omitempty"`
	ExitIP    string `json:"exit_ip,omitempty"`
	ElapsedMS int64  `json:"elapsed_ms"`
}

func NormalizeDynamicLeaseRequest(request DynamicLeaseRequest) (DynamicLeaseRequest, error) {
	country := strings.TrimSpace(request.Country)
	if strings.EqualFold(country, "rand") {
		country = "Rand"
	} else {
		if len(country) != 2 || !isASCIILetter(country[0]) || !isASCIILetter(country[1]) {
			return DynamicLeaseRequest{}, errors.New("country must be a two-letter code or Rand")
		}
		country = strings.ToUpper(country)
	}
	if request.DurationMinutes <= 0 {
		return DynamicLeaseRequest{}, errors.New("duration_minutes must be a positive integer")
	}
	if request.DurationMinutes > math.MaxInt64/int64(time.Minute) {
		return DynamicLeaseRequest{}, errors.New("duration_minutes is too large")
	}
	return DynamicLeaseRequest{Country: country, DurationMinutes: request.DurationMinutes}, nil
}

func isASCIILetter(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z'
}

func (m *Manager) LeaseDynamicContext(ctx context.Context, clientIP string, request DynamicLeaseRequest, refresh bool) (Assignment, error) {
	normalized, err := NormalizeDynamicLeaseRequest(request)
	if err != nil {
		return Assignment{}, &LeaseAPIError{Code: "invalid_request", Message: err.Error(), Err: err}
	}
	unlock, err := m.lockDynamicGate(ctx, clientIP)
	if err != nil {
		return Assignment{}, m.dynamicLeaseError(clientIP, "proxy_api_request_failed", "dynamic lease request was canceled", err)
	}
	defer unlock()

	m.mu.Lock()
	current := m.currentAssignmentLocked(clientIP)
	if !refresh && current.Status == LeaseActive && current.Mode == ProxySourceAPI &&
		current.Country == normalized.Country && current.DurationMinutes == normalized.DurationMinutes {
		m.mu.Unlock()
		return current, nil
	}
	m.mu.Unlock()

	proxyAPIConfig, client := m.proxyAPISnapshot()
	if !proxyAPIConfig.Enabled || client == nil {
		return Assignment{}, m.dynamicLeaseError(clientIP, "proxy_api_disabled", "proxy API mode is disabled", nil)
	}

	requestedAt := time.Now()
	endpoint, err := client.Acquire(ctx, normalized.Country, normalized.DurationMinutes)
	if err != nil {
		code := "proxy_api_request_failed"
		if providerapi.ErrorKindOf(err) == providerapi.ErrorResponse {
			code = "proxy_api_invalid_response"
		}
		return Assignment{}, m.dynamicLeaseError(clientIP, code, err.Error(), err)
	}
	if m.frontAddressConflict(endpoint.Address) {
		err := errors.New("proxy API returned the configured front proxy address")
		return Assignment{}, m.dynamicLeaseError(clientIP, "proxy_api_invalid_response", err.Error(), err)
	}

	expiresAt := requestedAt.Add(time.Duration(normalized.DurationMinutes) * time.Minute)
	proxyID, err := newDynamicProxyID()
	if err != nil {
		return Assignment{}, m.dynamicLeaseError(clientIP, "internal_error", "generate dynamic proxy ID failed", err)
	}
	candidate := Proxy{
		ID:                proxyID,
		Address:           endpoint.Address,
		Username:          endpoint.Username,
		Password:          endpoint.Password,
		Status:            ProxyChecking,
		ClientIP:          clientIP,
		Source:            ProxySourceAPI,
		Country:           normalized.Country,
		DurationMinutes:   normalized.DurationMinutes,
		ProviderExpiresAt: expiresAt,
	}
	m.mu.Lock()
	duplicate := m.proxyAddressExistsLocked(candidate.Address)
	m.mu.Unlock()
	if duplicate {
		err := errors.New("proxy API returned an endpoint already present in the proxy pool")
		return Assignment{}, m.dynamicLeaseError(clientIP, "proxy_api_duplicate_endpoint", err.Error(), err)
	}
	exitIP, err := m.probeProxy(ctx, candidate)
	if err != nil {
		return Assignment{}, m.dynamicLeaseError(clientIP, "dynamic_proxy_unusable", "the acquired SOCKS5 proxy failed validation", err)
	}
	candidate.ExitIP = exitIP
	return m.activateDynamicProxy(ctx, clientIP, normalized, candidate, refresh)
}

func (m *Manager) activateDynamicProxy(ctx context.Context, clientIP string, request DynamicLeaseRequest, candidate Proxy, refresh bool) (Assignment, error) {
	now := time.Now()
	m.mu.Lock()
	if m.proxyAddressExistsLocked(candidate.Address) {
		m.mu.Unlock()
		err := errors.New("proxy API endpoint became a duplicate before activation")
		return Assignment{}, m.dynamicLeaseError(clientIP, "proxy_api_duplicate_endpoint", err.Error(), err)
	}
	oldLease := m.leases[clientIP]
	oldProxy := (*Proxy)(nil)
	deleteOldID := ""
	if oldLease != nil {
		oldProxy = m.proxies[oldLease.ProxyID]
		if oldProxy != nil && oldProxy.Source == ProxySourceAPI && oldProxy.ActiveConns == 0 {
			deleteOldID = oldProxy.ID
		}
	}
	lease := store.Lease{
		ClientIP:  clientIP,
		ProxyID:   candidate.ID,
		Status:    LeaseActive,
		ExpiresAt: candidate.ProviderExpiresAt,
		UpdatedAt: now,
	}
	if err := m.db.ActivateDynamicLease(ctx, store.Proxy{
		ID:       candidate.ID,
		Address:  candidate.Address,
		Username: candidate.Username,
		Password: candidate.Password,
	}, store.DynamicProxy{
		ProxyID:           candidate.ID,
		Country:           request.Country,
		DurationMinutes:   request.DurationMinutes,
		ProviderExpiresAt: candidate.ProviderExpiresAt,
	}, lease, deleteOldID); err != nil {
		m.mu.Unlock()
		return Assignment{}, m.dynamicLeaseError(clientIP, "internal_error", "persist dynamic lease failed", err)
	}

	wakeQueue := false
	if oldProxy != nil {
		if oldProxy.ActiveConns > 0 {
			oldProxy.Status = ProxyDraining
			oldProxy.ClientIP = ""
			oldProxy.DrainingFor = clientIP
		} else if oldProxy.Source == ProxySourceAPI {
			delete(m.proxies, oldProxy.ID)
			m.proxyOrder = removeIP(m.proxyOrder, oldProxy.ID)
		} else {
			m.markProxyIdleLocked(oldProxy)
			wakeQueue = true
		}
	}
	candidate.Status = ProxyActive
	candidate.ClientIP = clientIP
	candidate.SuccessCount++
	m.proxies[candidate.ID] = &candidate
	m.proxyOrder = append(m.proxyOrder, candidate.ID)
	sort.Strings(m.proxyOrder)
	m.removeQueuedLocked(clientIP)
	m.leases[clientIP] = &LeaseView{
		ClientIP:        clientIP,
		ProxyID:         candidate.ID,
		Status:          LeaseActive,
		ExpiresAt:       candidate.ProviderExpiresAt,
		Mode:            ProxySourceAPI,
		Country:         request.Country,
		DurationMinutes: request.DurationMinutes,
	}
	m.db.AddEvent(ctx, clientIP, candidate.ID, "assigned", fmt.Sprintf("mode=api refresh=%v country=%s duration_minutes=%d", refresh, request.Country, request.DurationMinutes))
	assignment := assignmentFromLease(m.leases[clientIP], LeaseActive)
	m.mu.Unlock()
	if wakeQueue {
		m.processQueueAsync()
	}
	return assignment, nil
}

func (m *Manager) TestProxyAPI(ctx context.Context, request DynamicLeaseRequest) (ProxyAPITestResult, error) {
	normalized, err := NormalizeDynamicLeaseRequest(request)
	if err != nil {
		return ProxyAPITestResult{}, &LeaseAPIError{Code: "invalid_request", Message: err.Error(), Err: err}
	}
	proxyAPIConfig, client := m.proxyAPISnapshot()
	if !proxyAPIConfig.Enabled || client == nil {
		return ProxyAPITestResult{}, m.dynamicLeaseError("", "proxy_api_disabled", "proxy API mode is disabled", nil)
	}
	startedAt := time.Now()
	endpoint, err := client.Acquire(ctx, normalized.Country, normalized.DurationMinutes)
	if err != nil {
		code := "proxy_api_request_failed"
		if providerapi.ErrorKindOf(err) == providerapi.ErrorResponse {
			code = "proxy_api_invalid_response"
		}
		return ProxyAPITestResult{}, m.dynamicLeaseError("", code, err.Error(), err)
	}
	candidate := Proxy{
		Address:  endpoint.Address,
		Username: endpoint.Username,
		Password: endpoint.Password,
		Source:   ProxySourceAPI,
	}
	exitIP, err := m.probeProxy(ctx, candidate)
	if err != nil {
		return ProxyAPITestResult{}, m.dynamicLeaseError("", "dynamic_proxy_unusable", "the acquired SOCKS5 proxy failed validation", err)
	}
	return ProxyAPITestResult{
		OK:        true,
		Code:      "healthy",
		Address:   endpoint.Address,
		ExitIP:    exitIP,
		ElapsedMS: time.Since(startedAt).Milliseconds(),
	}, nil
}

func (m *Manager) DynamicRequestForClient(clientIP string) (DynamicLeaseRequest, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	lease := m.leases[clientIP]
	if lease == nil || lease.Mode != ProxySourceAPI || lease.Status != LeaseActive || !lease.ExpiresAt.After(time.Now()) {
		return DynamicLeaseRequest{}, false
	}
	return DynamicLeaseRequest{Country: lease.Country, DurationMinutes: lease.DurationMinutes}, true
}

func (m *Manager) ProxyAPIConfig() config.ProxyAPI {
	m.proxyAPIMu.RLock()
	defer m.proxyAPIMu.RUnlock()
	return m.proxyAPIConfig
}

func (m *Manager) UpdateProxyAPI(next config.ProxyAPI, persist func() error) error {
	next.URL = strings.TrimSpace(next.URL)
	next.CountryParam = strings.TrimSpace(next.CountryParam)
	next.DurationParam = strings.TrimSpace(next.DurationParam)
	if next.CountryParam == "" {
		next.CountryParam = "region"
	}
	if next.DurationParam == "" {
		next.DurationParam = "time"
	}
	m.proxyAPIMu.Lock()
	defer m.proxyAPIMu.Unlock()
	candidate, err := providerapi.New(next, m.cfg.FrontProxy)
	if err != nil {
		return err
	}
	if persist == nil {
		return errors.New("proxy API persistence is unavailable")
	}
	if err := persist(); err != nil {
		return err
	}
	m.proxyAPIConfig = next
	m.proxyAPIClient = candidate
	m.cfg.ProxyAPI = next
	return nil
}

func (m *Manager) proxyAPISnapshot() (config.ProxyAPI, proxyAPIAcquirer) {
	m.proxyAPIMu.RLock()
	defer m.proxyAPIMu.RUnlock()
	return m.proxyAPIConfig, m.proxyAPIClient
}

func (m *Manager) dynamicLeaseError(clientIP, code, message string, err error) *LeaseAPIError {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dynamicLeaseErrorLocked(code, message, err, clientIP)
}

func (m *Manager) dynamicLeaseErrorLocked(code, message string, err error, clientIP string) *LeaseAPIError {
	apiErr := &LeaseAPIError{Code: code, Message: message, Err: err}
	if clientIP != "" {
		current := m.currentAssignmentLocked(clientIP)
		if current.ProxyID != "" {
			apiErr.CurrentLease = &current
		}
	}
	return apiErr
}

func (m *Manager) currentAssignmentLocked(clientIP string) Assignment {
	if clientIP == "" {
		return Assignment{}
	}
	lease := m.leases[clientIP]
	if lease == nil {
		return Assignment{ClientIP: clientIP, Status: LeaseBlocked}
	}
	status := lease.Status
	if m.inQueueLocked(m.pendingRefs, clientIP) {
		status = LeasePending
	}
	return assignmentFromLease(lease, status)
}

func assignmentFromLease(lease *LeaseView, status string) Assignment {
	if lease == nil {
		return Assignment{}
	}
	return Assignment{
		ClientIP:        lease.ClientIP,
		ProxyID:         lease.ProxyID,
		Status:          status,
		ExpiresAt:       lease.ExpiresAt,
		Mode:            lease.Mode,
		Country:         lease.Country,
		DurationMinutes: lease.DurationMinutes,
	}
}

func (m *Manager) proxyAddressExistsLocked(address string) bool {
	for _, proxy := range m.proxies {
		if proxy != nil && sameEndpoint(proxy.Address, address) {
			return true
		}
	}
	return false
}

func (m *Manager) lockDynamicGate(ctx context.Context, clientIP string) (func(), error) {
	m.dynamicGatesMu.Lock()
	gate := m.dynamicGates[clientIP]
	if gate == nil {
		gate = &dynamicGate{token: make(chan struct{}, 1)}
		m.dynamicGates[clientIP] = gate
	}
	gate.refs++
	m.dynamicGatesMu.Unlock()

	releaseRef := func() {
		m.dynamicGatesMu.Lock()
		gate.refs--
		if gate.refs == 0 {
			delete(m.dynamicGates, clientIP)
		}
		m.dynamicGatesMu.Unlock()
	}
	select {
	case gate.token <- struct{}{}:
		return func() {
			<-gate.token
			releaseRef()
		}, nil
	case <-ctx.Done():
		releaseRef()
		return nil, ctx.Err()
	}
}

func newDynamicProxyID() (string, error) {
	bytes := make([]byte, 8)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return "api-" + hex.EncodeToString(bytes), nil
}
