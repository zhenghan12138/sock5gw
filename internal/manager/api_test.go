package manager

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sock5gw/internal/config"
	"sock5gw/internal/providerapi"
)

func TestValidCountryCode(t *testing.T) {
	if !validCountryCode("us") {
		t.Fatal("expected us to be valid")
	}
	for _, code := range []string{"", "u", "usa", "1s", "u1"} {
		if validCountryCode(code) {
			t.Fatalf("expected %q to be invalid", code)
		}
	}
}

func TestUniqueIPs(t *testing.T) {
	got := uniqueIPs([]string{" 198.51.100.1 ", "bad", "198.51.100.1", "2001:db8::1"})
	want := []string{"198.51.100.1", "2001:db8::1"}
	if len(got) != len(want) {
		t.Fatalf("got %#v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %#v, want %#v", got, want)
		}
	}
}

func TestFrontProxyInputPreservesAndClearsCredentials(t *testing.T) {
	current := config.FrontProxy{
		Protocol: "socks5",
		Address:  "127.0.0.1:11080",
		Username: "saved-user",
		Password: "saved-secret",
	}
	next, err := frontProxyFromInput(current, frontProxyUpdateInput{
		Enabled: true,
		URL:     "socks5://localhost:12080",
	})
	if err != nil {
		t.Fatal(err)
	}
	if next.Address != "localhost:12080" || next.Username != current.Username || next.Password != current.Password {
		t.Fatalf("credentials were not preserved: %+v", next)
	}
	next, err = frontProxyFromInput(next, frontProxyUpdateInput{
		Enabled:          true,
		URL:              "socks5://localhost:12080",
		ClearCredentials: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if next.Username != "" || next.Password != "" {
		t.Fatalf("credentials were not cleared: %+v", next)
	}
}

func TestFrontProxyAPIHotUpdatesWithoutReturningCredentials(t *testing.T) {
	cfg := testConfig(1)
	manager := testManagerFromConfig(t, cfg)
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"front_proxy":{"enabled":false}}`), 0600); err != nil {
		t.Fatal(err)
	}
	runtimeCfg := NewRuntimeConfig(path, cfg, nil, manager.UpdateFrontProxy)
	handler := NewAPI(manager, cfg.API, runtimeCfg)

	recorder := doAdminRequest(t, handler, http.MethodPut, "/v1/admin/front-proxy",
		`{"enabled":true,"url":"socks5://front-user:front-secret@127.0.0.1:11080"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("PUT status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if body := recorder.Body.String(); strings.Contains(body, "front-user") || strings.Contains(body, "front-secret") {
		t.Fatalf("PUT response leaked credentials: %s", body)
	}
	if status := manager.FrontStatus(); !status.Enabled || status.Address != "127.0.0.1:11080" {
		t.Fatalf("front proxy was not hot updated: %+v", status)
	}
	saved := string(mustReadFile(t, path))
	if !strings.Contains(saved, "front-user") || !strings.Contains(saved, "front-secret") {
		t.Fatalf("credentials were not persisted: %s", saved)
	}

	recorder = doAdminRequest(t, handler, http.MethodGet, "/v1/admin/front-proxy", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if body := recorder.Body.String(); strings.Contains(body, "front-user") || strings.Contains(body, "front-secret") {
		t.Fatalf("GET response leaked credentials: %s", body)
	}

	recorder = doAdminRequest(t, handler, http.MethodPut, "/v1/admin/front-proxy",
		`{"enabled":true,"url":"socks5://127.0.0.1:11080","clear_credentials":true}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("clear status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	current := runtimeCfg.FrontProxy()
	if current.Username != "" || current.Password != "" {
		t.Fatalf("credentials were not cleared: %+v", current)
	}
}

func TestFrontProxyAPIRejectsExitAddressWithoutPersisting(t *testing.T) {
	cfg := testConfig(1)
	manager := testManagerFromConfig(t, cfg)
	path := filepath.Join(t.TempDir(), "config.json")
	original := `{"front_proxy":{"enabled":false}}`
	if err := os.WriteFile(path, []byte(original), 0600); err != nil {
		t.Fatal(err)
	}
	runtimeCfg := NewRuntimeConfig(path, cfg, nil, manager.UpdateFrontProxy)
	handler := NewAPI(manager, cfg.API, runtimeCfg)

	recorder := doAdminRequest(t, handler, http.MethodPut, "/v1/admin/front-proxy",
		`{"enabled":true,"url":"socks5://127.0.0.1:1080"}`)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("PUT status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if got := string(mustReadFile(t, path)); got != original {
		t.Fatalf("rejected update changed config: %s", got)
	}
	if manager.FrontStatus().Enabled {
		t.Fatal("rejected update changed connector")
	}
}

func TestFrontProxyTestAPIRequiresAdminAndReturnsStructuredResult(t *testing.T) {
	cfg := testConfig(1)
	manager := testManagerFromConfig(t, cfg)
	handler := NewAPI(manager, cfg.API, nil)

	request := httptest.NewRequest(http.MethodPost, "/v1/admin/front-proxy/test", strings.NewReader(`{}`))
	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, request)
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", unauthorized.Code)
	}

	recorder := doAdminRequest(t, handler, http.MethodPost, "/v1/admin/front-proxy/test", `{}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("test status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if body := recorder.Body.String(); !strings.Contains(body, `"code":"disabled"`) || !strings.Contains(body, `"status":"disabled"`) {
		t.Fatalf("unexpected test response: %s", body)
	}
}

func TestDynamicLeaseAPIUsesOptionalRequestBody(t *testing.T) {
	cfg := dynamicTestConfig(":memory:")
	manager := testManagerFromConfig(t, cfg)
	fake := &fakeProxyAPI{outcomes: []proxyAPIOutcome{{endpoint: providerapi.Endpoint{Address: "198.51.100.60:1080"}}}}
	setFakeProxyAPI(manager, fake)
	handler := NewAPI(manager, cfg.API, nil)

	request := httptest.NewRequest(http.MethodPost, "/v1/lease", strings.NewReader(`{"country":"us","duration_minutes":10}`))
	request.RemoteAddr = "192.0.2.60:43210"
	request.Header.Set("Authorization", "Bearer client")
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if body := recorder.Body.String(); !strings.Contains(body, `"mode":"api"`) || !strings.Contains(body, `"country":"US"`) {
		t.Fatalf("response = %s", body)
	}

	request = httptest.NewRequest(http.MethodPost, "/v1/lease", strings.NewReader(`{"country":"US"}`))
	request.RemoteAddr = "192.0.2.61:43210"
	request.Header.Set("Authorization", "Bearer client")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), `"code":"invalid_request"`) {
		t.Fatalf("invalid status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestDynamicLeaseAPIErrorReturnsCurrentLease(t *testing.T) {
	cfg := dynamicTestConfig(":memory:")
	manager := testManagerFromConfig(t, cfg)
	fake := &fakeProxyAPI{outcomes: []proxyAPIOutcome{
		{endpoint: providerapi.Endpoint{Address: "198.51.100.61:1080"}},
		{err: errors.New("provider unavailable")},
	}}
	setFakeProxyAPI(manager, fake)
	handler := NewAPI(manager, cfg.API, nil)

	for index, body := range []string{
		`{"country":"US","duration_minutes":10}`,
		`{"country":"JP","duration_minutes":10}`,
	} {
		request := httptest.NewRequest(http.MethodPost, "/v1/lease", strings.NewReader(body))
		request.RemoteAddr = "192.0.2.62:43210"
		request.Header.Set("Authorization", "Bearer client")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if index == 0 && recorder.Code != http.StatusOK {
			t.Fatalf("initial status = %d, body = %s", recorder.Code, recorder.Body.String())
		}
		if index == 1 {
			if recorder.Code != http.StatusBadGateway || !strings.Contains(recorder.Body.String(), `"current_lease"`) {
				t.Fatalf("failure status = %d, body = %s", recorder.Code, recorder.Body.String())
			}
		}
	}
}

func TestProxyAPIAdminConfigurationAndTest(t *testing.T) {
	cfg := dynamicTestConfig(":memory:")
	manager := testManagerFromConfig(t, cfg)
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"proxy_api":{"enabled":false}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	runtimeCfg := NewRuntimeConfig(path, cfg, nil, manager.UpdateFrontProxy, manager.UpdateProxyAPI)
	handler := NewAPI(manager, cfg.API, runtimeCfg)
	recorder := doAdminRequest(t, handler, http.MethodPut, "/v1/admin/proxy-api",
		`{"enabled":true,"url":"https://provider.example/api?num=1&type=json","country_param":"region","duration_param":"time"}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("PUT status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if saved := string(mustReadFile(t, path)); !strings.Contains(saved, `"country_param": "region"`) {
		t.Fatalf("saved config = %s", saved)
	}

	setFakeProxyAPI(manager, &fakeProxyAPI{outcomes: []proxyAPIOutcome{{endpoint: providerapi.Endpoint{Address: "198.51.100.63:1080"}}}})
	recorder = doAdminRequest(t, handler, http.MethodPost, "/v1/admin/proxy-api/test", `{"country":"Rand","duration_minutes":10}`)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"code":"healthy"`) {
		t.Fatalf("test status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestAdminPageContainsProxyAPIControls(t *testing.T) {
	cfg := testConfig(0)
	manager := testManagerFromConfig(t, cfg)
	handler := NewAPI(manager, cfg.API, nil)
	request := httptest.NewRequest(http.MethodGet, "/admin/", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d", recorder.Code)
	}
	for _, id := range []string{"proxyAPIForm", "proxyAPIURL", "proxyAPICountryParam", "proxyAPITest"} {
		if !strings.Contains(recorder.Body.String(), `id="`+id+`"`) {
			t.Fatalf("admin page missing %s", id)
		}
	}
	if !strings.Contains(recorder.Body.String(), `<div class="stat"><span class="muted">等待队列</span><b id="queueCount">0</b></div>
    </div>
    <section>`) {
		t.Fatal("statistics grid is not closed before the settings sections")
	}
}

func doAdminRequest(t *testing.T, handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer admin")
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}
