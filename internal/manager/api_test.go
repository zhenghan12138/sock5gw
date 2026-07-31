package manager

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sock5gw/internal/config"
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

func doAdminRequest(t *testing.T, handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer admin")
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}
