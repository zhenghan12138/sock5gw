package manager

import "testing"

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
