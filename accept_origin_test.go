package nwws

import "testing"

func TestDefaultOriginCheckPorts(t *testing.T) {
	tests := []struct {
		origin, host string
		want         bool
	}{
		{"https://a", "a:443", true},
		{"http://a:80", "a", true},
		{"https://a:8443", "a", false},
		{"https://A.Example", "a.example:443", true},
		{"HTTP://a.example:80", "A.EXAMPLE", true},
		{"wss://a", "a:443", true},
		{"ws://a:80", "a", true},
		{"http://a", "a:80", true},
		{"https://a:443", "a", true},
		{"https://a:443", "a:443", true},
		{"http://a", "a", true},
		{"https://a", "a", true},
		{"http://a:8080", "a:8080", true},
		{"http://[::1]:80", "[::1]", true},
		{"https://[::1]", "[::1]:443", true},
		{"http://a", "a:443", false},
		{"https://a", "a:80", false},
		{"http://a:8080", "a", false},
		{"http://a", "a:8080", false},
		{"https://a:8443", "a:443", false},
		{"https://b", "a:443", false},
		{"http://a:80", "b", false},
		{"null", "a", false},
		{"https://", "a", false},
	}
	for _, tt := range tests {
		t.Run(tt.origin+" for "+tt.host, func(t *testing.T) {
			r := newUpgradeRequest()
			r.Host = tt.host
			r.Header.Set("Origin", tt.origin)
			if got := sameOrigin(r); got != tt.want {
				t.Errorf("sameOrigin = %v, want %v", got, tt.want)
			}
			_, rej := checkRequest(r, nil)
			if (rej == nil) != tt.want {
				t.Errorf("checkRequest rejection %v, want accepted %v", rej, tt.want)
			}
		})
	}
}
