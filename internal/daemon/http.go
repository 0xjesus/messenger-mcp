package daemon

import (
	"crypto/subtle"
	"encoding/json"
	"net"
	"net/http"
	"time"

	"github.com/0xjesus/messenger-mcp/internal/pair"
)

const pairTTL = 15 * time.Minute

// loopbackOnly rejects requests whose Host is not a loopback name on our port,
// which blocks DNS-rebinding pages from reaching the service through a browser.
func loopbackOnly(port string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, p, err := net.SplitHostPort(r.Host)
		if err != nil || p != port || (host != "localhost" && !isLoopbackIP(host)) {
			http.Error(w, "forbidden host", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func isLoopbackIP(h string) bool {
	ip := net.ParseIP(h)
	return ip != nil && ip.IsLoopback()
}

// pairTokenHandler mints a single-use /pair link for the CLI on this Mac.
func pairTokenHandler(secret, baseURL string, tokens *pair.Tokens) http.Handler {
	want := []byte("Bearer " + secret)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"url":        baseURL + "/pair?token=" + tokens.Issue(pairTTL),
			"expires_in": pairTTL.String(),
		})
	})
}
