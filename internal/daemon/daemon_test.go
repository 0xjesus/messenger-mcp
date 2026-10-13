package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/0xjesus/messenger-mcp/internal/ingest"
	"github.com/0xjesus/messenger-mcp/internal/ledger"
	"github.com/0xjesus/messenger-mcp/internal/outbox"
	"github.com/0xjesus/messenger-mcp/internal/pair"
)

func writeSecrets(t *testing.T, dir, body string, mode os.FileMode) {
	t.Helper()
	p := filepath.Join(dir, "secrets.env")
	if err := os.WriteFile(p, []byte(body), mode); err != nil {
		t.Fatal(err)
	}
	os.Chmod(p, mode)
}

const goodSecrets = `# comment
MATRIX_PASSWORD=pw
BRIDGE_PROVISIONING_SECRET="bridge secret"
INTERNAL_SECRET=internal
`

func TestLoadConfigDefaultsAndValidation(t *testing.T) {
	dir := t.TempDir()
	writeSecrets(t, dir, goodSecrets, 0o600)
	c, err := LoadConfig(dir)
	if err != nil {
		t.Fatal(err)
	}
	if c.Listen != "127.0.0.1:7381" || c.Homeserver != "http://127.0.0.1:7382" || c.BridgeURL != "http://127.0.0.1:7383" ||
		c.UserID != "@jesus:messenger.local" || c.BridgeBot != "@metabot:messenger.local" || c.BridgeSecret != "bridge secret" {
		t.Fatalf("config %+v", c)
	}
	if c.UploadsDir != filepath.Join(dir, "uploads") || c.DownloadsDir != filepath.Join(dir, "downloads") {
		t.Fatalf("dirs %+v", c)
	}

	writeSecrets(t, dir, goodSecrets, 0o644)
	if _, err := LoadConfig(dir); err == nil || !strings.Contains(err.Error(), "600") {
		t.Fatalf("world-readable secrets must be refused: %v", err)
	}
	writeSecrets(t, dir, "MATRIX_PASSWORD=pw\n", 0o600)
	if _, err := LoadConfig(dir); err == nil || !strings.Contains(err.Error(), "BRIDGE_PROVISIONING_SECRET") {
		t.Fatalf("missing secret must be named: %v", err)
	}
	writeSecrets(t, dir, goodSecrets+"LISTEN=0.0.0.0:7381\n", 0o600)
	if _, err := LoadConfig(dir); err == nil {
		t.Fatalf("non-loopback listen must be refused")
	}
}

func TestLoopbackHostGuard(t *testing.T) {
	h := loopbackOnly("7381", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusTeapot) }))
	for host, want := range map[string]int{
		"127.0.0.1:7381": http.StatusTeapot, "localhost:7381": http.StatusTeapot, "[::1]:7381": http.StatusTeapot,
		"evil.example:7381": http.StatusForbidden, "127.0.0.1:80": http.StatusForbidden, "attacker.localhost:7381": http.StatusForbidden,
	} {
		req := httptest.NewRequest("GET", "/mcp", nil)
		req.Host = host
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != want {
			t.Fatalf("host %s: %d want %d", host, rec.Code, want)
		}
	}
}

func TestPairTokenEndpointRequiresInternalSecret(t *testing.T) {
	tokens := pair.NewTokens(nil)
	h := pairTokenHandler("internal", "http://127.0.0.1:7381", tokens)
	req := httptest.NewRequest("POST", "/internal/pair-token", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no secret: %d", rec.Code)
	}
	req = httptest.NewRequest("POST", "/internal/pair-token", nil)
	req.Header.Set("Authorization", "Bearer internal")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var out struct{ URL string }
	json.Unmarshal(rec.Body.Bytes(), &out)
	tok, ok := strings.CutPrefix(out.URL, "http://127.0.0.1:7381/pair?token=")
	if rec.Code != http.StatusOK || !ok {
		t.Fatalf("url %q code %d", out.URL, rec.Code)
	}
	if _, valid := tokens.Consume(tok); !valid {
		t.Fatalf("issued token must be consumable")
	}
}

func TestClassifySendError(t *testing.T) {
	forbidden := mautrix.HTTPError{Response: &http.Response{StatusCode: 403}, RespError: &mautrix.RespError{ErrCode: "M_FORBIDDEN"}}
	limited := mautrix.HTTPError{Response: &http.Response{StatusCode: 429}}
	server := mautrix.HTTPError{Response: &http.Response{StatusCode: 502}}
	if !outbox.IsPermanent(classify(forbidden)) {
		t.Fatalf("4xx must be permanent")
	}
	for _, e := range []error{limited, server, errors.New("connection reset")} {
		if outbox.IsPermanent(classify(e)) {
			t.Fatalf("%v must be retriable", e)
		}
	}
	if classify(nil) != nil {
		t.Fatalf("nil stays nil")
	}
}

func parsed(t *testing.T, evt *event.Event) *event.Event {
	t.Helper()
	if err := evt.Content.ParseRaw(evt.Type); err != nil {
		t.Fatal(err)
	}
	return evt
}

func TestRouterFeedsLedgerAndStatus(t *testing.T) {
	l, err := ledger.Open(filepath.Join(t.TempDir(), "l.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	var statuses []ingest.StatusUpdate
	r := &router{
		ing:      &ingest.Handler{L: l, OperatorID: "@jesus:messenger.local", BridgeBot: "@metabot:messenger.local"},
		onStatus: func(u ingest.StatusUpdate) { statuses = append(statuses, u) },
	}
	ctx := context.Background()
	room := id.RoomID("!r:messenger.local")
	sk := ""
	r.handle(ctx, parsed(t, &event.Event{Type: event.StateBridge, StateKey: &sk, RoomID: room, Sender: "@metabot:messenger.local",
		Content: event.Content{VeryRaw: []byte(`{"bridgebot":"@metabot:messenger.local","channel":{"id":"245962885415945","displayname":"Sala Forum"},"com.beeper.room_type":"dm"}`)}}))
	r.handle(ctx, parsed(t, &event.Event{Type: event.EventMessage, RoomID: room, ID: "$m1", Sender: "@meta_9:messenger.local", Timestamp: 1_700_000_000_000,
		Content: event.Content{VeryRaw: []byte(`{"msgtype":"m.text","body":"Qué tal amigo."}`)}}))
	r.handle(ctx, parsed(t, &event.Event{Type: event.BeeperMessageStatus, RoomID: room, Sender: "@metabot:messenger.local",
		Content: event.Content{VeryRaw: []byte(`{"status":"SUCCESS","m.relates_to":{"rel_type":"m.reference","event_id":"$out"},"relates_to_txn_id":"msg-1"}`)}}))
	msgs, err := l.GetMessages(ctx, "245962885415945", 10, 0)
	if err != nil || len(msgs) != 1 || msgs[0].Text != "Qué tal amigo." {
		t.Fatalf("msgs %+v err=%v", msgs, err)
	}
	if len(statuses) != 1 || statuses[0].TxnID != "msg-1" || statuses[0].EventID != "$out" {
		t.Fatalf("statuses %+v", statuses)
	}
}

func TestShouldAutoJoin(t *testing.T) {
	op := id.UserID("@jesus:messenger.local")
	bot := id.UserID("@metabot:messenger.local")
	cases := []struct {
		sender id.UserID
		want   bool
	}{{bot, true}, {"@meta_123:messenger.local", true}, {"@stranger:messenger.local", false}, {"@meta_1:evil.org", false}}
	for _, c := range cases {
		if got := shouldAutoJoin(c.sender, bot, "messenger.local"); got != c.want {
			t.Fatalf("%s: %v", c.sender, got)
		}
	}
	_ = op
}
