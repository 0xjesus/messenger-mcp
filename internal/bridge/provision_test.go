package bridge

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func fakeBridge(t *testing.T, handler func(w http.ResponseWriter, r *http.Request, body map[string]string)) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer s3cret" || r.URL.Query().Get("user_id") != "@jesus:messenger.local" {
			w.WriteHeader(http.StatusUnauthorized)
			io.WriteString(w, `{"errcode":"M_UNKNOWN_TOKEN","error":"bad auth"}`)
			return
		}
		var body map[string]string
		if r.Body != nil {
			b, _ := io.ReadAll(r.Body)
			if len(b) > 0 {
				json.Unmarshal(b, &body)
			}
		}
		handler(w, r, body)
	}))
	t.Cleanup(srv.Close)
	return &Client{BaseURL: srv.URL, Secret: "s3cret", UserID: "@jesus:messenger.local"}
}

func TestStartAndStepForwardFields(t *testing.T) {
	var gotPath string
	var gotBody map[string]string
	c := fakeBridge(t, func(w http.ResponseWriter, r *http.Request, body map[string]string) {
		gotPath, gotBody = r.Method+" "+r.URL.Path, body
		switch r.URL.Path {
		case "/_matrix/provision/v3/login/start/messenger-android":
			io.WriteString(w, `{"login_id":"L1","type":"user_input","step_id":"s1","instructions":"Log in","user_input":{"fields":[{"type":"username","id":"username","name":"Username"},{"type":"password","id":"password","name":"Password"}]}}`)
		default:
			io.WriteString(w, `{"login_id":"L1","type":"complete","step_id":"done","complete":{"user_login_id":"100"}}`)
		}
	})
	step, err := c.Start(context.Background(), "messenger-android")
	if err != nil || step.LoginID != "L1" || step.Type != "user_input" || len(step.UserInput.Fields) != 2 || step.UserInput.Fields[1].Type != "password" {
		t.Fatalf("start: %+v err=%v", step, err)
	}
	next, err := c.Step(context.Background(), "L1", "s1", "user_input", map[string]string{"username": "u", "password": "p"})
	if err != nil || next.Type != "complete" {
		t.Fatalf("step: %+v err=%v", next, err)
	}
	if gotPath != "POST /_matrix/provision/v3/login/step/L1/s1/user_input" || gotBody["password"] != "p" {
		t.Fatalf("forwarded %s %v", gotPath, gotBody)
	}
}

func TestBridgeErrorsSurface(t *testing.T) {
	c := fakeBridge(t, func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, `{"errcode":"FI.MAU.META_CHECKPOINT","error":"Checkpoint required"}`)
	})
	_, err := c.Start(context.Background(), "messenger-android")
	if err == nil || err.Error() != "bridge: Checkpoint required (FI.MAU.META_CHECKPOINT)" {
		t.Fatalf("err=%v", err)
	}
	c.Secret = "wrong"
	if _, err := c.Flows(context.Background()); err == nil {
		t.Fatalf("auth failure must error")
	}
}

func TestLoginStateMapping(t *testing.T) {
	cases := map[string]string{
		"":                     StateNotLoggedIn,
		"CONNECTED":            StateConnected,
		"BAD_CREDENTIALS":      StateNeedsRelogin,
		"LOGGED_OUT":           StateNeedsRelogin,
		"TRANSIENT_DISCONNECT": StateDisconnected,
		"CONNECTING":           StateDisconnected,
	}
	for in, want := range cases {
		c := fakeBridge(t, func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
			if in == "" {
				io.WriteString(w, `{"bridge_bot":"@metabot:messenger.local","logins":[]}`)
				return
			}
			io.WriteString(w, `{"bridge_bot":"@metabot:messenger.local","logins":[{"id":"100","name":"Jesús","state":{"state_event":"`+in+`","error":"x"}}]}`)
		})
		st, err := c.Status(context.Background())
		if err != nil || st.State != want {
			t.Fatalf("%q: %+v err=%v", in, st, err)
		}
		if in == "CONNECTED" && (st.LoginID != "100" || st.Name != "Jesús") {
			t.Fatalf("account fields: %+v", st)
		}
	}
}
