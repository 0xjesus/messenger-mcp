package pair

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/0xjesus/messenger-mcp/internal/bridge"
)

type fakeBridge struct {
	started  string
	stepArgs []string
	values   map[string]string
}

func (f *fakeBridge) Flows(context.Context) ([]bridge.Flow, error) {
	return []bridge.Flow{{ID: "messenger", Name: "messenger.com"}, {ID: "messenger-lite-android", Name: "Messenger Android"}}, nil
}

func (f *fakeBridge) Start(_ context.Context, flow string) (*bridge.Step, error) {
	f.started = flow
	s := &bridge.Step{LoginID: "L1", Type: "user_input", StepID: "s1", Instructions: "Log in"}
	s.UserInput = &struct {
		Fields []bridge.InputField `json:"fields"`
	}{Fields: []bridge.InputField{{Type: "username", ID: "username", Name: "Usuario"}, {Type: "password", ID: "password", Name: "Contraseña"}}}
	return s, nil
}

func (f *fakeBridge) Step(_ context.Context, loginID, stepID, stepType string, values map[string]string) (*bridge.Step, error) {
	f.stepArgs = []string{loginID, stepID, stepType}
	f.values = values
	return &bridge.Step{LoginID: loginID, Type: "complete", StepID: "done"}, nil
}

func (f *fakeBridge) Cancel(context.Context, string) error { return nil }

type clock struct{ now time.Time }

func setup(t *testing.T) (*Handler, *fakeBridge, *clock) {
	c := &clock{now: time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)}
	b := &fakeBridge{}
	return &Handler{T: NewTokens(func() time.Time { return c.now }), B: b}, b, c
}

func do(h http.Handler, method, target string, form url.Values, cookie *http.Cookie) *httptest.ResponseRecorder {
	var body io.Reader
	if form != nil {
		body = strings.NewReader(form.Encode())
	}
	req := httptest.NewRequest(method, target, body)
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	if cookie != nil {
		req.AddCookie(cookie)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func sessionCookie(t *testing.T, rec *httptest.ResponseRecorder) *http.Cookie {
	t.Helper()
	for _, c := range rec.Result().Cookies() {
		if c.Name == cookieName {
			if !c.HttpOnly || c.SameSite != http.SameSiteStrictMode {
				t.Fatalf("session cookie must be HttpOnly+Strict: %+v", c)
			}
			return c
		}
	}
	t.Fatalf("no session cookie")
	return nil
}

func TestTokenIsSingleUseAndExpires(t *testing.T) {
	h, _, c := setup(t)
	if rec := do(h, "GET", "/pair", nil, nil); rec.Code != http.StatusForbidden {
		t.Fatalf("no token: %d", rec.Code)
	}
	tok := h.T.Issue(15 * time.Minute)
	rec := do(h, "GET", "/pair?token="+tok, nil, nil)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "Messenger Android") {
		t.Fatalf("first use: %d %s", rec.Code, rec.Body.String())
	}
	sess := sessionCookie(t, rec)
	if rec := do(h, "GET", "/pair?token="+tok, nil, nil); rec.Code != http.StatusForbidden {
		t.Fatalf("reused token must be refused: %d", rec.Code)
	}
	if rec := do(h, "GET", "/pair", nil, sess); rec.Code != http.StatusOK {
		t.Fatalf("session continues: %d", rec.Code)
	}
	old := h.T.Issue(15 * time.Minute)
	c.now = c.now.Add(16 * time.Minute)
	if rec := do(h, "GET", "/pair?token="+old, nil, nil); rec.Code != http.StatusForbidden {
		t.Fatalf("expired token: %d", rec.Code)
	}
	c.now = c.now.Add(31 * time.Minute)
	if rec := do(h, "GET", "/pair", nil, sess); rec.Code != http.StatusForbidden {
		t.Fatalf("expired session: %d", rec.Code)
	}
}

func TestStartRendersFieldsAndStepForwardsWithoutEcho(t *testing.T) {
	h, b, _ := setup(t)
	sess := sessionCookie(t, do(h, "GET", "/pair?token="+h.T.Issue(time.Minute), nil, nil))
	if rec := do(h, "POST", "/pair/start", url.Values{"flow": {"messenger-lite-android"}}, nil); rec.Code != http.StatusForbidden {
		t.Fatalf("start without session: %d", rec.Code)
	}
	rec := do(h, "POST", "/pair/start", url.Values{"flow": {"messenger-lite-android"}}, sess)
	page := rec.Body.String()
	if b.started != "messenger-lite-android" || !strings.Contains(page, `type="password"`) || !strings.Contains(page, `name="f_username"`) {
		t.Fatalf("start page: %s", page)
	}
	rec = do(h, "POST", "/pair/step", url.Values{
		"login_id": {"L1"}, "step_id": {"s1"}, "step_type": {"user_input"},
		"f_username": {"jesus@example.com"}, "f_password": {"hunter2"},
	}, sess)
	if strings.Join(b.stepArgs, "/") != "L1/s1/user_input" || b.values["password"] != "hunter2" || b.values["username"] != "jesus@example.com" {
		t.Fatalf("forwarded %v %v", b.stepArgs, b.values)
	}
	if strings.Contains(rec.Body.String(), "hunter2") {
		t.Fatalf("password must never be echoed")
	}
	if !strings.Contains(rec.Body.String(), "Listo") {
		t.Fatalf("complete page: %s", rec.Body.String())
	}
}

func TestParseCookies(t *testing.T) {
	cases := map[string]map[string]string{
		`{"c_user":"100","xs":"abc"}`: {"c_user": "100", "xs": "abc"},
		`curl 'https://www.messenger.com/' -H 'accept: */*' -H 'cookie: c_user=100; xs=abc%3A1; datr=z'`: {"c_user": "100", "xs": "abc%3A1", "datr": "z"},
		`curl 'https://www.messenger.com/' -b 'c_user=100; xs=abc'`:                                   {"c_user": "100", "xs": "abc"},
		`c_user=100; xs=abc`: {"c_user": "100", "xs": "abc"},
	}
	for in, want := range cases {
		got, err := ParseCookies(in)
		if err != nil {
			t.Fatalf("%q: %v", in, err)
		}
		for k, v := range want {
			if got[k] != v {
				t.Fatalf("%q: %s=%q want %q (got %v)", in, k, got[k], v, got)
			}
		}
	}
	if _, err := ParseCookies("   "); err == nil {
		t.Fatalf("empty input must error")
	}
}
