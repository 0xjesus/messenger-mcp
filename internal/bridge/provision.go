// Package bridge talks to the mautrix-meta provisioning API (v3).
package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const (
	StateConnected    = "connected"
	StateDisconnected = "disconnected"
	StateNeedsRelogin = "needs_relogin"
	StateNotLoggedIn  = "not_logged_in"
	StateUnreachable  = "bridge_unreachable"
)

type Client struct {
	BaseURL string
	Secret  string
	UserID  string
	HTTP    *http.Client
}

type Flow struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type InputField struct {
	Type        string   `json:"type"`
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	Default     string   `json:"default_value,omitempty"`
	Options     []string `json:"options,omitempty"`
}

type CookieField struct {
	ID       string `json:"id"`
	Required bool   `json:"required"`
}

type Step struct {
	LoginID      string `json:"login_id"`
	Type         string `json:"type"`
	StepID       string `json:"step_id"`
	Instructions string `json:"instructions"`
	UserInput    *struct {
		Fields []InputField `json:"fields"`
	} `json:"user_input,omitempty"`
	Cookies *struct {
		URL    string        `json:"url"`
		Fields []CookieField `json:"fields"`
	} `json:"cookies,omitempty"`
	DisplayAndWait *struct {
		Type     string `json:"type"`
		Data     string `json:"data,omitempty"`
		ImageURL string `json:"image_url,omitempty"`
	} `json:"display_and_wait,omitempty"`
	Complete *struct {
		UserLoginID string `json:"user_login_id"`
	} `json:"complete,omitempty"`
}

type LoginStatus struct {
	State     string `json:"state"`
	RawState  string `json:"raw_state,omitempty"`
	LoginID   string `json:"login_id,omitempty"`
	Name      string `json:"name,omitempty"`
	Error     string `json:"error,omitempty"`
	BridgeBot string `json:"bridge_bot,omitempty"`
}

func (c *Client) httpClient() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	// display_and_wait steps block until the user acts on another device.
	return &http.Client{Timeout: 10 * time.Minute}
}

func (c *Client) do(ctx context.Context, method, path string, body any, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	u := c.BaseURL + "/_matrix/provision/v3" + path + "?user_id=" + url.QueryEscape(c.UserID)
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Secret)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return fmt.Errorf("bridge unreachable: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		var e struct {
			Code  string `json:"errcode"`
			Error string `json:"error"`
		}
		if json.Unmarshal(data, &e) == nil && e.Error != "" {
			return fmt.Errorf("bridge: %s (%s)", e.Error, e.Code)
		}
		return fmt.Errorf("bridge: HTTP %d", resp.StatusCode)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(data, out)
}

func (c *Client) Flows(ctx context.Context) ([]Flow, error) {
	var r struct {
		Flows []Flow `json:"flows"`
	}
	err := c.do(ctx, http.MethodGet, "/login/flows", nil, &r)
	return r.Flows, err
}

func (c *Client) Start(ctx context.Context, flowID string) (*Step, error) {
	var s Step
	err := c.do(ctx, http.MethodPost, "/login/start/"+url.PathEscape(flowID), map[string]string{}, &s)
	return &s, err
}

// Step submits user input or cookies; values pass through and are never kept.
func (c *Client) Step(ctx context.Context, loginID, stepID, stepType string, values map[string]string) (*Step, error) {
	var s Step
	if values == nil {
		values = map[string]string{}
	}
	err := c.do(ctx, http.MethodPost, "/login/step/"+url.PathEscape(loginID)+"/"+url.PathEscape(stepID)+"/"+url.PathEscape(stepType), values, &s)
	if s.LoginID == "" {
		s.LoginID = loginID
	}
	return &s, err
}

func (c *Client) Cancel(ctx context.Context, loginID string) error {
	return c.do(ctx, http.MethodPost, "/login/cancel/"+url.PathEscape(loginID), map[string]string{}, nil)
}

// Status summarises the first Messenger login of the operator.
func (c *Client) Status(ctx context.Context) (LoginStatus, error) {
	var r struct {
		BridgeBot string `json:"bridge_bot"`
		Logins    []struct {
			ID    string `json:"id"`
			Name  string `json:"name"`
			State struct {
				Event   string `json:"state_event"`
				Error   string `json:"error"`
				Message string `json:"message"`
			} `json:"state"`
		} `json:"logins"`
	}
	if err := c.do(ctx, http.MethodGet, "/whoami", nil, &r); err != nil {
		return LoginStatus{State: StateUnreachable, Error: err.Error()}, err
	}
	if len(r.Logins) == 0 {
		return LoginStatus{State: StateNotLoggedIn, BridgeBot: r.BridgeBot}, nil
	}
	l := r.Logins[0]
	st := LoginStatus{RawState: l.State.Event, LoginID: l.ID, Name: l.Name, BridgeBot: r.BridgeBot, Error: l.State.Message}
	if st.Error == "" {
		st.Error = l.State.Error
	}
	switch l.State.Event {
	case "CONNECTED", "BACKFILLING":
		st.State = StateConnected
		st.Error = ""
	case "BAD_CREDENTIALS", "LOGGED_OUT":
		st.State = StateNeedsRelogin
	default:
		st.State = StateDisconnected
	}
	return st, nil
}
