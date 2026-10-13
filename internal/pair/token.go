package pair

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

const sessionTTL = 30 * time.Minute

// Tokens issues single-use pairing links and the short sessions they open.
type Tokens struct {
	mu       sync.Mutex
	now      func() time.Time
	tokens   map[string]time.Time
	sessions map[string]time.Time
}

func NewTokens(now func() time.Time) *Tokens {
	if now == nil {
		now = time.Now
	}
	return &Tokens{now: now, tokens: map[string]time.Time{}, sessions: map[string]time.Time{}}
}

func random() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (t *Tokens) Issue(ttl time.Duration) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	tok := random()
	t.tokens[tok] = t.now().Add(ttl)
	return tok
}

// Consume spends a token and returns a new session id.
func (t *Tokens) Consume(tok string) (string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	exp, ok := t.tokens[tok]
	delete(t.tokens, tok)
	if !ok || t.now().After(exp) {
		return "", false
	}
	s := random()
	t.sessions[s] = t.now().Add(sessionTTL)
	return s, true
}

func (t *Tokens) ValidSession(s string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	exp, ok := t.sessions[s]
	if ok && t.now().After(exp) {
		delete(t.sessions, s)
		return false
	}
	return ok
}
