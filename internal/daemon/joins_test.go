package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/id"
)

func rateLimited(retryMS float64) error {
	return mautrix.HTTPError{
		Response:  &http.Response{StatusCode: http.StatusTooManyRequests},
		RespError: &mautrix.RespError{ErrCode: "M_LIMIT_EXCEEDED", ExtraData: map[string]any{"retry_after_ms": retryMS}},
	}
}

func TestJoinRetryDelayUsesRetryAfter(t *testing.T) {
	d, ok := joinRetryDelay(rateLimited(1500))
	if !ok || d != 1500*time.Millisecond {
		t.Fatalf("got %v %v, want 1.5s true", d, ok)
	}
	if d, ok := joinRetryDelay(rateLimited(10 * 60 * 1000)); !ok || d != time.Minute {
		t.Fatalf("long waits must be capped at a minute, got %v", d)
	}
	forbidden := mautrix.HTTPError{Response: &http.Response{StatusCode: http.StatusForbidden}}
	if _, ok := joinRetryDelay(forbidden); ok {
		t.Fatal("403 is not a rate limit")
	}
}

type fakeJoins struct {
	mu     sync.Mutex
	calls  map[id.RoomID]int
	fail   map[id.RoomID]error
	limits map[id.RoomID]int // remaining rate-limited attempts per room
	slept  []time.Duration
	done   chan id.RoomID
}

func newFakeJoins() *fakeJoins {
	return &fakeJoins{calls: map[id.RoomID]int{}, fail: map[id.RoomID]error{}, limits: map[id.RoomID]int{}, done: make(chan id.RoomID, 16)}
}

func (f *fakeJoins) join(_ context.Context, r id.RoomID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[r]++
	if f.limits[r] > 0 {
		f.limits[r]--
		return rateLimited(250)
	}
	if err := f.fail[r]; err != nil {
		f.done <- r
		return err
	}
	f.done <- r
	return nil
}

func (f *fakeJoins) sleep(_ context.Context, d time.Duration) bool {
	f.mu.Lock()
	f.slept = append(f.slept, d)
	f.mu.Unlock()
	return true
}

func startJoiner(t *testing.T, f *fakeJoins) *joiner {
	t.Helper()
	j := newJoiner(f.join, nil)
	j.sleep = f.sleep
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go j.Run(ctx)
	return j
}

func waitJoin(t *testing.T, f *fakeJoins, want id.RoomID) {
	t.Helper()
	select {
	case got := <-f.done:
		if got != want {
			t.Fatalf("joined %s, want %s", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("join of %s never finished", want)
	}
}

func TestJoinerWaitsOutRateLimits(t *testing.T) {
	f := newFakeJoins()
	f.limits["!a:x"] = 2
	j := startJoiner(t, f)
	j.Enqueue("!a:x")
	waitJoin(t, f, "!a:x")
	if f.calls["!a:x"] != 3 {
		t.Fatalf("calls = %d, want 3", f.calls["!a:x"])
	}
	if len(f.slept) != 2 || f.slept[0] != 250*time.Millisecond {
		t.Fatalf("slept %v, want two waits of 250ms", f.slept)
	}
}

func TestJoinerDoesNotRetryOtherErrors(t *testing.T) {
	f := newFakeJoins()
	f.fail["!bad:x"] = errors.New("forbidden")
	j := startJoiner(t, f)
	j.Enqueue("!bad:x")
	waitJoin(t, f, "!bad:x")
	j.Enqueue("!ok:x")
	waitJoin(t, f, "!ok:x")
	if f.calls["!bad:x"] != 1 || len(f.slept) != 0 {
		t.Fatalf("calls=%d slept=%v, want one attempt and no wait", f.calls["!bad:x"], f.slept)
	}
}

func TestJoinerDropsDuplicatesWhilePending(t *testing.T) {
	f := newFakeJoins()
	j := newJoiner(f.join, nil)
	j.sleep = f.sleep
	j.Enqueue("!a:x")
	j.Enqueue("!a:x")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go j.Run(ctx)
	waitJoin(t, f, "!a:x")
	j.Enqueue("!b:x")
	waitJoin(t, f, "!b:x")
	if f.calls["!a:x"] != 1 {
		t.Fatalf("calls = %d, want 1", f.calls["!a:x"])
	}
	j.Enqueue("!a:x") // finished rooms can be queued again
	waitJoin(t, f, "!a:x")
}

func TestPendingInvitesFromFullSync(t *testing.T) {
	raw := `{"next_batch":"s1","rooms":{"invite":{
	  "!bridged:messenger.local":{"invite_state":{"events":[
	    {"type":"m.room.name","state_key":"","sender":"@metabot:messenger.local","content":{"name":"Beat 803"}},
	    {"type":"m.room.member","state_key":"@jesus:messenger.local","sender":"@meta_42:messenger.local","content":{"membership":"invite"}}]}},
	  "!stranger:messenger.local":{"invite_state":{"events":[
	    {"type":"m.room.member","state_key":"@jesus:messenger.local","sender":"@eve:elsewhere.org","content":{"membership":"invite"}}]}},
	  "!other:messenger.local":{"invite_state":{"events":[
	    {"type":"m.room.member","state_key":"@someone:messenger.local","sender":"@metabot:messenger.local","content":{"membership":"invite"}}]}}
	}}}`
	var res mautrix.RespSync
	if err := json.Unmarshal([]byte(raw), &res); err != nil {
		t.Fatal(err)
	}
	got := pendingInvites(&res, "@jesus:messenger.local", "@metabot:messenger.local")
	if len(got) != 1 || got[0] != "!bridged:messenger.local" {
		t.Fatalf("got %v, want only the bridge invite", got)
	}
}
