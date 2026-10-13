package daemon

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

const (
	joinAttempts = 10
	maxJoinWait  = time.Minute
)

// joinRetryDelay reports whether err is a Synapse rate limit and how long it
// asked us to wait, capped at maxJoinWait.
func joinRetryDelay(err error) (time.Duration, bool) {
	var he mautrix.HTTPError
	if !errors.As(err, &he) || he.Response == nil || he.Response.StatusCode != http.StatusTooManyRequests {
		return 0, false
	}
	d := 5 * time.Second
	if he.RespError != nil {
		if ms, ok := he.RespError.ExtraData["retry_after_ms"].(float64); ok && ms > 0 {
			d = time.Duration(ms) * time.Millisecond
		}
	}
	return min(d, maxJoinWait), true
}

// joiner accepts bridge invites one at a time and waits out rate limits, so a
// burst of new portals after linking does not leave rooms unjoined.
type joiner struct {
	join  func(ctx context.Context, roomID id.RoomID) error
	sleep func(ctx context.Context, d time.Duration) bool
	log   *slog.Logger
	queue chan id.RoomID

	mu      sync.Mutex
	pending map[id.RoomID]bool
}

func newJoiner(join func(context.Context, id.RoomID) error, log *slog.Logger) *joiner {
	if log == nil {
		log = slog.Default()
	}
	return &joiner{join: join, sleep: sleepCtx, log: log, queue: make(chan id.RoomID, 4096), pending: map[id.RoomID]bool{}}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// Enqueue schedules a join unless the room is already waiting. A full queue
// drops the room; the next startup reconciliation picks it up again.
func (j *joiner) Enqueue(roomID id.RoomID) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.pending[roomID] {
		return
	}
	select {
	case j.queue <- roomID:
		j.pending[roomID] = true
	default:
		j.log.Warn("join queue full", "room", roomID)
	}
}

func (j *joiner) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case roomID := <-j.queue:
			j.joinOne(ctx, roomID)
			j.mu.Lock()
			delete(j.pending, roomID)
			j.mu.Unlock()
		}
	}
}

func (j *joiner) joinOne(ctx context.Context, roomID id.RoomID) {
	for attempt := 1; ; attempt++ {
		err := j.join(ctx, roomID)
		if err == nil {
			return
		}
		wait, limited := joinRetryDelay(err)
		if !limited || attempt == joinAttempts {
			j.log.Error("join failed", "room", roomID, "attempt", attempt, "error", err)
			return
		}
		if !j.sleep(ctx, wait) {
			return
		}
	}
}

// pendingInvites lists rooms where self holds an invite from the bridge in a
// full /sync. Incremental syncs report an invite only once, so a join that
// failed before a restart is only recoverable this way.
func pendingInvites(res *mautrix.RespSync, self, bridgeBot id.UserID) []id.RoomID {
	var out []id.RoomID
	for roomID, room := range res.Rooms.Invite {
		for _, evt := range room.State.Events {
			if evt.Type.Type != event.StateMember.Type || evt.StateKey == nil || id.UserID(*evt.StateKey) != self {
				continue
			}
			if m, _ := evt.Content.Raw["membership"].(string); m == string(event.MembershipInvite) &&
				shouldAutoJoin(evt.Sender, bridgeBot, self.Homeserver()) {
				out = append(out, roomID)
			}
			break
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a] < out[b] })
	return out
}
