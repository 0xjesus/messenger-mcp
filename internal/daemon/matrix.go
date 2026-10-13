package daemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"log/slog"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/0xjesus/messenger-mcp/internal/ingest"
	"github.com/0xjesus/messenger-mcp/internal/ledger"
	"github.com/0xjesus/messenger-mcp/internal/outbox"
)

// router dispatches Matrix events to the ledger and the send queue.
type router struct {
	ing      *ingest.Handler
	onStatus func(ingest.StatusUpdate)
	join     func(ctx context.Context, roomID id.RoomID, inviter id.UserID)
	log      *slog.Logger
}

func (r *router) logger() *slog.Logger {
	if r.log == nil {
		return slog.Default()
	}
	return r.log
}

func (r *router) handle(ctx context.Context, evt *event.Event) {
	var err error
	switch evt.Type.Type {
	case event.StateBridge.Type, event.StateHalfShotBridge.Type:
		if c, ok := evt.Content.Parsed.(*event.BridgeEventContent); ok {
			err = r.ing.HandleBridgeState(ctx, evt.RoomID, c, "")
		}
	case event.EventMessage.Type, event.EventSticker.Type:
		err = r.ing.HandleMessage(ctx, evt)
	case event.EventRedaction.Type:
		err = r.ing.HandleRedaction(ctx, evt)
	case event.BeeperMessageStatus.Type:
		if u, ok := r.ing.HandleStatus(evt); ok && r.onStatus != nil {
			r.onStatus(u)
		}
	case event.StateMember.Type:
		if r.join != nil && evt.StateKey != nil && id.UserID(*evt.StateKey) == r.ing.OperatorID &&
			evt.Content.AsMember().Membership == event.MembershipInvite {
			r.join(ctx, evt.RoomID, evt.Sender)
		}
	}
	if err != nil {
		r.logger().Error("ingest failed", "type", evt.Type.Type, "event_id", evt.ID, "error", err)
	}
}

// shouldAutoJoin accepts invites only from the bridge bot or its ghosts on our server.
func shouldAutoJoin(sender, bridgeBot id.UserID, server string) bool {
	if sender == bridgeBot {
		return true
	}
	return sender.Homeserver() == server && strings.HasPrefix(sender.Localpart(), "meta_")
}

// classify marks client errors (except rate limits) as permanent rejections.
func classify(err error) error {
	if err == nil {
		return nil
	}
	var he mautrix.HTTPError
	if errors.As(err, &he) && he.Response != nil {
		if c := he.Response.StatusCode; c >= 400 && c < 500 && c != http.StatusTooManyRequests {
			return outbox.Permanent(err)
		}
	}
	return err
}

// syncStore persists next_batch one sync late, so a crash re-processes the
// last batch (ingest is idempotent) instead of skipping it.
type syncStore struct {
	l       *ledger.Ledger
	mu      sync.Mutex
	pending string
}

func (s *syncStore) SaveFilterID(ctx context.Context, _ id.UserID, filterID string) error {
	return s.l.SetSync(ctx, "filter_id", filterID)
}
func (s *syncStore) LoadFilterID(ctx context.Context, _ id.UserID) (string, error) {
	return s.l.GetSync(ctx, "filter_id")
}
func (s *syncStore) SaveNextBatch(ctx context.Context, _ id.UserID, tok string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending != "" {
		if err := s.l.SetSync(ctx, "next_batch", s.pending); err != nil {
			return err
		}
	}
	s.pending = tok
	return nil
}
func (s *syncStore) LoadNextBatch(ctx context.Context, _ id.UserID) (string, error) {
	return s.l.GetSync(ctx, "next_batch")
}

// Matrix owns the operator's Matrix session.
type Matrix struct {
	cli      *mautrix.Client
	l        *ledger.Ledger
	password string
	r        *router
	log      *slog.Logger
	bot      id.UserID
	joins    *joiner

	mu       sync.Mutex
	names    map[string]string
	lastSync time.Time
	lastErr  string
}

func NewMatrix(cfg Config, l *ledger.Ledger, log *slog.Logger) (*Matrix, error) {
	cli, err := mautrix.NewClient(cfg.Homeserver, id.UserID(cfg.UserID), "")
	if err != nil {
		return nil, err
	}
	cli.Store = &syncStore{l: l}
	m := &Matrix{cli: cli, l: l, password: cfg.Password, log: log, bot: id.UserID(cfg.BridgeBot), names: map[string]string{}}
	m.joins = newJoiner(func(ctx context.Context, roomID id.RoomID) error {
		_, err := cli.JoinRoomByID(ctx, roomID)
		return err
	}, log)
	return m, nil
}

// inviteFilter keeps the full /sync used for invite reconciliation small.
const inviteFilter = `{"presence":{"not_types":["*"]},"account_data":{"not_types":["*"]},` +
	`"room":{"timeline":{"limit":1},"ephemeral":{"not_types":["*"]},"account_data":{"not_types":["*"]},` +
	`"state":{"lazy_load_members":true}}}`

// reconcileInvites queues joins for bridge invites that are still pending,
// e.g. because a join was rate limited before the last restart.
func (m *Matrix) reconcileInvites(ctx context.Context) {
	res, err := m.cli.FullSyncRequest(ctx, mautrix.ReqSync{FilterID: inviteFilter})
	if err != nil {
		m.log.Warn("invite reconciliation failed", "error", err)
		return
	}
	rooms := pendingInvites(res, m.cli.UserID, m.bot)
	if len(rooms) > 0 {
		m.log.Info("joining pending invites", "rooms", len(rooms))
	}
	for _, roomID := range rooms {
		m.joins.Enqueue(roomID)
	}
}

func (m *Matrix) login(ctx context.Context) error {
	tok, _ := m.l.GetSync(ctx, "access_token")
	dev, _ := m.l.GetSync(ctx, "device_id")
	if tok != "" {
		m.cli.AccessToken, m.cli.DeviceID = tok, id.DeviceID(dev)
		if _, err := m.cli.Whoami(ctx); err == nil {
			return nil
		} else if !errors.Is(err, mautrix.MUnknownToken) {
			return err
		}
	}
	resp, err := m.cli.Login(ctx, &mautrix.ReqLogin{
		Type:                     mautrix.AuthTypePassword,
		Identifier:               mautrix.UserIdentifier{Type: mautrix.IdentifierTypeUser, User: m.cli.UserID.Localpart()},
		Password:                 m.password,
		DeviceID:                 id.DeviceID(dev),
		InitialDeviceDisplayName: "messenger-mcp",
		StoreCredentials:         true,
	})
	if err != nil {
		return fmt.Errorf("matrix login: %w", err)
	}
	if err := m.l.SetSync(ctx, "access_token", resp.AccessToken); err != nil {
		return err
	}
	return m.l.SetSync(ctx, "device_id", string(resp.DeviceID))
}

func prepare(evt *event.Event, roomID id.RoomID) {
	evt.RoomID = roomID
	if evt.StateKey != nil {
		evt.Type.Class = event.StateEventType
	} else {
		evt.Type.Class = event.MessageEventType
	}
	if evt.Content.Parsed == nil {
		_ = evt.Content.ParseRaw(evt.Type)
	}
}

// backfill fetches events hidden by a limited timeline, oldest first.
func (m *Matrix) backfill(ctx context.Context, roomID id.RoomID, from string) {
	var events []*event.Event
	for page := 0; page < 20 && from != ""; page++ {
		resp, err := m.cli.Messages(ctx, roomID, from, "", mautrix.DirectionBackward, nil, 100)
		if err != nil {
			m.log.Warn("backfill failed", "room", roomID, "error", err)
			break
		}
		known := false
		for _, evt := range resp.Chunk {
			if evt.Type.Type == event.EventMessage.Type {
				if _, err := m.l.MessageByEventID(ctx, string(evt.ID)); err == nil {
					known = true
					break
				}
			}
			events = append(events, evt)
		}
		if known || len(resp.Chunk) == 0 || resp.End == "" {
			break
		}
		from = resp.End
	}
	for i := len(events) - 1; i >= 0; i-- {
		prepare(events[i], roomID)
		m.r.handle(ctx, events[i])
	}
}

func (m *Matrix) onSync(ctx context.Context, res *mautrix.RespSync, _ string) bool {
	for roomID, rd := range res.Rooms.Join {
		if !rd.Timeline.Limited || rd.Timeline.PrevBatch == "" {
			continue
		}
		state := append(append([]*event.Event{}, rd.State.Events...), rd.Timeline.Events...)
		if rd.StateAfter != nil {
			state = append(state, rd.StateAfter.Events...)
		}
		for _, evt := range state {
			if evt.StateKey != nil {
				prepare(evt, roomID)
				m.r.handle(ctx, evt)
			}
		}
		m.backfill(ctx, roomID, rd.Timeline.PrevBatch)
	}
	m.mu.Lock()
	m.lastSync, m.lastErr = time.Now(), ""
	m.mu.Unlock()
	return true
}

func (m *Matrix) setErr(err error) {
	m.mu.Lock()
	m.lastErr = err.Error()
	m.mu.Unlock()
}

// Run logs in and syncs until ctx is cancelled, re-logging in when needed.
func (m *Matrix) Run(ctx context.Context) {
	syncer := mautrix.NewDefaultSyncer()
	syncer.FilterJSON = &mautrix.Filter{Room: &mautrix.RoomFilter{Timeline: &mautrix.FilterPart{Limit: 100}}}
	syncer.OnSync(m.onSync)
	syncer.OnEvent(m.r.handle)
	m.cli.Syncer = syncer
	go m.joins.Run(ctx)
	for ctx.Err() == nil {
		if err := m.login(ctx); err != nil {
			m.setErr(err)
			m.log.Warn("matrix not ready", "error", err)
		} else if err := m.syncOnce(ctx); err != nil && ctx.Err() == nil {
			m.setErr(err)
			m.log.Warn("matrix sync stopped", "error", err)
			if errors.Is(err, mautrix.MUnknownToken) {
				_ = m.l.SetSync(ctx, "access_token", "")
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
	}
}

// syncOnce recovers missed invites, then syncs until the stream stops.
func (m *Matrix) syncOnce(ctx context.Context) error {
	m.reconcileInvites(ctx)
	return m.cli.SyncWithContext(ctx)
}

func (m *Matrix) Status() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]any{"state": "starting"}
	if !m.lastSync.IsZero() {
		out["last_sync"] = m.lastSync.Unix()
		out["state"] = "syncing"
		if time.Since(m.lastSync) > 2*time.Minute {
			out["state"] = "stale"
		}
	}
	if m.lastErr != "" {
		out["error"] = m.lastErr
	}
	return out
}

func (m *Matrix) Join(ctx context.Context, roomID id.RoomID, inviter id.UserID, bridgeBot id.UserID) {
	if !shouldAutoJoin(inviter, bridgeBot, m.cli.UserID.Homeserver()) {
		m.log.Warn("ignoring invite", "room", roomID, "inviter", inviter)
		return
	}
	m.joins.Enqueue(roomID)
}

// Name returns a member's display name, cached per room.
func (m *Matrix) Name(ctx context.Context, roomID id.RoomID, userID id.UserID) string {
	key := string(roomID) + "|" + string(userID)
	m.mu.Lock()
	n, ok := m.names[key]
	m.mu.Unlock()
	if ok {
		return n
	}
	var mem event.MemberEventContent
	if err := m.cli.StateEvent(ctx, roomID, event.StateMember, string(userID), &mem); err != nil {
		return ""
	}
	m.mu.Lock()
	m.names[key] = mem.Displayname
	m.mu.Unlock()
	return mem.Displayname
}

func (m *Matrix) SendText(ctx context.Context, roomID, txnID, text string) (string, error) {
	resp, err := m.cli.SendMessageEvent(ctx, id.RoomID(roomID), event.EventMessage,
		&event.MessageEventContent{MsgType: event.MsgText, Body: text}, mautrix.ReqSendEvent{TransactionID: txnID})
	if err != nil {
		return "", classify(err)
	}
	return string(resp.EventID), nil
}

var msgTypes = map[string]event.MessageType{"image": event.MsgImage, "video": event.MsgVideo, "audio": event.MsgAudio}

func (m *Matrix) SendFile(ctx context.Context, roomID, txnID, path, caption string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", outbox.Permanent(err)
	}
	name := filepath.Base(path)
	mimeType := mime.TypeByExtension(strings.ToLower(filepath.Ext(name)))
	if mimeType == "" {
		mimeType = http.DetectContentType(data)
	}
	up, err := m.cli.UploadBytesWithName(ctx, data, mimeType, name)
	if err != nil {
		return "", classify(err)
	}
	msgType, ok := msgTypes[strings.SplitN(mimeType, "/", 2)[0]]
	if !ok {
		msgType = event.MsgFile
	}
	content := &event.MessageEventContent{
		MsgType: msgType, Body: name, FileName: name, URL: up.ContentURI.CUString(),
		Info: &event.FileInfo{MimeType: mimeType, Size: len(data)},
	}
	if caption != "" {
		content.Body = caption
	}
	if msgType == event.MsgImage {
		if cfg, _, err := image.DecodeConfig(bytes.NewReader(data)); err == nil {
			content.Info.Width, content.Info.Height = cfg.Width, cfg.Height
		}
	}
	resp, err := m.cli.SendMessageEvent(ctx, id.RoomID(roomID), event.EventMessage, content, mautrix.ReqSendEvent{TransactionID: txnID})
	if err != nil {
		return "", classify(err)
	}
	return string(resp.EventID), nil
}

func (m *Matrix) Download(ctx context.Context, mxc string) ([]byte, string, error) {
	uri, err := id.ParseContentURI(mxc)
	if err != nil {
		return nil, "", err
	}
	data, err := m.cli.DownloadBytes(ctx, uri)
	if err != nil {
		return nil, "", err
	}
	return data, http.DetectContentType(data), nil
}

func (m *Matrix) MarkRead(ctx context.Context, roomID, eventID string) error {
	return m.cli.MarkRead(ctx, id.RoomID(roomID), id.EventID(eventID))
}
