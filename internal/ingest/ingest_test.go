package ingest

import (
	"context"
	"path/filepath"
	"testing"

	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/0xjesus/messenger-mcp/internal/ledger"
)

const (
	operator = id.UserID("@jesus:messenger.local")
	bot      = id.UserID("@metabot:messenger.local")
	ownGhost = id.UserID("@meta_100:messenger.local")
	venue    = id.UserID("@meta_200:messenger.local")
	room     = id.RoomID("!forum:messenger.local")
)

func newHandler(t *testing.T) (*Handler, *ledger.Ledger) {
	t.Helper()
	l, err := ledger.Open(filepath.Join(t.TempDir(), "l.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	h := &Handler{
		L: l, OperatorID: operator, BridgeBot: bot,
		OwnGhosts: func() []id.UserID { return []id.UserID{ownGhost} },
		Names: func(_ context.Context, _ id.RoomID, u id.UserID) string {
			if u == venue {
				return "Sala Forum"
			}
			return ""
		},
	}
	return h, l
}

func bridgeState(dm bool) *event.BridgeEventContent {
	c := &event.BridgeEventContent{
		BridgeBot: bot,
		Channel:   event.BridgeInfoSection{ID: "245962885415945", DisplayName: "Sala Forum"},
	}
	if dm {
		c.BeeperRoomType = "dm"
	}
	return c
}

func msgEvent(evtID string, sender id.UserID, ts int64, c *event.MessageEventContent) *event.Event {
	return &event.Event{
		ID: id.EventID(evtID), RoomID: room, Sender: sender, Timestamp: ts,
		Type: event.EventMessage, Content: event.Content{Parsed: c},
	}
}

func TestBridgeStateCreatesChat(t *testing.T) {
	h, l := newHandler(t)
	ctx := context.Background()
	if err := h.HandleBridgeState(ctx, room, bridgeState(true), ""); err != nil {
		t.Fatal(err)
	}
	c, err := l.ChatByRoom(ctx, string(room))
	if err != nil || c.ChatID != "245962885415945" || c.Name != "Sala Forum" || !c.IsDM {
		t.Fatalf("chat %+v err=%v", c, err)
	}
	foreign := bridgeState(false)
	foreign.BridgeBot = "@otherbot:messenger.local"
	if err := h.HandleBridgeState(ctx, "!x:messenger.local", foreign, "x"); err != nil {
		t.Fatal(err)
	}
	if _, err := l.ChatByRoom(ctx, "!x:messenger.local"); err != ledger.ErrNotFound {
		t.Fatalf("state from another bridge must be ignored, err=%v", err)
	}
}

func TestMessagesTextMediaOwnEditRedact(t *testing.T) {
	h, l := newHandler(t)
	ctx := context.Background()
	if err := h.HandleBridgeState(ctx, room, bridgeState(true), "Sala Forum"); err != nil {
		t.Fatal(err)
	}
	steps := []*event.Event{
		msgEvent("$t", venue, 1000, &event.MessageEventContent{MsgType: event.MsgText, Body: "Le echo un vistazo"}),
		msgEvent("$own", ownGhost, 2000, &event.MessageEventContent{MsgType: event.MsgText, Body: "Gracias!"}),
		msgEvent("$op", operator, 2500, &event.MessageEventContent{MsgType: event.MsgText, Body: "desde el MCP"}),
		msgEvent("$img", venue, 3000, &event.MessageEventContent{MsgType: event.MsgImage, Body: "salon.jpg", URL: "mxc://messenger.local/abc"}),
		msgEvent("$cap", venue, 3500, &event.MessageEventContent{MsgType: event.MsgFile, Body: "aquí van las medidas", FileName: "medidas.pdf", URL: "mxc://messenger.local/pdf"}),
	}
	for _, e := range steps {
		if err := h.HandleMessage(ctx, e); err != nil {
			t.Fatalf("%s: %v", e.ID, err)
		}
	}
	edit := msgEvent("$edit", venue, 4000, &event.MessageEventContent{
		MsgType: event.MsgText, Body: "* Le echo un vistazo hoy",
		NewContent: &event.MessageEventContent{MsgType: event.MsgText, Body: "Le echo un vistazo hoy"},
		RelatesTo:  &event.RelatesTo{Type: event.RelReplace, EventID: "$t"},
	})
	if err := h.HandleMessage(ctx, edit); err != nil {
		t.Fatal(err)
	}
	if err := h.HandleRedaction(ctx, &event.Event{ID: "$r", RoomID: room, Type: event.EventRedaction, Redacts: "$img"}); err != nil {
		t.Fatal(err)
	}

	msgs, err := l.GetMessages(ctx, "245962885415945", 10, 0)
	if err != nil || len(msgs) != 5 {
		t.Fatalf("expected 5 messages (edit is not a new row), got %d err=%v", len(msgs), err)
	}
	if m := msgs[0]; m.Text != "Le echo un vistazo hoy" || !m.Edited || m.Own || m.Sender != "Sala Forum" {
		t.Fatalf("edited venue message: %+v", m)
	}
	if !msgs[1].Own || !msgs[2].Own {
		t.Fatalf("ghost of linked account and operator are own: %+v %+v", msgs[1], msgs[2])
	}
	if m := msgs[3]; !m.Deleted || m.Media != "" {
		t.Fatalf("redacted image: %+v", m)
	}
	if m := msgs[4]; m.Media != "file" || m.MediaName != "medidas.pdf" || m.Text != "aquí van las medidas" || m.MediaMXC != "mxc://messenger.local/pdf" {
		t.Fatalf("file with caption: %+v", m)
	}
}

func TestMessageInUnknownRoomIsIgnored(t *testing.T) {
	h, l := newHandler(t)
	ctx := context.Background()
	e := msgEvent("$x", venue, 1000, &event.MessageEventContent{MsgType: event.MsgText, Body: "hola"})
	e.RoomID = "!management:messenger.local"
	if err := h.HandleMessage(ctx, e); err != nil {
		t.Fatal(err)
	}
	if st, _ := l.Stats(ctx); st.Messages != 0 {
		t.Fatalf("message outside a portal must not be stored")
	}
}

func TestStatusEventTranslation(t *testing.T) {
	h, _ := newHandler(t)
	e := &event.Event{
		ID: "$s", RoomID: room, Sender: bot, Type: event.BeeperMessageStatus,
		Content: event.Content{Parsed: &event.BeeperMessageStatusEventContent{
			RelatesTo:   event.RelatesTo{Type: event.RelReference, EventID: "$sent"},
			Status:      event.MessageStatusFail,
			Reason:      event.MessageStatusGenericError,
			Message:     "blocked",
			TargetTxnID: "job-1",
		}},
	}
	u, ok := h.HandleStatus(e)
	if !ok || u.EventID != "$sent" || u.TxnID != "job-1" || u.Status != event.MessageStatusFail || u.Reason != "blocked" {
		t.Fatalf("status %+v ok=%v", u, ok)
	}
	e.Sender = venue
	if _, ok := h.HandleStatus(e); ok {
		t.Fatalf("status from non-bridge sender must be ignored")
	}
}
