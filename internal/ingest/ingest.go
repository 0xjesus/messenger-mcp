// Package ingest turns Matrix events from bridge portal rooms into ledger rows.
package ingest

import (
	"context"
	"slices"

	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/0xjesus/messenger-mcp/internal/ledger"
)

type Handler struct {
	L          *ledger.Ledger
	OperatorID id.UserID
	BridgeBot  id.UserID
	// OwnGhosts returns the ghost users of the linked Messenger account, whose
	// messages were sent by Jesús from another device.
	OwnGhosts func() []id.UserID
	// Names resolves a sender's display name; empty falls back to the localpart.
	Names func(ctx context.Context, roomID id.RoomID, userID id.UserID) string
}

// StatusUpdate is the bridge's verdict on a message sent through Matrix.
type StatusUpdate struct {
	EventID id.EventID
	TxnID   string
	Status  event.MessageStatus
	Reason  string
}

// HandleBridgeState registers a portal room. State from other bridges is ignored.
func (h *Handler) HandleBridgeState(ctx context.Context, roomID id.RoomID, c *event.BridgeEventContent, roomName string) error {
	if c == nil || c.BridgeBot != h.BridgeBot || c.Channel.ID == "" {
		return nil
	}
	name := roomName
	if name == "" {
		name = c.Channel.DisplayName
	}
	return h.L.UpsertChat(ctx, ledger.Chat{
		ChatID: c.Channel.ID,
		RoomID: string(roomID),
		Name:   name,
		IsDM:   c.BeeperRoomType == "dm",
	})
}

func (h *Handler) own(sender id.UserID) bool {
	if sender == h.OperatorID {
		return true
	}
	return h.OwnGhosts != nil && slices.Contains(h.OwnGhosts(), sender)
}

func (h *Handler) senderName(ctx context.Context, roomID id.RoomID, sender id.UserID) string {
	if h.Names != nil {
		if n := h.Names(ctx, roomID, sender); n != "" {
			return n
		}
	}
	return sender.Localpart()
}

var mediaTypes = map[event.MessageType]string{
	event.MsgImage: "image",
	event.MsgVideo: "video",
	event.MsgAudio: "audio",
	event.MsgFile:  "file",
}

func (h *Handler) HandleMessage(ctx context.Context, evt *event.Event) error {
	chat, err := h.L.ChatByRoom(ctx, string(evt.RoomID))
	if err == ledger.ErrNotFound {
		return nil
	} else if err != nil {
		return err
	}
	if evt.Type == event.EventSticker {
		c := evt.Content.AsMessage()
		_, _, err := h.L.InsertMessage(ctx, ledger.Message{
			EventID: string(evt.ID), ChatID: chat.ChatID, SenderName: h.senderName(ctx, evt.RoomID, evt.Sender),
			Own: h.own(evt.Sender), TS: evt.Timestamp, MediaType: "sticker", MediaMXC: string(c.URL), MediaName: c.Body,
		})
		return err
	}
	c := evt.Content.AsMessage()
	if c.RelatesTo != nil && c.RelatesTo.Type == event.RelReplace && c.NewContent != nil {
		return h.L.EditMessage(ctx, string(c.RelatesTo.EventID), c.NewContent.Body)
	}
	m := ledger.Message{
		EventID:    string(evt.ID),
		ChatID:     chat.ChatID,
		SenderName: h.senderName(ctx, evt.RoomID, evt.Sender),
		Own:        h.own(evt.Sender),
		TS:         evt.Timestamp,
		Text:       c.Body,
	}
	if media, ok := mediaTypes[c.MsgType]; ok {
		m.MediaType = media
		m.MediaMXC = string(c.URL)
		if c.File != nil {
			m.MediaMXC = string(c.File.URL)
		}
		m.MediaName = c.FileName
		if c.FileName == "" || c.FileName == c.Body {
			m.MediaName, m.Text = c.Body, ""
		}
	}
	_, _, err = h.L.InsertMessage(ctx, m)
	return err
}

func (h *Handler) HandleRedaction(ctx context.Context, evt *event.Event) error {
	target := evt.Redacts
	if target == "" {
		target = evt.Content.AsRedaction().Redacts
	}
	if target == "" {
		return nil
	}
	return h.L.RedactMessage(ctx, string(target))
}

func (h *Handler) HandleStatus(evt *event.Event) (StatusUpdate, bool) {
	if evt.Sender != h.BridgeBot {
		return StatusUpdate{}, false
	}
	c, ok := evt.Content.Parsed.(*event.BeeperMessageStatusEventContent)
	if !ok || c == nil {
		return StatusUpdate{}, false
	}
	reason := c.Message
	if reason == "" {
		reason = string(c.Reason)
	}
	return StatusUpdate{EventID: c.RelatesTo.EventID, TxnID: c.TargetTxnID, Status: c.Status, Reason: reason}, true
}
