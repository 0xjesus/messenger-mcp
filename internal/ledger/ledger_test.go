package ledger

import (
	"context"
	"path/filepath"
	"testing"
)

func openTest(t *testing.T) *Ledger {
	t.Helper()
	l, err := Open(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func mustChat(t *testing.T, l *Ledger, chatID, roomID, name string, dm bool) {
	t.Helper()
	if err := l.UpsertChat(context.Background(), Chat{ChatID: chatID, RoomID: roomID, Name: name, IsDM: dm}); err != nil {
		t.Fatalf("upsert chat: %v", err)
	}
}

func mustMsg(t *testing.T, l *Ledger, m Message) int64 {
	t.Helper()
	id, inserted, err := l.InsertMessage(context.Background(), m)
	if err != nil {
		t.Fatalf("insert %s: %v", m.EventID, err)
	}
	if !inserted {
		t.Fatalf("insert %s: expected new row", m.EventID)
	}
	return id
}

func TestInsertMessageIsIdempotentByEventID(t *testing.T) {
	l := openTest(t)
	mustChat(t, l, "245962885415945", "!forum:messenger.local", "Sala Forum", true)
	first := mustMsg(t, l, Message{EventID: "$a", ChatID: "245962885415945", TS: 1000, Text: "hola"})
	again, inserted, err := l.InsertMessage(context.Background(), Message{EventID: "$a", ChatID: "245962885415945", TS: 1000, Text: "hola"})
	if err != nil || inserted || again != first {
		t.Fatalf("duplicate insert: id=%d inserted=%v err=%v, want id=%d inserted=false", again, inserted, err, first)
	}
	second := mustMsg(t, l, Message{EventID: "$b", ChatID: "245962885415945", TS: 900, Text: "antes"})
	if second <= first {
		t.Fatalf("rowid must grow with ingest order: first=%d second=%d", first, second)
	}
}

func TestHistorySinceReturnsThreadsShapeInRowidOrder(t *testing.T) {
	l := openTest(t)
	ctx := context.Background()
	mustChat(t, l, "1", "!dm:messenger.local", "Beat 803", true)
	mustChat(t, l, "2", "!g:messenger.local", "Banda", false)
	a := mustMsg(t, l, Message{EventID: "$a", ChatID: "1", TS: 5_000, Text: "Le echo un vistazo", SenderName: "Beat 803"})
	mustMsg(t, l, Message{EventID: "$b", ChatID: "2", TS: 6_000, Own: true, Text: "ok"})
	mustMsg(t, l, Message{EventID: "$c", ChatID: "1", TS: 7_000, MediaType: "image", MediaName: "salon.jpg"})

	rows, next, err := l.HistorySince(ctx, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || next != rows[1].RowID {
		t.Fatalf("got %d rows next=%d", len(rows), next)
	}
	r := rows[0]
	if r.RowID != a || r.MessageID != "$a" || r.ChatID != "1" || r.Text != "Le echo un vistazo" || r.Date != 5 || r.Own || r.Label != "Beat 803" || !r.DM {
		t.Fatalf("unexpected first row: %+v", r)
	}
	if rows[1].DM || !rows[1].Own {
		t.Fatalf("group row should be dm=false own=true: %+v", rows[1])
	}
	rest, next2, err := l.HistorySince(ctx, next, 10)
	if err != nil || len(rest) != 1 || rest[0].Media != "image" || rest[0].Text != "" || next2 != rest[0].RowID {
		t.Fatalf("rest=%+v next=%d err=%v", rest, next2, err)
	}
	empty, next3, err := l.HistorySince(ctx, next2, 10)
	if err != nil || len(empty) != 0 || next3 != next2 {
		t.Fatalf("after end: rows=%d next=%d err=%v", len(empty), next3, err)
	}
}

func TestEditAndRedactUpdateTextAndSearch(t *testing.T) {
	l := openTest(t)
	ctx := context.Background()
	mustChat(t, l, "1", "!dm:messenger.local", "Sala Forum", true)
	mustMsg(t, l, Message{EventID: "$a", ChatID: "1", TS: 1000, Text: "precio del show"})
	if err := l.EditMessage(ctx, "$a", "propuesta de presentación"); err != nil {
		t.Fatal(err)
	}
	got, err := l.MessageByEventID(ctx, "$a")
	if err != nil || got.Text != "propuesta de presentación" || !got.Edited {
		t.Fatalf("edited: %+v err=%v", got, err)
	}
	hits, err := l.Search(ctx, "presentacion", "", 0, 0, 10)
	if err != nil || len(hits) != 1 {
		t.Fatalf("accent-insensitive search: hits=%d err=%v", len(hits), err)
	}
	if hits, _ := l.Search(ctx, "precio", "", 0, 0, 10); len(hits) != 0 {
		t.Fatalf("old text must leave the index, got %d hits", len(hits))
	}
	if err := l.RedactMessage(ctx, "$a"); err != nil {
		t.Fatal(err)
	}
	got, _ = l.MessageByEventID(ctx, "$a")
	if got.Text != "" || !got.Deleted {
		t.Fatalf("redacted: %+v", got)
	}
	if hits, _ := l.Search(ctx, "propuesta", "", 0, 0, 10); len(hits) != 0 {
		t.Fatalf("redacted text must leave the index")
	}
}

func TestSearchToleratesQuotesAndOperators(t *testing.T) {
	l := openTest(t)
	ctx := context.Background()
	mustChat(t, l, "1", "!dm:messenger.local", "Bar", true)
	mustMsg(t, l, Message{EventID: "$a", ChatID: "1", TS: 1000, Text: `dice "hola" AND adiós`})
	for _, q := range []string{`"hola`, `AND`, `hola OR`, `*`, `adiós (`} {
		if _, err := l.Search(ctx, q, "", 0, 0, 10); err != nil {
			t.Fatalf("query %q: %v", q, err)
		}
	}
	hits, err := l.Search(ctx, `"hola"`, "1", 0, 0, 10)
	if err != nil || len(hits) != 1 {
		t.Fatalf("quoted query: hits=%d err=%v", len(hits), err)
	}
}

func TestGetMessagesChronologicalWithPaging(t *testing.T) {
	l := openTest(t)
	ctx := context.Background()
	mustChat(t, l, "1", "!dm:messenger.local", "Bar", true)
	mustMsg(t, l, Message{EventID: "$late", ChatID: "1", TS: 3000, Text: "tres"})
	early := mustMsg(t, l, Message{EventID: "$early", ChatID: "1", TS: 1000, Text: "uno"})
	mid := mustMsg(t, l, Message{EventID: "$mid", ChatID: "1", TS: 2000, Text: "dos"})
	_ = early

	msgs, err := l.GetMessages(ctx, "1", 2, 0)
	if err != nil || len(msgs) != 2 || msgs[0].Text != "dos" || msgs[1].Text != "tres" {
		t.Fatalf("latest page: %+v err=%v", msgs, err)
	}
	older, err := l.GetMessages(ctx, "1", 5, mid)
	if err != nil || len(older) != 1 || older[0].Text != "uno" {
		t.Fatalf("before mid: %+v err=%v", older, err)
	}
}

func TestListChatsUnreadMeansIncomingAfterLastReplyOrRead(t *testing.T) {
	l := openTest(t)
	ctx := context.Background()
	mustChat(t, l, "1", "!a:messenger.local", "Sala Forum", true)
	mustChat(t, l, "2", "!b:messenger.local", "Salón Country", true)
	mustMsg(t, l, Message{EventID: "$1", ChatID: "1", TS: 1000, Text: "hola"})
	mustMsg(t, l, Message{EventID: "$2", ChatID: "1", TS: 2000, Own: true, Text: "propuesta"})
	mustMsg(t, l, Message{EventID: "$3", ChatID: "1", TS: 3000, Text: "va"})
	mustMsg(t, l, Message{EventID: "$4", ChatID: "2", TS: 2500, Text: "¿precio?"})

	chats, err := l.ListChats(ctx, "", false, 10)
	if err != nil || len(chats) != 2 {
		t.Fatalf("chats=%+v err=%v", chats, err)
	}
	if chats[0].ChatID != "1" || chats[0].Unread != 1 || chats[0].LastText != "va" {
		t.Fatalf("most recent first with 1 unread: %+v", chats[0])
	}
	if _, _, err := l.MarkRead(ctx, "1"); err != nil {
		t.Fatal(err)
	}
	unread, err := l.ListChats(ctx, "", true, 10)
	if err != nil || len(unread) != 1 || unread[0].ChatID != "2" {
		t.Fatalf("unread only: %+v err=%v", unread, err)
	}
	named, err := l.ListChats(ctx, "salon", false, 10)
	if err != nil || len(named) != 1 || named[0].ChatID != "2" {
		t.Fatalf("accent-insensitive name filter: %+v err=%v", named, err)
	}
}

func TestContextAndSyncState(t *testing.T) {
	l := openTest(t)
	ctx := context.Background()
	mustChat(t, l, "1", "!a:messenger.local", "Bar", true)
	for i, e := range []string{"$1", "$2", "$3", "$4", "$5"} {
		mustMsg(t, l, Message{EventID: e, ChatID: "1", TS: int64(1000 * (i + 1)), Text: e})
	}
	around, err := l.Context(ctx, "$3", 1, 1)
	if err != nil || len(around) != 3 || around[0].MessageID != "$2" || around[2].MessageID != "$4" {
		t.Fatalf("context: %+v err=%v", around, err)
	}
	if v, err := l.GetSync(ctx, "next_batch"); err != nil || v != "" {
		t.Fatalf("empty sync: %q %v", v, err)
	}
	if err := l.SetSync(ctx, "next_batch", "s123"); err != nil {
		t.Fatal(err)
	}
	if v, _ := l.GetSync(ctx, "next_batch"); v != "s123" {
		t.Fatalf("sync value %q", v)
	}
	if c, err := l.ChatByRoom(ctx, "!a:messenger.local"); err != nil || c.ChatID != "1" {
		t.Fatalf("chat by room: %+v %v", c, err)
	}
	st, err := l.Stats(ctx)
	if err != nil || st.Messages != 5 || st.Chats != 1 || st.LastTS != 5000 {
		t.Fatalf("stats %+v err=%v", st, err)
	}
}
