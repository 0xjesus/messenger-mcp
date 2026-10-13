// Package ledger stores Messenger chats and messages mirrored from Matrix.
//
// Message ids grow with ingest order and are never reused, so consumers such
// as Threads0x can follow the log with a single cursor.
package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
	_ "modernc.org/sqlite"
)

var ErrNotFound = errors.New("not found")

type Chat struct {
	ChatID string `json:"chat_id"`
	RoomID string `json:"-"`
	Name   string `json:"name"`
	IsDM   bool   `json:"is_dm"`
}

type Message struct {
	EventID    string
	ChatID     string
	SenderName string
	Own        bool
	TS         int64 // milliseconds
	Text       string
	MediaType  string
	MediaMXC   string
	MediaName  string
}

type StoredMessage struct {
	RowID     int64  `json:"rowid"`
	MessageID string `json:"message_id"`
	ChatID    string `json:"chat_id"`
	Sender    string `json:"sender"`
	Own       bool   `json:"own"`
	Date      int64  `json:"date"`
	Text      string `json:"text"`
	Media     string `json:"media,omitempty"`
	MediaName string `json:"media_name,omitempty"`
	MediaMXC  string `json:"-"`
	Edited    bool   `json:"edited,omitempty"`
	Deleted   bool   `json:"deleted,omitempty"`
	TS        int64  `json:"-"`
}

type HistoryRow struct {
	RowID     int64  `json:"rowid"`
	MessageID string `json:"message_id"`
	ChatID    string `json:"chat_id"`
	Text      string `json:"text"`
	Date      int64  `json:"date"`
	Own       bool   `json:"own"`
	Media     string `json:"media"`
	Label     string `json:"label"`
	DM        bool   `json:"dm"`
}

type ChatSummary struct {
	ChatID   string `json:"chat_id"`
	Name     string `json:"name"`
	IsDM     bool   `json:"is_dm"`
	LastText string `json:"last_message"`
	LastOwn  bool   `json:"last_own"`
	LastDate int64  `json:"last_date"`
	Unread   int    `json:"unread"`
}

type Stats struct {
	Chats    int   `json:"chats"`
	Messages int   `json:"messages"`
	LastTS   int64 `json:"last_ts"`
}

type Ledger struct {
	db *sql.DB
}

const schema = `
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS chats(
	chat_id TEXT PRIMARY KEY,
	room_id TEXT UNIQUE NOT NULL,
	name TEXT NOT NULL DEFAULT '',
	name_norm TEXT NOT NULL DEFAULT '',
	is_dm INTEGER NOT NULL DEFAULT 0,
	read_upto_ts INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS messages(
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	event_id TEXT UNIQUE NOT NULL,
	chat_id TEXT NOT NULL,
	sender_name TEXT NOT NULL DEFAULT '',
	own INTEGER NOT NULL DEFAULT 0,
	ts INTEGER NOT NULL,
	text TEXT NOT NULL DEFAULT '',
	media_type TEXT NOT NULL DEFAULT '',
	media_mxc TEXT NOT NULL DEFAULT '',
	media_name TEXT NOT NULL DEFAULT '',
	edited INTEGER NOT NULL DEFAULT 0,
	deleted INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS messages_chat_ts ON messages(chat_id, ts, id);
CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
	text, content='messages', content_rowid='id', tokenize='unicode61 remove_diacritics 2'
);
CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN
	INSERT INTO messages_fts(rowid, text) VALUES (new.id, new.text);
END;
CREATE TRIGGER IF NOT EXISTS messages_au AFTER UPDATE OF text ON messages BEGIN
	INSERT INTO messages_fts(messages_fts, rowid, text) VALUES ('delete', old.id, old.text);
	INSERT INTO messages_fts(rowid, text) VALUES (new.id, new.text);
END;
CREATE TABLE IF NOT EXISTS sync_state(key TEXT PRIMARY KEY, value TEXT NOT NULL);
`

func Open(path string) (*Ledger, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("ledger schema: %w", err)
	}
	return &Ledger{db: db}, nil
}

func (l *Ledger) Close() error { return l.db.Close() }

// DB exposes the connection so sibling packages (the outbox) share one file.
func (l *Ledger) DB() *sql.DB { return l.db }

var foldTransformer = transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)

// Fold lowercases and strips diacritics for accent-insensitive matching.
func Fold(s string) string {
	out, _, err := transform.String(foldTransformer, s)
	if err != nil {
		out = s
	}
	return strings.ToLower(out)
}

func (l *Ledger) UpsertChat(ctx context.Context, c Chat) error {
	if c.ChatID == "" || c.RoomID == "" {
		return errors.New("chat_id and room_id are required")
	}
	_, err := l.db.ExecContext(ctx, `INSERT INTO chats(chat_id, room_id, name, name_norm, is_dm) VALUES (?,?,?,?,?)
		ON CONFLICT(chat_id) DO UPDATE SET room_id=excluded.room_id,
			name=CASE WHEN excluded.name<>'' THEN excluded.name ELSE chats.name END,
			name_norm=CASE WHEN excluded.name<>'' THEN excluded.name_norm ELSE chats.name_norm END,
			is_dm=excluded.is_dm`,
		c.ChatID, c.RoomID, c.Name, Fold(c.Name), c.IsDM)
	return err
}

func (l *Ledger) chatWhere(ctx context.Context, where string, arg any) (Chat, error) {
	var c Chat
	err := l.db.QueryRowContext(ctx, `SELECT chat_id, room_id, name, is_dm FROM chats WHERE `+where, arg).
		Scan(&c.ChatID, &c.RoomID, &c.Name, &c.IsDM)
	if errors.Is(err, sql.ErrNoRows) {
		return c, ErrNotFound
	}
	return c, err
}

func (l *Ledger) ChatByID(ctx context.Context, chatID string) (Chat, error) {
	return l.chatWhere(ctx, "chat_id=?", chatID)
}

func (l *Ledger) ChatByRoom(ctx context.Context, roomID string) (Chat, error) {
	return l.chatWhere(ctx, "room_id=?", roomID)
}

func (l *Ledger) InsertMessage(ctx context.Context, m Message) (int64, bool, error) {
	if m.EventID == "" || m.ChatID == "" {
		return 0, false, errors.New("event_id and chat_id are required")
	}
	res, err := l.db.ExecContext(ctx, `INSERT INTO messages(event_id, chat_id, sender_name, own, ts, text, media_type, media_mxc, media_name)
		VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(event_id) DO NOTHING`,
		m.EventID, m.ChatID, m.SenderName, m.Own, m.TS, m.Text, m.MediaType, m.MediaMXC, m.MediaName)
	if err != nil {
		return 0, false, err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		id, err := res.LastInsertId()
		return id, true, err
	}
	var id int64
	err = l.db.QueryRowContext(ctx, `SELECT id FROM messages WHERE event_id=?`, m.EventID).Scan(&id)
	return id, false, err
}

func (l *Ledger) EditMessage(ctx context.Context, targetEventID, newText string) error {
	_, err := l.db.ExecContext(ctx, `UPDATE messages SET text=?, edited=1 WHERE event_id=? AND deleted=0`, newText, targetEventID)
	return err
}

func (l *Ledger) RedactMessage(ctx context.Context, targetEventID string) error {
	_, err := l.db.ExecContext(ctx, `UPDATE messages SET text='', media_type='', media_mxc='', media_name='', deleted=1 WHERE event_id=?`, targetEventID)
	return err
}

const msgColumns = `m.id, m.event_id, m.chat_id, m.sender_name, m.own, m.ts, m.text, m.media_type, m.media_name, m.media_mxc, m.edited, m.deleted`

func scanMessages(rows *sql.Rows) ([]StoredMessage, error) {
	defer rows.Close()
	out := []StoredMessage{}
	for rows.Next() {
		var s StoredMessage
		if err := rows.Scan(&s.RowID, &s.MessageID, &s.ChatID, &s.Sender, &s.Own, &s.TS, &s.Text, &s.Media, &s.MediaName, &s.MediaMXC, &s.Edited, &s.Deleted); err != nil {
			return nil, err
		}
		s.Date = s.TS / 1000
		out = append(out, s)
	}
	return out, rows.Err()
}

func (l *Ledger) MessageByEventID(ctx context.Context, eventID string) (StoredMessage, error) {
	rows, err := l.db.QueryContext(ctx, `SELECT `+msgColumns+` FROM messages m WHERE m.event_id=?`, eventID)
	if err != nil {
		return StoredMessage{}, err
	}
	msgs, err := scanMessages(rows)
	if err != nil {
		return StoredMessage{}, err
	}
	if len(msgs) == 0 {
		return StoredMessage{}, ErrNotFound
	}
	return msgs[0], nil
}

func reverse(msgs []StoredMessage) {
	for i, j := 0, len(msgs)-1; i < j; i, j = i+1, j-1 {
		msgs[i], msgs[j] = msgs[j], msgs[i]
	}
}

// GetMessages returns the newest `limit` messages of a chat, oldest first.
// beforeRowID pages to messages that sort before that message.
func (l *Ledger) GetMessages(ctx context.Context, chatID string, limit int, beforeRowID int64) ([]StoredMessage, error) {
	q := `SELECT ` + msgColumns + ` FROM messages m WHERE m.chat_id=?`
	args := []any{chatID}
	if beforeRowID > 0 {
		q += ` AND (m.ts, m.id) < (SELECT ts, id FROM messages WHERE id=?)`
		args = append(args, beforeRowID)
	}
	q += ` ORDER BY m.ts DESC, m.id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := l.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	msgs, err := scanMessages(rows)
	reverse(msgs)
	return msgs, err
}

// ftsQuery turns free text into an FTS5 query of quoted terms (implicit AND),
// so user input can never be parsed as FTS5 syntax.
func ftsQuery(q string) string {
	var terms []string
	for _, f := range strings.FieldsFunc(q, func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsNumber(r) }) {
		terms = append(terms, `"`+strings.ReplaceAll(f, `"`, `""`)+`"`)
	}
	return strings.Join(terms, " ")
}

func (l *Ledger) Search(ctx context.Context, query, chatID string, after, before int64, limit int) ([]StoredMessage, error) {
	fq := ftsQuery(query)
	if fq == "" {
		return []StoredMessage{}, nil
	}
	q := `SELECT ` + msgColumns + ` FROM messages_fts f JOIN messages m ON m.id=f.rowid WHERE messages_fts MATCH ? AND m.deleted=0`
	args := []any{fq}
	if chatID != "" {
		q += ` AND m.chat_id=?`
		args = append(args, chatID)
	}
	if after > 0 {
		q += ` AND m.ts>=?`
		args = append(args, after*1000)
	}
	if before > 0 {
		q += ` AND m.ts<=?`
		args = append(args, before*1000)
	}
	q += ` ORDER BY m.ts DESC, m.id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := l.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanMessages(rows)
}

func (l *Ledger) Context(ctx context.Context, eventID string, before, after int) ([]StoredMessage, error) {
	target, err := l.MessageByEventID(ctx, eventID)
	if err != nil {
		return nil, err
	}
	rows, err := l.db.QueryContext(ctx, `SELECT `+msgColumns+` FROM messages m WHERE m.chat_id=? AND (m.ts, m.id) < (?, ?) ORDER BY m.ts DESC, m.id DESC LIMIT ?`,
		target.ChatID, target.TS, target.RowID, before)
	if err != nil {
		return nil, err
	}
	prev, err := scanMessages(rows)
	if err != nil {
		return nil, err
	}
	reverse(prev)
	rows, err = l.db.QueryContext(ctx, `SELECT `+msgColumns+` FROM messages m WHERE m.chat_id=? AND (m.ts, m.id) > (?, ?) ORDER BY m.ts, m.id LIMIT ?`,
		target.ChatID, target.TS, target.RowID, after)
	if err != nil {
		return nil, err
	}
	next, err := scanMessages(rows)
	if err != nil {
		return nil, err
	}
	out := append(prev, target)
	return append(out, next...), nil
}

// HistorySince returns messages in ingest order after a cursor, in the shape
// Threads0x's follow-up reader expects. next is the cursor to resume from.
func (l *Ledger) HistorySince(ctx context.Context, afterRowID int64, limit int) ([]HistoryRow, int64, error) {
	rows, err := l.db.QueryContext(ctx, `SELECT m.id, m.event_id, m.chat_id, m.text, m.ts, m.own, m.media_type,
			COALESCE(NULLIF(c.name,''), m.chat_id), COALESCE(c.is_dm, 0)
		FROM messages m LEFT JOIN chats c ON c.chat_id=m.chat_id
		WHERE m.id>? ORDER BY m.id LIMIT ?`, afterRowID, limit)
	if err != nil {
		return nil, afterRowID, err
	}
	defer rows.Close()
	out := []HistoryRow{}
	next := afterRowID
	for rows.Next() {
		var r HistoryRow
		var ts int64
		if err := rows.Scan(&r.RowID, &r.MessageID, &r.ChatID, &r.Text, &ts, &r.Own, &r.Media, &r.Label, &r.DM); err != nil {
			return nil, afterRowID, err
		}
		r.Date = ts / 1000
		next = r.RowID
		out = append(out, r)
	}
	return out, next, rows.Err()
}

// ListChats orders chats by latest message. Unread counts incoming messages
// newer than both the last own message and the last mark_read.
func (l *Ledger) ListChats(ctx context.Context, query string, unreadOnly bool, limit int) ([]ChatSummary, error) {
	q := `SELECT * FROM (SELECT c.chat_id, c.name, c.is_dm,
			COALESCE(m.text, ''), COALESCE(m.own, 0), COALESCE(m.ts, 0),
			(SELECT COUNT(*) FROM messages x WHERE x.chat_id=c.chat_id AND x.own=0 AND x.deleted=0
				AND x.ts > MAX(c.read_upto_ts, COALESCE((SELECT MAX(y.ts) FROM messages y WHERE y.chat_id=c.chat_id AND y.own=1), 0))) AS unread
		FROM chats c
		LEFT JOIN messages m ON m.id=(SELECT z.id FROM messages z WHERE z.chat_id=c.chat_id AND z.deleted=0 ORDER BY z.ts DESC, z.id DESC LIMIT 1)
		WHERE c.name_norm LIKE ?) WHERE (? = 0 OR unread > 0)
		ORDER BY 6 DESC LIMIT ?`
	rows, err := l.db.QueryContext(ctx, q, "%"+Fold(query)+"%", unreadOnly, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ChatSummary{}
	for rows.Next() {
		var s ChatSummary
		var ts int64
		if err := rows.Scan(&s.ChatID, &s.Name, &s.IsDM, &s.LastText, &s.LastOwn, &ts, &s.Unread); err != nil {
			return nil, err
		}
		s.LastDate = ts / 1000
		out = append(out, s)
	}
	return out, rows.Err()
}

// MarkRead records a read point and returns the newest message to acknowledge.
func (l *Ledger) MarkRead(ctx context.Context, chatID string) (string, string, error) {
	c, err := l.ChatByID(ctx, chatID)
	if err != nil {
		return "", "", err
	}
	var eventID string
	var ts int64
	err = l.db.QueryRowContext(ctx, `SELECT event_id, ts FROM messages WHERE chat_id=? ORDER BY ts DESC, id DESC LIMIT 1`, chatID).Scan(&eventID, &ts)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", "", err
	}
	upto := max(ts, time.Now().UnixMilli())
	if _, err := l.db.ExecContext(ctx, `UPDATE chats SET read_upto_ts=? WHERE chat_id=?`, upto, chatID); err != nil {
		return "", "", err
	}
	return eventID, c.RoomID, nil
}

func (l *Ledger) GetSync(ctx context.Context, key string) (string, error) {
	var v string
	err := l.db.QueryRowContext(ctx, `SELECT value FROM sync_state WHERE key=?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}

func (l *Ledger) SetSync(ctx context.Context, key, value string) error {
	_, err := l.db.ExecContext(ctx, `INSERT INTO sync_state(key, value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
	return err
}

func (l *Ledger) Stats(ctx context.Context) (Stats, error) {
	var s Stats
	err := l.db.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM chats), COUNT(*), COALESCE(MAX(ts), 0) FROM messages`).
		Scan(&s.Chats, &s.Messages, &s.LastTS)
	return s, err
}
