// Package outbox is the durable, paced send queue. A job is only "sent" once
// the bridge confirms delivery; uncertain outcomes become "needs_review" and
// are never resent automatically.
package outbox

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"maunium.net/go/mautrix/event"

	"github.com/0xjesus/messenger-mcp/internal/ingest"
	"github.com/0xjesus/messenger-mcp/internal/ledger"
)

const (
	KindText = "text"
	KindFile = "file"

	StatusQueued      = "queued"
	StatusSending     = "sending"
	StatusSent        = "sent"
	StatusFailed      = "failed"
	StatusNeedsReview = "needs_review"
	StatusCancelled   = "cancelled"

	sendAttempts = 3
)

var ErrIdempotencyConflict = errors.New("idempotency_key already used with different content")

// Sender delivers a message into a Matrix room. The txnID must make retries
// idempotent. Wrap definite rejections with Permanent.
type Sender interface {
	SendText(ctx context.Context, roomID, txnID, text string) (string, error)
	SendFile(ctx context.Context, roomID, txnID, path, caption string) (string, error)
}

type permanentError struct{ err error }

func (p permanentError) Error() string { return p.err.Error() }
func (p permanentError) Unwrap() error { return p.err }

// Permanent marks an error as a definite rejection (nothing was sent).
func Permanent(err error) error { return permanentError{err} }

type Config struct {
	MinGap        time.Duration
	StatusTimeout time.Duration
	DailyMax      int
	UploadsDir    string
	MaxFileBytes  int64
	Clock         func() time.Time
}

type Job struct {
	ChatID         string
	Kind           string
	Text           string
	FilePath       string
	IdempotencyKey string
}

type JobView struct {
	JobID     string `json:"job_id"`
	ChatID    string `json:"chat_id"`
	Kind      string `json:"kind"`
	Status    string `json:"status"`
	MessageID string `json:"message_id,omitempty"`
	Error     string `json:"error,omitempty"`
	Attempts  int    `json:"attempts"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

type Queue struct {
	l    *ledger.Ledger
	db   *sql.DB
	s    Sender
	cfg  Config
	mu   sync.Mutex // serialises Step so only one send is in flight
	last time.Time
}

const schema = `CREATE TABLE IF NOT EXISTS outbox(
	job_id TEXT PRIMARY KEY,
	idempotency_key TEXT UNIQUE NOT NULL,
	chat_id TEXT NOT NULL,
	kind TEXT NOT NULL,
	text TEXT NOT NULL DEFAULT '',
	file_path TEXT NOT NULL DEFAULT '',
	status TEXT NOT NULL,
	attempts INTEGER NOT NULL DEFAULT 0,
	event_id TEXT NOT NULL DEFAULT '',
	error TEXT NOT NULL DEFAULT '',
	created_at INTEGER NOT NULL,
	updated_at INTEGER NOT NULL,
	sent_at INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS outbox_status ON outbox(status, created_at);
CREATE INDEX IF NOT EXISTS outbox_event ON outbox(event_id);`

func New(l *ledger.Ledger, s Sender, cfg Config) (*Queue, error) {
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if _, err := l.DB().Exec(schema); err != nil {
		return nil, fmt.Errorf("outbox schema: %w", err)
	}
	q := &Queue{l: l, db: l.DB(), s: s, cfg: cfg}
	var lastSent int64
	if err := q.db.QueryRow(`SELECT COALESCE(MAX(sent_at), 0) FROM outbox`).Scan(&lastSent); err != nil {
		return nil, err
	}
	if lastSent > 0 {
		q.last = time.UnixMilli(lastSent)
	}
	return q, nil
}

func newJobID() string {
	b := make([]byte, 9)
	_, _ = rand.Read(b)
	return "msg-" + hex.EncodeToString(b)
}

// ValidateUpload returns the cleaned path if it is a regular file inside the
// uploads directory, not reached through a symlink, and within the size cap.
func ValidateUpload(uploadsDir, path string, maxBytes int64) (string, error) {
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("path must be absolute and inside %s", uploadsDir)
	}
	root, err := filepath.EvalSymlinks(uploadsDir)
	if err != nil {
		return "", fmt.Errorf("uploads dir: %w", err)
	}
	clean := filepath.Clean(path)
	info, err := os.Lstat(clean)
	if err != nil {
		return "", fmt.Errorf("file not found: %s", clean)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("not a regular file (symlinks and directories are refused): %s", clean)
	}
	real, err := filepath.EvalSymlinks(clean)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(root, real)
	if err != nil || rel == "." || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
		return "", fmt.Errorf("file must be inside %s", uploadsDir)
	}
	if info.Size() > maxBytes {
		return "", fmt.Errorf("file is %d bytes; limit is %d", info.Size(), maxBytes)
	}
	return real, nil
}

func (q *Queue) Enqueue(ctx context.Context, j Job) (JobView, error) {
	if _, err := q.l.ChatByID(ctx, j.ChatID); err != nil {
		return JobView{}, fmt.Errorf("unknown chat_id %q", j.ChatID)
	}
	switch j.Kind {
	case KindText:
		if strings.TrimSpace(j.Text) == "" {
			return JobView{}, errors.New("text is empty")
		}
		j.FilePath = ""
	case KindFile:
		p, err := ValidateUpload(q.cfg.UploadsDir, j.FilePath, q.cfg.MaxFileBytes)
		if err != nil {
			return JobView{}, err
		}
		j.FilePath = p
	default:
		return JobView{}, fmt.Errorf("unknown kind %q", j.Kind)
	}
	jobID := newJobID()
	key := j.IdempotencyKey
	if key == "" {
		key = jobID
	}
	now := q.cfg.Clock().UnixMilli()
	res, err := q.db.ExecContext(ctx, `INSERT INTO outbox(job_id, idempotency_key, chat_id, kind, text, file_path, status, created_at, updated_at)
		VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(idempotency_key) DO NOTHING`,
		jobID, key, j.ChatID, j.Kind, j.Text, j.FilePath, StatusQueued, now, now)
	if err != nil {
		return JobView{}, err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		return q.Get(ctx, jobID)
	}
	var existingID, chat, kind, text, file string
	err = q.db.QueryRowContext(ctx, `SELECT job_id, chat_id, kind, text, file_path FROM outbox WHERE idempotency_key=?`, key).
		Scan(&existingID, &chat, &kind, &text, &file)
	if err != nil {
		return JobView{}, err
	}
	if chat != j.ChatID || kind != j.Kind || text != j.Text || file != j.FilePath {
		return JobView{}, ErrIdempotencyConflict
	}
	return q.Get(ctx, existingID)
}

const viewColumns = `job_id, chat_id, kind, status, event_id, error, attempts, created_at, updated_at`

func scanView(row interface{ Scan(...any) error }) (JobView, error) {
	var v JobView
	var eventID string
	err := row.Scan(&v.JobID, &v.ChatID, &v.Kind, &v.Status, &eventID, &v.Error, &v.Attempts, &v.CreatedAt, &v.UpdatedAt)
	if v.Status == StatusSent {
		v.MessageID = eventID
	}
	return v, err
}

func (q *Queue) Get(ctx context.Context, jobID string) (JobView, error) {
	v, err := scanView(q.db.QueryRowContext(ctx, `SELECT `+viewColumns+` FROM outbox WHERE job_id=?`, jobID))
	if errors.Is(err, sql.ErrNoRows) {
		return v, ledger.ErrNotFound
	}
	return v, err
}

func (q *Queue) List(ctx context.Context, limit int) ([]JobView, error) {
	rows, err := q.db.QueryContext(ctx, `SELECT `+viewColumns+` FROM outbox ORDER BY created_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []JobView{}
	for rows.Next() {
		v, err := scanView(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (q *Queue) Counts(ctx context.Context) (map[string]int, error) {
	rows, err := q.db.QueryContext(ctx, `SELECT status, COUNT(*) FROM outbox GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var s string
		var n int
		if err := rows.Scan(&s, &n); err != nil {
			return nil, err
		}
		out[s] = n
	}
	return out, rows.Err()
}

func (q *Queue) setStatus(ctx context.Context, jobID, from, to, eventID, errText string) (bool, error) {
	res, err := q.db.ExecContext(ctx, `UPDATE outbox SET status=?, event_id=CASE WHEN ?<>'' THEN ? ELSE event_id END,
		error=CASE WHEN ?<>'' THEN ? ELSE error END, updated_at=? WHERE job_id=? AND status=?`,
		to, eventID, eventID, errText, errText, q.cfg.Clock().UnixMilli(), jobID, from)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

func (q *Queue) Cancel(ctx context.Context, jobID string) (JobView, error) {
	ok, err := q.setStatus(ctx, jobID, StatusQueued, StatusCancelled, "", "")
	if err != nil {
		return JobView{}, err
	}
	if !ok {
		v, gerr := q.Get(ctx, jobID)
		if gerr != nil {
			return JobView{}, gerr
		}
		return v, fmt.Errorf("only queued jobs can be cancelled; job is %s", v.Status)
	}
	return q.Get(ctx, jobID)
}

// OnStatus applies a bridge send-status event to the matching job.
func (q *Queue) OnStatus(u ingest.StatusUpdate) {
	ctx := context.Background()
	jobID := u.TxnID
	if jobID == "" || !q.exists(ctx, jobID) {
		jobID = ""
		_ = q.db.QueryRowContext(ctx, `SELECT job_id FROM outbox WHERE event_id=? AND event_id<>''`, string(u.EventID)).Scan(&jobID)
	}
	if jobID == "" {
		return
	}
	switch u.Status {
	case event.MessageStatusSuccess:
		_, _ = q.setStatus(ctx, jobID, StatusSending, StatusSent, "", "")
	case event.MessageStatusFail:
		_, _ = q.setStatus(ctx, jobID, StatusSending, StatusFailed, "", "bridge: "+u.Reason)
	case event.MessageStatusRetriable:
		_, _ = q.setStatus(ctx, jobID, StatusSending, StatusNeedsReview, "", "bridge (retriable): "+u.Reason)
	}
}

func (q *Queue) exists(ctx context.Context, jobID string) bool {
	var one int
	return q.db.QueryRowContext(ctx, `SELECT 1 FROM outbox WHERE job_id=?`, jobID).Scan(&one) == nil
}

// CheckTimeouts moves jobs the bridge never confirmed to needs_review.
func (q *Queue) CheckTimeouts(ctx context.Context) {
	cutoff := q.cfg.Clock().Add(-q.cfg.StatusTimeout).UnixMilli()
	_, _ = q.db.ExecContext(ctx, `UPDATE outbox SET status=?, error='no confirmation from bridge; verify in Messenger before resending', updated_at=?
		WHERE status=? AND sent_at>0 AND sent_at<?`, StatusNeedsReview, q.cfg.Clock().UnixMilli(), StatusSending, cutoff)
}

// Step sends at most one due job. It reports whether a send was attempted.
func (q *Queue) Step(ctx context.Context) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.cfg.Clock()
	if !q.last.IsZero() && now.Sub(q.last) < q.cfg.MinGap {
		return false, nil
	}
	var recent int
	if err := q.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox WHERE sent_at>?`, now.Add(-24*time.Hour).UnixMilli()).Scan(&recent); err != nil {
		return false, err
	}
	if recent >= q.cfg.DailyMax {
		return false, nil
	}
	var jobID, chatID, kind, text, file string
	err := q.db.QueryRowContext(ctx, `SELECT job_id, chat_id, kind, text, file_path FROM outbox WHERE status=? ORDER BY created_at, rowid LIMIT 1`, StatusQueued).
		Scan(&jobID, &chatID, &kind, &text, &file)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	chat, err := q.l.ChatByID(ctx, chatID)
	if err != nil {
		_, err = q.setStatus(ctx, jobID, StatusQueued, StatusFailed, "", "chat no longer known")
		return false, err
	}
	if _, err := q.db.ExecContext(ctx, `UPDATE outbox SET status=?, sent_at=?, updated_at=? WHERE job_id=? AND status=?`,
		StatusSending, now.UnixMilli(), now.UnixMilli(), jobID, StatusQueued); err != nil {
		return false, err
	}
	q.last = now

	var eventID string
	var sendErr error
	for attempt := 1; attempt <= sendAttempts; attempt++ {
		_, _ = q.db.ExecContext(ctx, `UPDATE outbox SET attempts=? WHERE job_id=?`, attempt, jobID)
		if kind == KindFile {
			eventID, sendErr = q.s.SendFile(ctx, chat.RoomID, jobID, file, text)
		} else {
			eventID, sendErr = q.s.SendText(ctx, chat.RoomID, jobID, text)
		}
		var perm permanentError
		if sendErr == nil || errors.As(sendErr, &perm) {
			break
		}
	}
	var perm permanentError
	switch {
	case sendErr == nil:
		_, err = q.db.ExecContext(ctx, `UPDATE outbox SET event_id=?, updated_at=? WHERE job_id=?`, eventID, now.UnixMilli(), jobID)
	case errors.As(sendErr, &perm):
		_, err = q.setStatus(ctx, jobID, StatusSending, StatusFailed, "", sendErr.Error())
	default:
		_, err = q.setStatus(ctx, jobID, StatusSending, StatusNeedsReview, "", "send outcome unknown: "+sendErr.Error())
	}
	return true, err
}

// Wait polls a job until it leaves queued/sending or the duration elapses.
func (q *Queue) Wait(ctx context.Context, jobID string, d time.Duration) (JobView, error) {
	deadline := time.Now().Add(d)
	for {
		v, err := q.Get(ctx, jobID)
		if err != nil || (v.Status != StatusQueued && v.Status != StatusSending) || time.Now().After(deadline) {
			return v, err
		}
		select {
		case <-ctx.Done():
			return v, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// Run processes the queue until ctx is cancelled.
func (q *Queue) Run(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		q.CheckTimeouts(ctx)
		_, _ = q.Step(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
