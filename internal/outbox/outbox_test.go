package outbox

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/0xjesus/messenger-mcp/internal/ingest"
	"github.com/0xjesus/messenger-mcp/internal/ledger"
)

type sent struct{ room, txn, text, path string }

type fakeSender struct {
	mu    sync.Mutex
	calls []sent
	fail  []error // consumed one per call
}

func (f *fakeSender) next() error {
	if len(f.fail) == 0 {
		return nil
	}
	err := f.fail[0]
	f.fail = f.fail[1:]
	return err
}

func (f *fakeSender) SendText(_ context.Context, room, txn, text string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, sent{room: room, txn: txn, text: text})
	if err := f.next(); err != nil {
		return "", err
	}
	return "$ev-" + txn, nil
}

func (f *fakeSender) SendFile(_ context.Context, room, txn, path, caption string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, sent{room: room, txn: txn, text: caption, path: path})
	if err := f.next(); err != nil {
		return "", err
	}
	return "$ev-" + txn, nil
}

type clock struct{ now time.Time }

func (c *clock) Now() time.Time          { return c.now }
func (c *clock) Advance(d time.Duration) { c.now = c.now.Add(d) }

func setup(t *testing.T) (*Queue, *fakeSender, *clock, string) {
	t.Helper()
	dir := t.TempDir()
	l, err := ledger.Open(filepath.Join(dir, "l.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	if err := l.UpsertChat(context.Background(), ledger.Chat{ChatID: "245962885415945", RoomID: "!forum:messenger.local", Name: "Sala Forum", IsDM: true}); err != nil {
		t.Fatal(err)
	}
	uploads := filepath.Join(dir, "uploads")
	if err := os.MkdirAll(uploads, 0o700); err != nil {
		t.Fatal(err)
	}
	f := &fakeSender{}
	c := &clock{now: time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)}
	q, err := New(l, f, Config{MinGap: 10 * time.Second, StatusTimeout: 120 * time.Second, DailyMax: 3, UploadsDir: uploads, MaxFileBytes: 1024, Clock: c.Now})
	if err != nil {
		t.Fatal(err)
	}
	return q, f, c, uploads
}

func enqueueText(t *testing.T, q *Queue, text, key string) JobView {
	t.Helper()
	v, err := q.Enqueue(context.Background(), Job{ChatID: "245962885415945", Kind: KindText, Text: text, IdempotencyKey: key})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func step(t *testing.T, q *Queue) bool {
	t.Helper()
	did, err := q.Step(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return did
}

func TestSendLifecycleUntilBridgeConfirms(t *testing.T) {
	q, f, _, _ := setup(t)
	v := enqueueText(t, q, "Hola, buen día", "")
	if v.Status != StatusQueued {
		t.Fatalf("new job status %s", v.Status)
	}
	if !step(t, q) || len(f.calls) != 1 || f.calls[0].room != "!forum:messenger.local" || f.calls[0].txn != v.JobID {
		t.Fatalf("calls %+v", f.calls)
	}
	got, _ := q.Get(context.Background(), v.JobID)
	if got.Status != StatusSending {
		t.Fatalf("awaiting bridge confirmation, got %s", got.Status)
	}
	q.OnStatus(ingest.StatusUpdate{TxnID: v.JobID, EventID: "$ev-" + "x", Status: event.MessageStatusSuccess})
	got, _ = q.Get(context.Background(), v.JobID)
	if got.Status != StatusSent || got.MessageID != "$ev-"+v.JobID {
		t.Fatalf("after success: %+v", got)
	}
}

func TestStatusByEventIDWithoutTxn(t *testing.T) {
	q, _, _, _ := setup(t)
	v := enqueueText(t, q, "uno", "")
	step(t, q)
	q.OnStatus(ingest.StatusUpdate{EventID: "$ev-" + "nope", Status: event.MessageStatusSuccess})
	if got, _ := q.Get(context.Background(), v.JobID); got.Status != StatusSending {
		t.Fatalf("unrelated event must not change job: %s", got.Status)
	}
	q.OnStatus(ingest.StatusUpdate{EventID: id.EventID("$ev-" + v.JobID), Status: event.MessageStatusFail, Reason: "blocked by Meta"})
	got, _ := q.Get(context.Background(), v.JobID)
	if got.Status != StatusFailed || !strings.Contains(got.Error, "blocked by Meta") {
		t.Fatalf("permanent failure: %+v", got)
	}
}

func TestRetriableFailureNeedsReviewNotResend(t *testing.T) {
	q, f, c, _ := setup(t)
	v := enqueueText(t, q, "uno", "")
	step(t, q)
	q.OnStatus(ingest.StatusUpdate{TxnID: v.JobID, Status: event.MessageStatusRetriable, Reason: "timeout"})
	c.Advance(time.Minute)
	step(t, q)
	got, _ := q.Get(context.Background(), v.JobID)
	if got.Status != StatusNeedsReview || len(f.calls) != 1 {
		t.Fatalf("retriable: %+v calls=%d", got, len(f.calls))
	}
}

func TestNoStatusWithinTimeoutNeedsReview(t *testing.T) {
	q, f, c, _ := setup(t)
	v := enqueueText(t, q, "uno", "")
	step(t, q)
	c.Advance(119 * time.Second)
	q.CheckTimeouts(context.Background())
	if got, _ := q.Get(context.Background(), v.JobID); got.Status != StatusSending {
		t.Fatalf("before timeout: %s", got.Status)
	}
	c.Advance(2 * time.Second)
	q.CheckTimeouts(context.Background())
	if got, _ := q.Get(context.Background(), v.JobID); got.Status != StatusNeedsReview {
		t.Fatalf("after timeout: %s", got.Status)
	}
	if len(f.calls) != 1 {
		t.Fatalf("never auto-resend, calls=%d", len(f.calls))
	}
}

func TestTransientErrorsRetrySameTxnThenPermanentFails(t *testing.T) {
	q, f, _, _ := setup(t)
	f.fail = []error{errors.New("connection reset"), nil}
	v := enqueueText(t, q, "uno", "")
	step(t, q)
	if len(f.calls) != 2 || f.calls[0].txn != f.calls[1].txn {
		t.Fatalf("retry must reuse txn: %+v", f.calls)
	}
	if got, _ := q.Get(context.Background(), v.JobID); got.Status != StatusSending {
		t.Fatalf("status %s", got.Status)
	}

	q2, f2, _, _ := setup(t)
	f2.fail = []error{Permanent(errors.New("M_FORBIDDEN"))}
	v2 := enqueueText(t, q2, "dos", "")
	step(t, q2)
	if got, _ := q2.Get(context.Background(), v2.JobID); got.Status != StatusFailed || len(f2.calls) != 1 {
		t.Fatalf("permanent: %+v calls=%d", got, len(f2.calls))
	}

	q3, f3, _, _ := setup(t)
	f3.fail = []error{errors.New("eof"), errors.New("eof"), errors.New("eof")}
	v3 := enqueueText(t, q3, "tres", "")
	step(t, q3)
	if got, _ := q3.Get(context.Background(), v3.JobID); got.Status != StatusNeedsReview || len(f3.calls) != 3 {
		t.Fatalf("uncertain after retries: %+v calls=%d", got, len(f3.calls))
	}
}

func TestPacingAndDailyLimit(t *testing.T) {
	q, f, c, _ := setup(t)
	for _, s := range []string{"a", "b", "c", "d"} {
		enqueueText(t, q, s, "")
	}
	step(t, q)
	if step(t, q) {
		t.Fatalf("second send must wait for MinGap")
	}
	for i := 0; i < 5; i++ {
		c.Advance(11 * time.Second)
		step(t, q)
	}
	if len(f.calls) != 3 {
		t.Fatalf("daily max 3, sent %d", len(f.calls))
	}
	c.Advance(24 * time.Hour)
	step(t, q)
	if len(f.calls) != 4 {
		t.Fatalf("window rolls after a day, sent %d", len(f.calls))
	}
}

func TestIdempotencyKey(t *testing.T) {
	q, _, _, _ := setup(t)
	a := enqueueText(t, q, "hola", "k1")
	b := enqueueText(t, q, "hola", "k1")
	if a.JobID != b.JobID {
		t.Fatalf("same key and content must return the same job")
	}
	if _, err := q.Enqueue(context.Background(), Job{ChatID: "245962885415945", Kind: KindText, Text: "otro", IdempotencyKey: "k1"}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("different content with same key: %v", err)
	}
}

func TestCancelOnlyQueued(t *testing.T) {
	q, f, _, _ := setup(t)
	a := enqueueText(t, q, "a", "")
	b := enqueueText(t, q, "b", "")
	if v, err := q.Cancel(context.Background(), b.JobID); err != nil || v.Status != StatusCancelled {
		t.Fatalf("cancel queued: %+v %v", v, err)
	}
	step(t, q)
	if _, err := q.Cancel(context.Background(), a.JobID); err == nil {
		t.Fatalf("cancelling a job in flight must fail")
	}
	if len(f.calls) != 1 || f.calls[0].text != "a" {
		t.Fatalf("cancelled job must not be sent: %+v", f.calls)
	}
}

func TestEnqueueValidation(t *testing.T) {
	q, _, _, uploads := setup(t)
	ctx := context.Background()
	if _, err := q.Enqueue(ctx, Job{ChatID: "999", Kind: KindText, Text: "x"}); err == nil {
		t.Fatalf("unknown chat must be rejected")
	}
	if _, err := q.Enqueue(ctx, Job{ChatID: "245962885415945", Kind: KindText, Text: "  "}); err == nil {
		t.Fatalf("blank text must be rejected")
	}
	ok := filepath.Join(uploads, "presskit.pdf")
	os.WriteFile(ok, []byte("%PDF"), 0o600)
	if _, err := q.Enqueue(ctx, Job{ChatID: "245962885415945", Kind: KindFile, FilePath: ok, Text: "press kit"}); err != nil {
		t.Fatalf("valid upload: %v", err)
	}
	outside := filepath.Join(filepath.Dir(uploads), "secret.txt")
	os.WriteFile(outside, []byte("x"), 0o600)
	link := filepath.Join(uploads, "link.pdf")
	os.Symlink(outside, link)
	big := filepath.Join(uploads, "big.bin")
	os.WriteFile(big, make([]byte, 2048), 0o600)
	for _, p := range []string{outside, filepath.Join(uploads, "..", "secret.txt"), link, big, filepath.Join(uploads, "missing.pdf"), uploads, "presskit.pdf"} {
		if _, err := q.Enqueue(ctx, Job{ChatID: "245962885415945", Kind: KindFile, FilePath: p}); err == nil {
			t.Fatalf("path %s must be rejected", p)
		}
	}
}
