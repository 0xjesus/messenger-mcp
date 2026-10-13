package tools

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/0xjesus/messenger-mcp/internal/ledger"
	"github.com/0xjesus/messenger-mcp/internal/outbox"
)

type nopSender struct{}

func (nopSender) SendText(context.Context, string, string, string) (string, error) { return "$x", nil }
func (nopSender) SendFile(context.Context, string, string, string, string) (string, error) {
	return "$x", nil
}

type fakeMedia struct{}

func (fakeMedia) Download(_ context.Context, mxc string) ([]byte, string, error) {
	return []byte("\x89PNG fake"), "image/png", nil
}

type fakeReads struct{ room, event string }

func (f *fakeReads) MarkRead(_ context.Context, room, event string) error {
	f.room, f.event = room, event
	return nil
}

func setup(t *testing.T) (*server.MCPServer, *ledger.Ledger, *fakeReads, string) {
	t.Helper()
	dir := t.TempDir()
	l, err := ledger.Open(filepath.Join(dir, "l.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	ctx := context.Background()
	l.UpsertChat(ctx, ledger.Chat{ChatID: "245962885415945", RoomID: "!forum:messenger.local", Name: "Sala Forum", IsDM: true})
	l.InsertMessage(ctx, ledger.Message{EventID: "$1", ChatID: "245962885415945", TS: 1_700_000_000_000, Text: "Le echo un vistazo", SenderName: "Sala Forum"})
	l.InsertMessage(ctx, ledger.Message{EventID: "$img", ChatID: "245962885415945", TS: 1_700_000_001_000, MediaType: "image", MediaMXC: "mxc://messenger.local/abc", MediaName: "salón 1.png"})
	uploads := filepath.Join(dir, "uploads")
	downloads := filepath.Join(dir, "downloads")
	os.MkdirAll(uploads, 0o700)
	os.MkdirAll(downloads, 0o700)
	q, err := outbox.New(l, nopSender{}, outbox.Config{MinGap: time.Second, StatusTimeout: time.Minute, DailyMax: 10, UploadsDir: uploads, MaxFileBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	reads := &fakeReads{}
	s := server.NewMCPServer("messenger", "test", server.WithToolCapabilities(true))
	Register(s, Deps{
		L: l, Q: q, Media: fakeMedia{}, Reads: reads, DownloadsDir: downloads, UploadsDir: uploads,
		SendWait: 10 * time.Millisecond,
		Status:   func(context.Context) map[string]any { return map[string]any{"bridge": "connected"} },
	})
	return s, l, reads, downloads
}

func rpc(t *testing.T, s *server.MCPServer, method string, params any) json.RawMessage {
	t.Helper()
	req, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	resp := s.HandleMessage(context.Background(), req)
	b, _ := json.Marshal(resp)
	var env struct {
		Result json.RawMessage `json:"result"`
		Error  any             `json:"error"`
	}
	json.Unmarshal(b, &env)
	if env.Error != nil {
		t.Fatalf("%s: %v", method, env.Error)
	}
	return env.Result
}

func call(t *testing.T, s *server.MCPServer, name string, args map[string]any) (mcp.CallToolResult, string) {
	t.Helper()
	raw := rpc(t, s, "tools/call", map[string]any{"name": name, "arguments": args})
	var res struct {
		IsError bool `json:"isError"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	json.Unmarshal(raw, &res)
	text := ""
	if len(res.Content) > 0 {
		text = res.Content[0].Text
	}
	return mcp.CallToolResult{IsError: res.IsError}, text
}

func TestToolsList(t *testing.T) {
	s, _, _, _ := setup(t)
	var list struct {
		Tools []struct {
			Name        string `json:"name"`
			Description string `json:"description"`
		} `json:"tools"`
	}
	json.Unmarshal(rpc(t, s, "tools/list", map[string]any{}), &list)
	var names []string
	for _, tl := range list.Tools {
		names = append(names, tl.Name)
		if tl.Name == "send_message" && !strings.Contains(tl.Description, "autoriz") {
			t.Fatalf("send_message must require explicit authorization: %s", tl.Description)
		}
	}
	sort.Strings(names)
	want := []string{"cancel_outbox", "download_attachment", "get_message_context", "get_messages", "get_outbox", "get_status",
		"history_since", "list_chats", "mark_read", "search_messages", "send_file", "send_message"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("tools = %v", names)
	}
}

func TestHistorySinceShape(t *testing.T) {
	s, _, _, _ := setup(t)
	_, text := call(t, s, "history_since", map[string]any{"after_rowid": 0, "limit": 1})
	var out struct {
		Rows []map[string]any `json:"rows"`
		Next int64            `json:"next_rowid"`
	}
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		t.Fatalf("%v: %s", err, text)
	}
	if len(out.Rows) != 1 || out.Next != 1 {
		t.Fatalf("out %+v", out)
	}
	for _, k := range []string{"rowid", "message_id", "chat_id", "text", "date", "own", "media", "label", "dm"} {
		if _, ok := out.Rows[0][k]; !ok {
			t.Fatalf("missing key %s in %v", k, out.Rows[0])
		}
	}
	if out.Rows[0]["date"].(float64) != 1_700_000_000 || out.Rows[0]["label"] != "Sala Forum" {
		t.Fatalf("row %v", out.Rows[0])
	}
}

func TestReadToolsAndMarkRead(t *testing.T) {
	s, _, reads, _ := setup(t)
	if _, text := call(t, s, "list_chats", map[string]any{"query": "forum"}); !strings.Contains(text, "245962885415945") {
		t.Fatalf("list_chats: %s", text)
	}
	if _, text := call(t, s, "get_messages", map[string]any{"chat_id": "245962885415945"}); !strings.Contains(text, "Le echo un vistazo") || !strings.Contains(text, `"messages"`) {
		t.Fatalf("get_messages: %s", text)
	}
	if _, text := call(t, s, "search_messages", map[string]any{"query": "vistazo"}); !strings.Contains(text, "$1") {
		t.Fatalf("search: %s", text)
	}
	if _, text := call(t, s, "get_message_context", map[string]any{"message_id": "$img", "before": 1}); !strings.Contains(text, "$1") {
		t.Fatalf("context: %s", text)
	}
	if _, text := call(t, s, "get_status", nil); !strings.Contains(text, `"bridge": "connected"`) || !strings.Contains(text, `"messages": 2`) {
		t.Fatalf("status: %s", text)
	}
	if res, text := call(t, s, "mark_read", map[string]any{"chat_id": "245962885415945"}); res.IsError || reads.event != "$img" || reads.room != "!forum:messenger.local" {
		t.Fatalf("mark_read: %s %+v", text, reads)
	}
}

func TestSendAndOutbox(t *testing.T) {
	s, _, _, _ := setup(t)
	if res, _ := call(t, s, "send_message", map[string]any{"chat_id": "nope", "text": "hola"}); !res.IsError {
		t.Fatalf("unknown chat must error")
	}
	res, text := call(t, s, "send_message", map[string]any{"chat_id": "245962885415945", "text": "hola", "idempotency_key": "k"})
	var job outbox.JobView
	json.Unmarshal([]byte(text), &job)
	if res.IsError || job.Status != outbox.StatusQueued || job.JobID == "" {
		t.Fatalf("send: %s", text)
	}
	if _, text := call(t, s, "get_outbox", map[string]any{"job_id": job.JobID}); !strings.Contains(text, job.JobID) || strings.Contains(text, "hola") {
		t.Fatalf("get_outbox must not include content: %s", text)
	}
	if _, text := call(t, s, "cancel_outbox", map[string]any{"job_id": job.JobID}); !strings.Contains(text, "cancelled") {
		t.Fatalf("cancel: %s", text)
	}
	if res, _ := call(t, s, "send_file", map[string]any{"chat_id": "245962885415945", "path": "/etc/passwd"}); !res.IsError {
		t.Fatalf("file outside uploads must error")
	}
}

func TestDownloadAttachment(t *testing.T) {
	s, _, _, downloads := setup(t)
	raw := rpc(t, s, "tools/call", map[string]any{"name": "download_attachment", "arguments": map[string]any{"message_id": "$img"}})
	var res struct {
		Content []struct {
			Type, Text, MimeType string
		} `json:"content"`
	}
	json.Unmarshal(raw, &res)
	if len(res.Content) != 2 || res.Content[1].Type != "image" {
		t.Fatalf("expected json + image content: %s", raw)
	}
	var info struct{ Path string }
	json.Unmarshal([]byte(res.Content[0].Text), &info)
	if filepath.Dir(info.Path) != downloads || strings.ContainsAny(filepath.Base(info.Path), "/ ") {
		t.Fatalf("path %q must be a safe name inside downloads", info.Path)
	}
	if b, err := os.ReadFile(info.Path); err != nil || string(b) != "\x89PNG fake" {
		t.Fatalf("file: %q %v", b, err)
	}
	if res, _ := call(t, s, "download_attachment", map[string]any{"message_id": "$1"}); !res.IsError {
		t.Fatalf("text message has no attachment")
	}
}
