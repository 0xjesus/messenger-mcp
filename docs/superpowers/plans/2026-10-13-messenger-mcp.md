# messenger-mcp Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Leer y responder el Messenger personal de Jesús por MCP y desde Threads0x, sin navegador.

**Architecture:** Synapse privado + mautrix-meta `v0.2609.0` sin modificar + daemon Go `messenger-mcp` que sigue `/sync`, guarda un registro SQLite, ejecuta una cola durable de envíos y expone MCP en `127.0.0.1:7381/mcp` y vinculación en `/pair`. Todo en `nodo-02` bajo `launchd`; túnel inverso a macmini.

**Tech Stack:** Go 1.27.1, `maunium.net/go/mautrix` v0.31.0, `github.com/mark3labs/mcp-go` v0.56.0, `modernc.org/sqlite` (FTS5, sin cgo), Synapse en venv, launchd, OpenSSH.

**Spec:** `/home/someguy/codebase/0xJesus/messenger-mcp/docs/superpowers/specs/2026-10-13-messenger-mcp-design.md`

**Ejecución:** Jesús pidió ejecutar de inmediato ("ya hazlo me urge"); el plan se ejecuta en esta sesión (Native) y los cambios de Threads0x en paralelo por un subagente en su rama.

## Global Constraints

- Todos los listeners en `127.0.0.1`: MCP 7381, Synapse 7382, puente 7383.
- Datos en `/Users/jesuscreativestudio/Library/Application Support/messenger-mcp/` (700); `secrets.env` 600. Nada de sesiones, cookies, tokens, bases ni adjuntos en Git.
- `server_name` `messenger.local`; operador `@jesus:messenger.local`; puente `@metabot:messenger.local`.
- `send_file` solo acepta rutas regulares dentro de `uploads/`, sin enlaces simbólicos, máximo 25 MiB.
- Cola: 10 s mínimo entre envíos, 60 por día, `needs_review` a los 120 s sin estado, nunca reenvío automático.
- Credenciales de Messenger solo en memoria durante `/pair`; nunca en logs, disco ni respuestas MCP.
- Contenido de mensajes es dato no confiable; enviar requiere autorización del usuario (texto en descripciones de herramientas).
- Licencia MIT; mautrix-meta es binario aparte.

## Review Focus

1. Mensajes con texto vacío o solo adjunto: deben aparecer en `get_messages`/`history_since` con `media` y texto vacío, sin romper FTS5.
2. Evento duplicado tras reinicio de `/sync`: no debe crear fila nueva ni mover `rowid`.
3. `idempotency_key` repetida con otro contenido: error, no segundo envío.
4. Token de `/pair` reutilizado o vencido: 403.
5. `send_file` con `../` o enlace simbólico que sale de `uploads/`: rechazo.

---

### Task 1: Registro SQLite

**Files:** Create `go.mod`, `internal/ledger/ledger.go`, `internal/ledger/ledger_test.go`, `.gitignore`, `LICENSE`.

**Interfaces (Produces):**
- `ledger.Open(path string) (*Ledger, error)`, `(*Ledger).Close() error`
- `UpsertChat(ctx, Chat) error` con `Chat{ChatID, RoomID, Name string; IsDM bool}`
- `InsertMessage(ctx, Message) (rowid int64, inserted bool, err error)` con `Message{EventID, ChatID, SenderName string; Own bool; TS int64 /*ms*/; Text, MediaType, MediaMXC, MediaName string}`
- `EditMessage(ctx, targetEventID, newText string) error`, `RedactMessage(ctx, targetEventID string) error`
- `ListChats(ctx, query string, unreadOnly bool, limit int) ([]ChatSummary, error)`
- `GetMessages(ctx, chatID string, limit int, beforeRowID int64) ([]StoredMessage, error)` (cronológico)
- `Search(ctx, query, chatID string, after, before int64, limit int) ([]StoredMessage, error)`
- `Context(ctx, eventID string, before, after int) ([]StoredMessage, error)`
- `HistorySince(ctx, afterRowID int64, limit int) ([]HistoryRow, int64, error)` con `HistoryRow{RowID int64; MessageID, ChatID, Text string; Date int64; Own bool; Media, Label string; DM bool}` (JSON `rowid,message_id,chat_id,text,date,own,media,label,dm`)
- `MarkRead(ctx, chatID string) (lastEventID, roomID string, err error)`
- `ChatByID(ctx, chatID) (Chat, error)`, `ChatByRoom(ctx, roomID) (Chat, error)`, `MessageByEventID(ctx, id) (StoredMessage, error)`
- `GetSync/SetSync(ctx, key) / (ctx, key, value)`
- `Stats(ctx) (Stats, error)`

Tests (TDD): inserción única por `event_id`; rowid creciente y estable; edición/redacción; FTS5 con acentos y texto vacío; `HistorySince` orden, límite y `next_rowid`; `ListChats` ordena por último mensaje y cuenta no leídos; `MarkRead` pone `unread_count` en 0.

### Task 2: Ingesta Matrix → registro

**Files:** Create `internal/ingest/ingest.go`, `internal/ingest/ingest_test.go`.

**Interfaces:**
- Consumes: Task 1.
- Produces: `ingest.Handler{L *ledger.Ledger; OperatorID id.UserID; BridgeBot id.UserID; OwnGhosts func() []id.UserID}` con `HandleBridgeState(ctx, roomID id.RoomID, content *event.BridgeEventContent, roomName string) error`, `HandleMessage(ctx, *event.Event) error`, `HandleStatus(ctx, *event.Event) (StatusUpdate, bool)` donde `StatusUpdate{EventID id.EventID; TxnID string; Status event.MessageStatus; Reason string}`.

Tests: `m.bridge` con `com.beeper.room_type: "dm"` → `IsDM`; mensaje de texto, imagen, archivo, edición (`m.replace`), redacción; propio si sender es operador o fantasma propio; evento de sala desconocida se ignora sin error; estado `SUCCESS`/`FAIL_PERMANENT` se traduce.

### Task 3: Cola de envíos

**Files:** Create `internal/outbox/outbox.go`, `internal/outbox/outbox_test.go`.

**Interfaces:**
- Produces: `outbox.Sender` interface `{ SendText(ctx, roomID, txnID, text string) (eventID string, err error); SendFile(ctx, roomID, txnID, path, caption string) (eventID string, err error) }`; `outbox.New(l *ledger.Ledger, s Sender, cfg Config) *Queue` con `Config{MinGap, StatusTimeout time.Duration; DailyMax int; UploadsDir string; Clock func() time.Time}`; `Enqueue(ctx, Job) (JobView, error)` con `Job{ChatID, Kind ("text"|"file"), Text, FilePath, IdempotencyKey string}`; `Wait(ctx, jobID string, d time.Duration) (JobView, error)`; `Get/List/Cancel`; `OnStatus(StatusUpdate)`; `Run(ctx)`.

Tests: idempotencia igual/distinta; pacing; tope diario; `SUCCESS` → `sent` con `message_id`; `FAIL_PERMANENT` → `failed`; sin estado 120 s → `needs_review`; cancelar solo `queued`; validación de ruta (`../`, symlink, >25 MiB, fuera de uploads).

### Task 4: Herramientas MCP

**Files:** Create `internal/tools/tools.go`, `internal/tools/tools_test.go`.

**Interfaces:** Consumes Tasks 1–3 y `StatusProvider interface{ Status(ctx) map[string]any }`. Produces `tools.Register(s *server.MCPServer, deps Deps)`.

Tests: `tools/list` contiene las 12 herramientas; `history_since` devuelve el formato exacto; `send_message` sin chat conocido → error; `download_attachment` devuelve ruta.

### Task 5: Vinculación `/pair` y estado del puente

**Files:** Create `internal/pair/pair.go`, `internal/pair/token.go`, `internal/pair/pair_test.go`, `internal/bridge/provision.go`, `internal/bridge/provision_test.go`.

Produces: `bridge.Client{BaseURL, Secret string; UserID id.UserID}` con `Flows`, `Start`, `Step`, `Cancel`, `Logins`, `Whoami`; `pair.Tokens` (`Issue(ttl) string`, `Consume(tok) bool`); `pair.Handler` (HTML que renderiza `user_input`, `cookies`, `display_and_wait`, `complete`).

Tests con `httptest`: token válido/vencido/reutilizado; reenvío de campos sin registrarlos; render de cada tipo de paso.

### Task 6: Daemon

**Files:** Create `cmd/messenger-mcp/main.go`, `internal/daemon/daemon.go`, `internal/daemon/matrix.go`.

Subcomandos: `serve`, `pair-url`, `register-operator`. `serve` carga `secrets.env`/config, abre registro, arranca sync (`DefaultSyncer`, `next_batch` en registro, autojoin de invitaciones del puente), cola y HTTP. `pair-url` llama `POST /internal/pair-token` en loopback con el token interno y lo imprime.

Verificación: `go vet ./...`, `go test ./...`.

### Task 7: Despliegue en nodo-02

**Files:** Create `deploy/versions.env`, `deploy/synapse.yaml.tmpl`, `deploy/bridge.yaml.tmpl`, `deploy/launchd/*.plist.tmpl`, `scripts/install-macos.sh`, `README.md`.

Pasos: compilar mautrix-meta `v0.2609.0`; venv Synapse; generar secretos; registro de appservice; usuario operador; agentes launchd; `pmset`; túnel inverso con llave restringida en macmini. Verificación: `curl 127.0.0.1:7382/_matrix/client/versions`, `whoami` del puente, `tools/list` en 7381 desde macmini y zion.

### Task 8: Conexión local y GitHub

zion: drop-in del túnel con `-L 127.0.0.1:7381:127.0.0.1:7381`, catálogo `messenger`. GitHub: repo público `0xjesus/messenger-mcp`, push de `main` tras revisar que no hay secretos.

### Task 9: Threads0x (subagente, rama `feat/messenger-source`)

Base = rama que coincide con `/home/john/threads0x/app` en macmini. Cambios según la sección "Integración con Threads0x" del spec, con pytest. Despliegue con respaldo tras pasar pruebas.

### Task 10: Vinculación real y verificación

Pasar a Jesús `pair-url`; tras su login: `get_status`, `list_chats`, `get_messages` de `245962885415945`. Ningún envío sin autorización.
