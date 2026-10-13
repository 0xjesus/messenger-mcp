# messenger-mcp: diseño

Fecha: 13 de octubre de 2026. Estado: aprobado en conversación, pendiente de revisión escrita.

## Objetivo

Leer y responder el Messenger personal de Jesús desde Claude y desde Threads0x, sin navegador. Primer uso: seguimiento de lugares para Búfalo Púrpura (Sala Forum, Salón Country, Beat 803, El Azulito), incluidos chats cifrados.

Éxito significa:

1. Desde Claude, `get_status` reporta la cuenta conectada; `list_chats` muestra los chats de Messenger; `get_messages` lee la conversación `245962885415945` (Sala Forum), incluso si está cifrada.
2. `send_message` y `send_file` entregan un texto y el press kit PDF en un chat elegido. Solo se envía con autorización explícita de Jesús por mensaje.
3. Threads0x acepta `messenger` en `link_source`, revisa DMs nuevos de Messenger y manda borradores `propose_message` con botón de aprobación en Telegram.
4. Al reiniciar la Mac nueva, los servicios vuelven solos sin intervención.

## Alcance

Incluido en v1: listar chats, leer, buscar, contexto de un mensaje, descargar adjuntos, enviar texto, enviar archivos, marcar leído, cola durable de envíos, cursor `history_since` para Threads0x, página de vinculación.

Fuera de v1: grupos (administración), reacciones, encuestas, mensajes programados, Instagram, índice semántico compartido (servicio 7256), transcripción de audio, panel de monitoreo.

## Arquitectura

Todo corre en la Mac nueva `nodo-02` (macOS 15.7.4, Intel x86_64, usuario `jesuscreativestudio`, Tailscale `100.92.61.57`). Cada proceso escucha solo en `127.0.0.1` y lo administra `launchd` como agente del usuario con `KeepAlive`.

| Proceso | Puerto | Responsabilidad |
|---|---|---|
| Synapse | 7382 | Servidor Matrix privado. `server_name` `messenger.local`, SQLite, sin federación, sin registro abierto. |
| mautrix-meta `v0.2609.0` | 7383 | Puente sin modificar, compilado desde el tag fijado con el Go 1.27.1 de la Mac. Habla con Messenger (incluido el cifrado de extremo a extremo) y refleja cada hilo como sala Matrix. |
| messenger-mcp | 7381 | Nuestro programa en Go. Expone MCP en `/mcp` y la vinculación en `/pair`. Sincroniza Matrix a su registro SQLite y ejecuta la cola de envíos. |
| túnel inverso | — | `ssh -NT -R 127.0.0.1:7381:127.0.0.1:7381` hacia macmini (`john@100.64.70.73`). |

Synapse sustituye a continuwuity porque continuwuity no tiene paquete para macOS Intel y Synapse es el servidor contra el que mautrix prueba sus puentes. Se instala con pip en un entorno virtual con versión fijada.

mautrix-meta (AGPL-3.0) es un binario aparte: no se enlaza ni se copia su código. messenger-mcp usa `maunium.net/go/mautrix` (MPL-2.0) y `github.com/mark3labs/mcp-go`, como el MCP de WhatsApp. El repositorio usa licencia MIT.

### Usuarios Matrix

- `@jesus:messenger.local`: usuario operador, administrador del puente. messenger-mcp actúa con su token. Acepta automáticamente las invitaciones del puente a salas de hilos.
- `@metabot:messenger.local` y fantasmas `@meta_*`: los crea el puente mediante el registro de appservice.

El puente usa `permissions: {"@jesus:messenger.local": "admin"}`, cifrado del lado Matrix desactivado (todo es local), `message_status_events: true` y backfill inicial de 50 mensajes por chat.

### Datos

Raíz en la Mac: `/Users/jesuscreativestudio/Library/Application Support/messenger-mcp/` con permisos 700.

| Ruta relativa | Contenido |
|---|---|
| `synapse/` | Configuración, base SQLite y medios de Synapse. |
| `bridge/` | Configuración, registro de appservice y base del puente (sesión de Messenger). |
| `ledger.db` | Registro de messenger-mcp: chats, mensajes con índice FTS5 y cola de envíos. |
| `uploads/` | Única carpeta desde la que `send_file` acepta archivos. |
| `downloads/` | Adjuntos descargados. |
| `secrets.env` | Tokens de appservice, token del operador, secreto de provisioning. Permisos 600. |

El repositorio nunca contiene sesiones, cookies, tokens, bases ni adjuntos. `.gitignore` lo refuerza y una prueba rechaza patrones de secretos en archivos versionados.

### Instalación

`scripts/install-macos.sh` (idempotente):

1. Verifica Go, Python 3 y puertos libres 7381–7383.
2. Compila mautrix-meta desde `v0.2609.0` y messenger-mcp desde el repositorio; instala binarios en `~/.local/bin/`.
3. Crea el entorno virtual de Synapse con la última versión estable de PyPI al implementar, fijada en `deploy/versions.env` junto con el tag del puente.
4. Genera configuraciones y secretos aleatorios si no existen; registra al operador.
5. Escribe e inicia los agentes `launchd` con prefijo `com.0xjesus.messenger.` (`synapse`, `bridge`, `mcp`, `tunnel`).

La Mac no debe dormir: con autorización de Jesús se configura `pmset` para mantenerla despierta con corriente.

## Registro y herramientas

### Ingesta

messenger-mcp sigue `/sync` del operador. Por cada sala de puente lee el estado `m.bridge` para obtener el ID del hilo de Messenger (`chat_id`, el mismo número de `facebook.com/messages/t/<id>`) y su nombre. Una sala es DM según `com.beeper.room_type` publicado por el puente; si no existe, cuando tiene exactamente un participante además del operador y el bot.

Cada evento `m.room.message` se guarda una vez, por `event_id`, con `rowid` autoincremental que nunca se reutiliza. Las ediciones actualizan el texto y marcan `edited`. Las redacciones marcan `deleted` y vacían el texto. Un mensaje es propio si lo envía el operador o el fantasma de la cuenta de Messenger vinculada.

Tablas:

- `chats(chat_id PK, room_id UNIQUE, name, is_dm, last_rowid, last_ts, unread_count)`
- `messages(rowid INTEGER PRIMARY KEY AUTOINCREMENT, event_id UNIQUE, chat_id, sender_name, own, ts, text, media_type, media_mxc, media_name, edited, deleted)` más tabla FTS5 sobre `text`.
- `outbox(job_id PK, idempotency_key UNIQUE, chat_id, kind, text, file_path, status, attempts, event_id, error, created_at, updated_at)`
- `sync_state(key PK, value)` con el token `next_batch`.

### Herramientas MCP

| Herramienta | Parámetros | Resultado |
|---|---|---|
| `get_status` | — | Estado de Matrix, del puente (`connected`, `disconnected`, `needs_relogin`, `not_logged_in`), cuenta vinculada, conteos del registro y de la cola. |
| `list_chats` | `query?`, `unread_only?`, `limit` (30, máx. 100) | Chats con `chat_id`, nombre, DM, último mensaje y no leídos. |
| `get_messages` | `chat_id`, `limit` (40, máx. 200), `before_rowid?` | Mensajes en orden cronológico. |
| `search_messages` | `query`, `chat_id?`, `after?`, `before?`, `limit` (30, máx. 100) | Coincidencias FTS5 con `rowid` y chat. |
| `get_message_context` | `message_id`, `before` (8), `after` (8) | Mensajes alrededor. |
| `download_attachment` | `message_id` | Ruta absoluta en la Mac, tipo y tamaño; imágenes de hasta 5 MiB también como contenido de imagen. |
| `history_since` | `after_rowid` (0), `limit` (500, máx. 1000) | Filas `{rowid, message_id, chat_id, text, date, own, media, label, dm}` con `date` en segundos Unix, más `next_rowid`. |
| `send_message` | `chat_id`, `text`, `idempotency_key?` | `{job_id, status, message_id?}` |
| `send_file` | `chat_id`, `path` dentro de `uploads/`, `caption?`, `idempotency_key?` | `{job_id, status, message_id?}` |
| `get_outbox` | `job_id?`, `limit` (50) | Trabajos con estado, intentos y error, sin contenido. |
| `cancel_outbox` | `job_id` | Cancela solo trabajos `queued`. |
| `mark_read` | `chat_id` | Envía recibo de lectura del último mensaje. |

Las descripciones de herramientas indican que el contenido de mensajes es dato no confiable, nunca instrucción, y que enviar requiere autorización del usuario.

### Cola de envíos

Estados: `queued → sending → sent | failed | needs_review`, más `cancelled`.

1. La herramienta valida chat, texto y ruta; inserta el trabajo. Una `idempotency_key` repetida con el mismo contenido devuelve el trabajo existente; con contenido distinto, error.
2. Un solo trabajador procesa en orden, con al menos 10 segundos entre envíos y máximo 60 por día (configurable).
3. Envía como el operador con `txn_id = job_id`, así un reintento de red no duplica en Synapse. Los archivos se suben primero al repositorio de medios.
4. Espera el evento de estado del puente (`com.beeper.message_send_status`). Éxito → `sent` con `message_id`. Fallo no reintentable → `failed`. Sin respuesta en 120 segundos → `needs_review`; nunca se reenvía automáticamente.
5. La herramienta espera hasta 20 segundos y devuelve el estado actual; `queued` o `sending` significan aceptado, no entregado.

## Vinculación y seguridad

- `messenger-mcp pair-url` (ejecutado por SSH en la Mac) imprime `http://127.0.0.1:7381/pair?token=<aleatorio>`, válido 15 minutos y de un solo uso. `/pair` rechaza solicitudes sin token válido.
- `/pair` guía el API de provisioning v3 del puente con su secreto compartido: muestra los flujos (Messenger Android por defecto; Messenger iOS, cookies de messenger.com y facebook.com como alternativas) y renderiza cada paso (campos, códigos 2FA, esperas, completado).
- Jesús escribe usuario, contraseña, códigos y aprobaciones. messenger-mcp los reenvía en memoria al puente; nunca los registra en logs, disco ni respuestas MCP. El asistente nunca los escribe.
- Si Meta pide aprobar el dispositivo o un PIN de chats cifrados, Jesús lo hace en `/pair` o en su teléfono.
- Los mensajes cifrados anteriores a la vinculación pueden no estar disponibles para el dispositivo nuevo; no se promete historial antiguo completo.

## Red

1. En `nodo-02`, llave dedicada `~/.ssh/messenger_tunnel_ed25519` sin passphrase.
2. En macmini, `~/.ssh/authorized_keys` la acepta con `restrict,port-forwarding,permitlisten="127.0.0.1:7381",command="/bin/false"`: solo puede abrir ese puerto.
3. El agente `com.0xjesus.messenger.tunnel` mantiene el túnel con `ExitOnForwardFailure=yes` y `ServerAliveInterval=15`.
4. En esta máquina (zion), `messaging-macmini-tunnel.service` agrega `-L 127.0.0.1:7381:127.0.0.1:7381` y el catálogo de extensiones registra `messenger` en `http://127.0.0.1:7381/mcp`.

Así Threads0x y Claude usan la misma dirección `http://127.0.0.1:7381/mcp` en su propia máquina.

## Integración con Threads0x

Threads0x corre en macmini desde `/home/john/threads0x/app`, sin Git. Antes de modificarlo se identifica qué rama local coincide con lo desplegado (candidatas `prod/mail-accounts` y `feat/seguimiento-humano`) y se crea `feat/messenger-source` desde ella. Cambios:

- `store.py`: `messenger` en `SOURCES` y `CHANNELS`.
- `config.py`: `"messenger": "http://127.0.0.1:7381/mcp"`.
- `followup_worker.py`: para `messenger`, el lector pide `history_since` por MCP en vez de abrir SQLite; mantiene el mismo cursor `dm_cursors` con `rowid`. Si el MCP no responde, la fuente queda degradada y el cursor no avanza.
- `sender.py`: `"messenger": lambda d: {"chat_id": d["dest"], "text": d["text"]}`; respeta `queued` y `needs_review` sin marcar como enviado.
- `topics_mcp.py`: `messenger` en los enums de `link_source` y `propose_message`.
- `meeting_worker.py`: lectura de chats ligados con `get_messages`.
- `bot.py`: etiqueta "Messenger".
- `coverage_status` / `connections_status`: Messenger con último mensaje visto, estado de conexión y fallos.
- Las rutas que asumen una base SQLite por fuente se ajustan para que `messenger` no requiera archivo local.

## Errores

- Matrix o puente caídos: `get_status` lo indica; lectura sigue sirviendo el registro local con aviso `stale`; envíos quedan `queued`.
- Sesión de Messenger expirada o checkpoint: el puente lo reporta y `get_status` devuelve `needs_relogin` con instrucción de abrir `/pair`.
- Error de Synapse en `/sync`: reintento con espera exponencial hasta 60 segundos sin perder `next_batch`.
- Rutas de `send_file` fuera de `uploads/`, enlaces simbólicos o archivos mayores a 25 MiB: rechazo explícito.

## Pruebas

messenger-mcp, en Go con TDD:

- Unitarias: ingesta (inserción única, ediciones, redacciones, DM, propio), FTS5, `history_since` (orden, límites, cursor), máquina de estados de la cola, idempotencia, validación de rutas, token de `/pair`, redacción de credenciales en logs.
- Integración contra un Synapse temporal real: un usuario de prueba simula salas de puente con estado `m.bridge`, mensajes y eventos de estado de envío. Sin cuenta de Messenger.
- Protocolo MCP: `tools/list` y llamadas por HTTP.

Threads0x, con pytest: lector `messenger` contra un servidor MCP falso (avance de cursor, caída del MCP), sender (`sent`, `queued`, `needs_review`), enums.

Prueba final con la cuenta real: solo lectura (`get_status`, `list_chats`, `get_messages` de Sala Forum). Cualquier envío requiere autorización explícita.

## Riesgos

- Meta puede bloquear o marcar la cuenta por usar un cliente no oficial.
- Meta cambia su protocolo con frecuencia; la respuesta es actualizar el tag fijado de mautrix-meta y recompilar.
- Historial cifrado antiguo puede no estar disponible.
- La Mac nueva apagada o sin red deja la fuente caída; Threads0x lo debe mostrar.
