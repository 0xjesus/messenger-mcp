# messenger-mcp

Servidor MCP para leer y enviar mensajes del Messenger personal sin navegador.
Corre en una Mac con tres procesos locales:

| Proceso | Puerto (127.0.0.1) | Qué hace |
|---|---|---|
| Synapse | 7382 | Servidor Matrix privado (`messenger.local`), sin federación ni registro |
| mautrix-meta | 7383 | Puente open source entre Messenger y Matrix |
| messenger-mcp | 7381 | Sincroniza Matrix a un registro SQLite y expone las herramientas MCP en `/mcp` |

## Herramientas

`get_status`, `list_chats`, `get_messages`, `search_messages`, `get_message_context`,
`download_attachment`, `history_since`, `mark_read`, `send_message`, `send_file`,
`get_outbox`, `cancel_outbox`.

Los envíos pasan por una cola durable: al menos 10 s entre mensajes, máximo 60 por día,
`idempotency_key` para no duplicar y un resultado `needs_review` cuando Messenger no
confirma. Un mensaje en `needs_review` nunca se reenvía solo.

## Instalación (macOS)

Requisitos: Go 1.26+, Python 3.12 en `/usr/local/bin/python3.12`, `git`, `openssl`.

```sh
TUNNEL_TARGET=usuario@host ./scripts/install-macos.sh
```

`TUNNEL_TARGET` es opcional: publica `127.0.0.1:7381` en otra máquina por SSH inverso.
La llave `~/.ssh/messenger_tunnel_ed25519.pub` se autoriza allá con
`restrict,port-forwarding,permitlisten="127.0.0.1:7381",command="/bin/false"`.

El script es idempotente: genera secretos una sola vez en
`~/Library/Application Support/messenger-mcp/secrets.env` (modo 600), instala las
versiones de `deploy/versions.env` y registra agentes de launchd `com.0xjesus.messenger.*`.
Los registros quedan en `~/Library/Logs/messenger-mcp/`.

## Vincular la cuenta

```sh
messenger-mcp pair-url
```

Imprime una liga `http://127.0.0.1:7381/pair?token=…` de un solo uso que vence en 15 minutos.
La persona dueña de la cuenta escribe ahí sus datos; se pasan al puente en memoria y no se
guardan ni se registran.

## Seguridad

- Todo escucha en loopback y el servicio rechaza `Host` que no sea loopback (DNS rebinding).
- El contenido de los mensajes se trata como dato no confiable, nunca como instrucciones.
- No se versionan sesiones, cookies, tokens, bases de datos ni adjuntos.

## Licencia

MIT. mautrix-meta (AGPL-3.0) se usa como binario aparte, sin enlazarlo.
