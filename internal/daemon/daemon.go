package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/mark3labs/mcp-go/server"
	"maunium.net/go/mautrix/id"

	"github.com/0xjesus/messenger-mcp/internal/bridge"
	"github.com/0xjesus/messenger-mcp/internal/ingest"
	"github.com/0xjesus/messenger-mcp/internal/ledger"
	"github.com/0xjesus/messenger-mcp/internal/outbox"
	"github.com/0xjesus/messenger-mcp/internal/pair"
	"github.com/0xjesus/messenger-mcp/internal/tools"
)

const Version = "0.1.0"

// Run serves messenger-mcp until ctx is cancelled.
func Run(ctx context.Context, cfg Config, log *slog.Logger) error {
	for _, d := range []string{cfg.UploadsDir, cfg.DownloadsDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return err
		}
	}
	l, err := ledger.Open(filepath.Join(cfg.DataDir, "ledger.db"))
	if err != nil {
		return err
	}
	defer l.Close()

	mx, err := NewMatrix(cfg, l, log)
	if err != nil {
		return err
	}
	q, err := outbox.New(l, mx, outbox.Config{
		MinGap: 10 * time.Second, StatusTimeout: 120 * time.Second, DailyMax: 60,
		UploadsDir: cfg.UploadsDir, MaxFileBytes: 25 << 20,
	})
	if err != nil {
		return err
	}
	bc := &bridge.Client{BaseURL: cfg.BridgeURL, Secret: cfg.BridgeSecret, UserID: cfg.UserID}
	bot := id.UserID(cfg.BridgeBot)
	hs := id.UserID(cfg.UserID).Homeserver()

	var ownGhost atomic.Value // id.UserID of the linked account's ghost
	ownGhost.Store(id.UserID(""))
	var lastLogin atomic.Value
	lastLogin.Store(bridge.LoginStatus{State: bridge.StateUnreachable})
	refresh := func(ctx context.Context) bridge.LoginStatus {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		st, _ := bc.Status(c)
		if st.LoginID != "" {
			ownGhost.Store(id.NewUserID("meta_"+st.LoginID, hs))
		}
		lastLogin.Store(st)
		return st
	}

	mx.r = &router{
		ing: &ingest.Handler{
			L: l, OperatorID: id.UserID(cfg.UserID), BridgeBot: bot, Names: mx.Name,
			OwnGhosts: func() []id.UserID {
				if g := ownGhost.Load().(id.UserID); g != "" {
					return []id.UserID{g}
				}
				return nil
			},
		},
		onStatus: q.OnStatus,
		join:     func(ctx context.Context, room id.RoomID, inviter id.UserID) { mx.Join(ctx, room, inviter, bot) },
		log:      log,
	}

	mcpSrv := server.NewMCPServer("messenger", Version, server.WithToolCapabilities(true))
	tools.Register(mcpSrv, tools.Deps{
		L: l, Q: q, Media: mx, Reads: mx, DownloadsDir: cfg.DownloadsDir, UploadsDir: cfg.UploadsDir,
		Status: func(ctx context.Context) map[string]any {
			st := refresh(ctx)
			out := map[string]any{"matrix": mx.Status(), "messenger": st, "version": Version}
			switch st.State {
			case bridge.StateNotLoggedIn, bridge.StateNeedsRelogin:
				out["action"] = "Jesús debe vincular Messenger: en nodo-02 corre `messenger-mcp pair-url` y abre la liga."
			}
			return out
		},
	})

	tokens := pair.NewTokens(nil)
	mux := http.NewServeMux()
	mux.Handle("/mcp", server.NewStreamableHTTPServer(mcpSrv))
	ph := &pair.Handler{T: tokens, B: bc}
	mux.Handle("/pair", ph)
	mux.Handle("/pair/", ph)
	mux.Handle("/internal/pair-token", pairTokenHandler(cfg.InternalSecret, cfg.BaseURL(), tokens))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })

	_, port, _ := net.SplitHostPort(cfg.Listen)
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.Listen, err)
	}
	srv := &http.Server{Handler: loopbackOnly(port, mux), ReadHeaderTimeout: 10 * time.Second}

	go mx.Run(ctx)
	go q.Run(ctx)
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			refresh(ctx)
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(c)
	}()
	log.Info("messenger-mcp listening", "addr", cfg.Listen, "version", Version)
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
