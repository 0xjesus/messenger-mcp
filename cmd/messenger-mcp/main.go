// Command messenger-mcp exposes Jesús's personal Messenger as an MCP server
// through a local mautrix-meta bridge.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/0xjesus/messenger-mcp/internal/daemon"
)

const usage = `uso:
  messenger-mcp serve    [--data DIR]   arranca el servicio en 127.0.0.1:7381
  messenger-mcp pair-url [--data DIR]   imprime una liga /pair de un solo uso (15 min)
  messenger-mcp version`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	data := fs.String("data", daemon.DefaultDataDir(), "directorio de datos")
	_ = fs.Parse(os.Args[2:])

	switch cmd {
	case "serve":
		cfg, err := daemon.LoadConfig(*data)
		if err != nil {
			fail(err)
		}
		log := slog.New(slog.NewTextHandler(os.Stderr, nil))
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if err := daemon.Run(ctx, cfg, log); err != nil {
			fail(err)
		}
	case "pair-url":
		cfg, err := daemon.LoadConfig(*data)
		if err != nil {
			fail(err)
		}
		req, _ := http.NewRequest(http.MethodPost, cfg.BaseURL()+"/internal/pair-token", nil)
		req.Header.Set("Authorization", "Bearer "+cfg.InternalSecret)
		resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
		if err != nil {
			fail(fmt.Errorf("el servicio no responde en %s: %w", cfg.Listen, err))
		}
		defer resp.Body.Close()
		var out struct {
			URL       string `json:"url"`
			ExpiresIn string `json:"expires_in"`
		}
		if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&out) != nil {
			fail(fmt.Errorf("no se pudo crear la liga (HTTP %d)", resp.StatusCode))
		}
		fmt.Println(out.URL)
		fmt.Fprintf(os.Stderr, "Vence en %s y sirve una sola vez.\n", out.ExpiresIn)
	case "version":
		fmt.Println(daemon.Version)
	default:
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "messenger-mcp:", err)
	os.Exit(1)
}
