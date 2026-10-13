// Package daemon wires the ledger, Matrix sync, send queue, pairing page and
// MCP tools into the messenger-mcp service.
package daemon

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
)

type Config struct {
	DataDir        string
	Listen         string
	Homeserver     string
	UserID         string
	Password       string
	BridgeURL      string
	BridgeSecret   string
	BridgeBot      string
	InternalSecret string
	UploadsDir     string
	DownloadsDir   string
}

// BaseURL is the loopback URL the service is reachable on.
func (c Config) BaseURL() string { return "http://" + c.Listen }

func DefaultDataDir() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, "Library", "Application Support", "messenger-mcp")
}

func readEnv(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		v = strings.TrimSpace(v)
		if len(v) >= 2 && (v[0] == '"' || v[0] == '\'') && v[len(v)-1] == v[0] {
			v = v[1 : len(v)-1]
		}
		out[strings.TrimSpace(k)] = v
	}
	return out, sc.Err()
}

// LoadConfig reads <dataDir>/secrets.env, which must not be readable by other users.
func LoadConfig(dataDir string) (Config, error) {
	path := filepath.Join(dataDir, "secrets.env")
	info, err := os.Stat(path)
	if err != nil {
		return Config{}, fmt.Errorf("secrets: %w", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return Config{}, fmt.Errorf("%s must have mode 600 (it is %o)", path, info.Mode().Perm())
	}
	env, err := readEnv(path)
	if err != nil {
		return Config{}, err
	}
	get := func(k, def string) string {
		if v := env[k]; v != "" {
			return v
		}
		return def
	}
	c := Config{
		DataDir:        dataDir,
		Listen:         get("LISTEN", "127.0.0.1:7381"),
		Homeserver:     get("MATRIX_HOMESERVER", "http://127.0.0.1:7382"),
		UserID:         get("MATRIX_USER_ID", "@jesus:messenger.local"),
		Password:       env["MATRIX_PASSWORD"],
		BridgeURL:      get("BRIDGE_URL", "http://127.0.0.1:7383"),
		BridgeSecret:   env["BRIDGE_PROVISIONING_SECRET"],
		BridgeBot:      get("BRIDGE_BOT", "@metabot:messenger.local"),
		InternalSecret: env["INTERNAL_SECRET"],
		UploadsDir:     filepath.Join(dataDir, "uploads"),
		DownloadsDir:   filepath.Join(dataDir, "downloads"),
	}
	var missing []string
	for k, v := range map[string]string{"MATRIX_PASSWORD": c.Password, "BRIDGE_PROVISIONING_SECRET": c.BridgeSecret, "INTERNAL_SECRET": c.InternalSecret} {
		if v == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("%s is missing %s", path, strings.Join(missing, ", "))
	}
	host, _, err := net.SplitHostPort(c.Listen)
	if err != nil {
		return Config{}, fmt.Errorf("LISTEN: %w", err)
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		return Config{}, errors.New("LISTEN must be a loopback address such as 127.0.0.1:7381")
	}
	return c, nil
}
