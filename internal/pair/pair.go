// Package pair serves the local page where Jesús links Messenger himself.
// Credentials typed here are forwarded to the bridge in memory and never
// logged, stored or echoed back.
package pair

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/0xjesus/messenger-mcp/internal/bridge"
)

const cookieName = "mm_pair"

type Bridge interface {
	Flows(ctx context.Context) ([]bridge.Flow, error)
	Start(ctx context.Context, flowID string) (*bridge.Step, error)
	Step(ctx context.Context, loginID, stepID, stepType string, values map[string]string) (*bridge.Step, error)
	Cancel(ctx context.Context, loginID string) error
}

type Handler struct {
	T *Tokens
	B Bridge
}

var flowOrder = map[string]int{"messenger-lite-android": 0, "messenger-lite": 1, "messenger": 2, "facebook": 3}

func (h *Handler) authorized(w http.ResponseWriter, r *http.Request) bool {
	if c, err := r.Cookie(cookieName); err == nil && h.T.ValidSession(c.Value) {
		return true
	}
	if r.Method == http.MethodGet && r.URL.Path == "/pair" {
		if s, ok := h.T.Consume(r.URL.Query().Get("token")); ok {
			http.SetCookie(w, &http.Cookie{
				Name: cookieName, Value: s, Path: "/pair", HttpOnly: true,
				SameSite: http.SameSiteStrictMode, MaxAge: int(sessionTTL / time.Second),
			})
			return true
		}
	}
	w.WriteHeader(http.StatusForbidden)
	render(w, page{Title: "Liga vencida", Error: "Esta liga ya se usó o venció. Pide una nueva con: messenger-mcp pair-url"})
	return false
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	if !h.authorized(w, r) {
		return
	}
	ctx := r.Context()
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/pair":
		flows, err := h.B.Flows(ctx)
		if err != nil {
			render(w, page{Title: "Error", Error: err.Error()})
			return
		}
		sort.SliceStable(flows, func(i, j int) bool { return rank(flows[i].ID) < rank(flows[j].ID) })
		render(w, page{Title: "Vincular Messenger", Flows: flows})
	case r.Method == http.MethodPost && r.URL.Path == "/pair/start":
		step, err := h.B.Start(ctx, r.FormValue("flow"))
		h.renderStep(w, step, err)
	case r.Method == http.MethodPost && r.URL.Path == "/pair/step":
		h.submit(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/pair/cancel":
		if id := r.FormValue("login_id"); id != "" {
			_ = h.B.Cancel(ctx, id)
		}
		http.Redirect(w, r, "/pair", http.StatusSeeOther)
	default:
		http.NotFound(w, r)
	}
}

func rank(id string) int {
	if r, ok := flowOrder[id]; ok {
		return r
	}
	return len(flowOrder)
}

func (h *Handler) submit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		render(w, page{Title: "Error", Error: "Formulario inválido"})
		return
	}
	loginID, stepID, stepType := r.PostForm.Get("login_id"), r.PostForm.Get("step_id"), r.PostForm.Get("step_type")
	values := map[string]string{}
	switch stepType {
	case "user_input":
		for k, v := range r.PostForm {
			if name, ok := strings.CutPrefix(k, "f_"); ok && len(v) > 0 {
				values[name] = v[0]
			}
		}
	case "cookies":
		parsed, err := ParseCookies(r.PostForm.Get("cookies"))
		if err != nil {
			render(w, page{Title: "Error", Error: err.Error()})
			return
		}
		values = parsed
	case "display_and_wait":
	default:
		render(w, page{Title: "Error", Error: "Paso desconocido"})
		return
	}
	step, err := h.B.Step(r.Context(), loginID, stepID, stepType, values)
	h.renderStep(w, step, err)
}

func (h *Handler) renderStep(w http.ResponseWriter, step *bridge.Step, err error) {
	if err != nil {
		render(w, page{Title: "No se pudo continuar", Error: err.Error()})
		return
	}
	if step.Type == "complete" {
		render(w, page{Title: "Listo", Done: true})
		return
	}
	render(w, page{Title: "Vincular Messenger", Step: step})
}

var (
	curlHeader = regexp.MustCompile(`(?i)-H\s+['"]cookie:\s*([^'"]+)['"]`)
	curlFlag   = regexp.MustCompile(`(?:\s-b|--cookie)\s+['"]([^'"]+)['"]`)
)

// ParseCookies accepts a JSON object, a cURL command copied from devtools, or
// a raw "name=value; name2=value2" cookie header.
func ParseCookies(input string) (map[string]string, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil, errors.New("pega las cookies (JSON, comando cURL o encabezado Cookie)")
	}
	out := map[string]string{}
	if strings.HasPrefix(input, "{") {
		var raw map[string]any
		if err := json.Unmarshal([]byte(input), &raw); err != nil {
			return nil, fmt.Errorf("JSON inválido: %w", err)
		}
		for k, v := range raw {
			out[k] = fmt.Sprint(v)
		}
		return out, nil
	}
	header := input
	if m := curlHeader.FindStringSubmatch(input); m != nil {
		header = m[1]
	} else if m := curlFlag.FindStringSubmatch(input); m != nil {
		header = m[1]
	} else if strings.HasPrefix(input, "curl") {
		return nil, errors.New("el comando cURL no trae cookies")
	}
	for _, part := range strings.Split(header, ";") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if ok && k != "" {
			out[k] = v
		}
	}
	if len(out) == 0 {
		return nil, errors.New("no se encontraron cookies")
	}
	return out, nil
}

type page struct {
	Title string
	Error string
	Flows []bridge.Flow
	Step  *bridge.Step
	Done  bool
}

var inputTypes = map[string]string{"password": "password", "email": "email", "phone_number": "tel", "url": "url"}

var tmpl = template.Must(template.New("pair").Funcs(template.FuncMap{
	"inputType": func(t string) string {
		if v, ok := inputTypes[t]; ok {
			return v
		}
		return "text"
	},
}).Parse(`<!doctype html>
<html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{.Title}} · messenger-mcp</title>
<style>
body{font:16px/1.5 -apple-system,system-ui,sans-serif;max-width:32rem;margin:2rem auto;padding:0 1rem;color:#1c1c1e;background:#fafafa}
h1{font-size:1.4rem}form{margin:1rem 0}label{display:block;margin:.8rem 0 .2rem;font-weight:600}
input,select,textarea{width:100%;box-sizing:border-box;padding:.6rem;font:inherit;border:1px solid #c7c7cc;border-radius:8px;background:#fff}
textarea{min-height:8rem}button{margin-top:1rem;padding:.6rem 1rem;font:inherit;border:0;border-radius:8px;background:#0a7cff;color:#fff;cursor:pointer}
button.flow{display:block;width:100%;text-align:left;background:#fff;color:#1c1c1e;border:1px solid #c7c7cc}
button.secondary{background:#e5e5ea;color:#1c1c1e}.err{background:#ffe5e5;padding:.8rem;border-radius:8px}
.note{color:#636366;font-size:.9rem}pre{white-space:pre-wrap;background:#fff;padding:.8rem;border-radius:8px}
</style></head><body>
<h1>{{.Title}}</h1>
{{if .Error}}<p class="err">{{.Error}}</p><p><a href="/pair">Volver a empezar</a></p>{{end}}
{{if .Done}}<p>Listo. Messenger quedó vinculado. Ya puedes cerrar esta pestaña.</p>{{end}}
{{if .Flows}}<p>Elige cómo entrar. Recomendado: <b>Messenger Android</b> (usuario, contraseña y código).</p>
{{range .Flows}}<form method="post" action="/pair/start"><input type="hidden" name="flow" value="{{.ID}}">
<button class="flow" type="submit"><b>{{.Name}}</b><br><span class="note">{{.Description}}</span></button></form>{{end}}
<p class="note">Tus datos van directo al puente local en esta Mac; no se guardan ni se registran.</p>{{end}}
{{with .Step}}
{{if .Instructions}}<p>{{.Instructions}}</p>{{end}}
{{if eq .Type "user_input"}}<form method="post" action="/pair/step" autocomplete="off">
<input type="hidden" name="login_id" value="{{.LoginID}}"><input type="hidden" name="step_id" value="{{.StepID}}"><input type="hidden" name="step_type" value="user_input">
{{range .UserInput.Fields}}<label for="f_{{.ID}}">{{.Name}}</label>
{{if eq .Type "select"}}<select id="f_{{.ID}}" name="f_{{.ID}}">{{range .Options}}<option>{{.}}</option>{{end}}</select>
{{else}}<input id="f_{{.ID}}" name="f_{{.ID}}" type="{{inputType .Type}}" value="{{.Default}}" required>{{end}}
{{if .Description}}<div class="note">{{.Description}}</div>{{end}}{{end}}
<button type="submit">Continuar</button></form>{{end}}
{{if eq .Type "cookies"}}<form method="post" action="/pair/step">
<input type="hidden" name="login_id" value="{{.LoginID}}"><input type="hidden" name="step_id" value="{{.StepID}}"><input type="hidden" name="step_type" value="cookies">
{{with .Cookies}}<p class="note">Abre <a href="{{.URL}}" target="_blank" rel="noreferrer">{{.URL}}</a> con tu sesión iniciada y copia las cookies o un comando cURL desde las herramientas de desarrollador.</p>{{end}}
<label for="cookies">Cookies o comando cURL</label><textarea id="cookies" name="cookies" required></textarea>
<button type="submit">Continuar</button></form>{{end}}
{{if eq .Type "display_and_wait"}}{{with .DisplayAndWait}}{{if .Data}}<pre>{{.Data}}</pre>{{end}}{{if .ImageURL}}<img src="{{.ImageURL}}" alt="" style="max-width:100%">{{end}}{{end}}
<form method="post" action="/pair/step"><input type="hidden" name="login_id" value="{{.LoginID}}"><input type="hidden" name="step_id" value="{{.StepID}}"><input type="hidden" name="step_type" value="display_and_wait">
<button type="submit">Ya lo hice</button></form>{{end}}
<form method="post" action="/pair/cancel"><input type="hidden" name="login_id" value="{{.LoginID}}"><button class="secondary" type="submit">Cancelar</button></form>
{{end}}
</body></html>`))

func render(w http.ResponseWriter, p page) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = tmpl.Execute(w, p)
}
