package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"log"
	"math"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
)

type User struct {
	ID, Email, Name, Username     string
	Country                       string
	Verified, Admin, Banned, Hide bool
	Seen                          int64
	Following                     []string
	DOB                           string
	Agreed                        int64
	Bio                           string
	Saved                         []string
	Avatar, Cover                 string
	Blocked                       []string
}
type Comment struct {
	ID, Parent string
	User, Text string
	TS         int64
	Likes      []string
}
type Post struct {
	More                        []string
	Poll                        *Poll
	ID, User, Text, Media, Kind string
	TS                          int64
	Likes                       []string
	Comments                    []Comment
	Comm                        string
}
type Msg struct {
	From, To, Text, Kind, Media string
	TS                          int64
	Read                        bool
	ID, Reply                   string
	Edited                      bool
	React                       map[string]string
	Exp                         int64
	Rd                          bool
}
type Notif struct {
	To, From, Type, Post string
	TS                   int64
	Seen                 bool
}
type Group struct {
	ID, Name, Owner string
	Members         []string
	Msgs            []Msg
}
type Status struct {
	ID, User, Text, Media, Kind string
	TS                          int64
}
type ChPost struct {
	Text string
	TS   int64
}
type Channel struct {
	ID, Name, Desc, Owner string
	Subs                  []string
	Posts                 []ChPost
}
type Report struct {
	ID, Post, By, Reason string
	TS                   int64
}
type State struct {
	X        Extra
	X2       Ext2
	X3       Ext3
	X4       Ext4
	X5       Ext5
	X6       Ext6
	X7       Ext7
	X8       Ext8
	X9       Ext9
	X10      Ext10
	X11      Ext11
	X12      Ext12
	Users    map[string]*User
	Posts    []*Post
	Msgs     []Msg
	Notifs   []Notif
	Sess     map[string]string
	Groups   []*Group
	Statuses []*Status
	Channels []*Channel
	Reports  []*Report
	Notice   string
	SessTS   map[string]int64
	Log      []string
}

var (
	S   = State{Users: map[string]*User{}, Sess: map[string]string{}}
	mu  sync.Mutex
	env = os.Getenv
)

// ---------- storage: Neon (state), Supabase (media), or local state.json ----------
func sb(method, path string, body io.Reader, ct string) (*http.Response, error) {
	r, _ := http.NewRequest(method, env("SUPABASE_URL")+path, body)
	r.Header.Set("apikey", env("SUPABASE_KEY"))
	r.Header.Set("Authorization", "Bearer "+env("SUPABASE_KEY"))
	r.Header.Set("Prefer", "resolution=merge-duplicates")
	if ct != "" {
		r.Header.Set("Content-Type", ct)
	}
	if method == "POST" && strings.HasPrefix(path, "/storage/v1/object/media/") {
		r.Header.Set("Cache-Control", "max-age=31536000")
	}
	return hu.Do(r)
}

// Neon (Postgres) over HTTPS: no driver needed. Set DATABASE_URL to your Neon connection string.
func neon(q string, p ...any) ([]map[string]json.RawMessage, error) {
	u, e := url.Parse(env("DATABASE_URL"))
	if e != nil {
		return nil, e
	}
	if p == nil {
		p = []any{}
	}
	b, _ := json.Marshal(map[string]any{"query": q, "params": p})
	r, _ := http.NewRequest("POST", "https://"+u.Hostname()+"/sql", bytes.NewReader(b))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Neon-Connection-String", env("DATABASE_URL"))
	resp, e := hc.Do(r)
	if e != nil {
		return nil, e
	}
	defer resp.Body.Close()
	var o struct {
		Rows    []map[string]json.RawMessage
		Message string
	}
	json.NewDecoder(resp.Body).Decode(&o)
	if resp.StatusCode > 299 {
		return nil, errors.New(o.Message)
	}
	return o.Rows, nil
}

func load() {
	loaded := false
	if env("DATABASE_URL") != "" {
		neon("CREATE TABLE IF NOT EXISTS store(key text PRIMARY KEY, value jsonb NOT NULL)")
		for _, t := range rowTables {
			neon("CREATE TABLE IF NOT EXISTS " + t + "(n bigserial, id text PRIMARY KEY, ts bigint NOT NULL DEFAULT 0, data jsonb NOT NULL)")
		}
		if ok, err := loadTables(); err != nil {
			log.Fatal("cannot read the database, refusing to start so no data is overwritten: ", err)
		} else if ok {
			loaded = true
		} else if loadParts() {
			loaded = true
		} else if rows, e := neon("SELECT value::text AS v FROM store WHERE key='state'"); e == nil && len(rows) > 0 {
			raw := rows[0]["v"]
			if len(raw) > 0 && raw[0] == '"' {
				var t string
				json.Unmarshal(raw, &t)
				raw = json.RawMessage(t)
			}
			loaded = json.Unmarshal(raw, &S) == nil
		}
	}
	if !loaded && env("SUPABASE_URL") != "" { // one-time move of old data from Supabase
		if r, e := sb("GET", "/rest/v1/store?key=eq.state&select=value", nil, ""); e == nil {
			defer r.Body.Close()
			var a []struct{ Value State }
			json.NewDecoder(r.Body).Decode(&a)
			if len(a) > 0 {
				S = a[0].Value
			}
		}
	} else if !loaded && env("DATABASE_URL") == "" {
		b, _ := os.ReadFile("state.json")
		json.Unmarshal(b, &S)
	}
	if S.Users == nil {
		S.Users = map[string]*User{}
	}
	if S.Sess == nil {
		S.Sess = map[string]string{}
	}
	if S.SessTS == nil {
		S.SessTS = map[string]int64{}
	}
}

var saveCh = make(chan []byte, 1)

func saveOld() { // call while holding mu; only the newest state is written
	b, _ := json.Marshal(S)
	select {
	case <-saveCh:
	default:
	}
	saveCh <- b
}
func writerOld() {
	for b := range saveCh {
		if env("DATABASE_URL") != "" {
			neon("INSERT INTO store(key,value) VALUES('state',$1::jsonb) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value", string(b))
		} else {
			os.WriteFile("state.json", b, 0644)
		}
	}
}
func old13(d string) bool {
	t, e := time.Parse("2006-01-02", d)
	return e == nil && t.Year() > 1900 && !t.After(time.Now().AddDate(-13, 0, 0))
}

// ---------- helpers ----------
func uid() string {
	b := make([]byte, 12)
	rand.Read(b)
	return hex.EncodeToString(b)
}
func js(w http.ResponseWriter, v any) {
	if u, ok := v.(*User); ok {
		v = pubUser(u)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, c int, m string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(c)
	json.NewEncoder(w).Encode(map[string]string{"error": m})
}
func in(r *http.Request) map[string]string {
	m := map[string]string{}
	json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20)).Decode(&m)
	return m
}
func has(a []string, s string) bool {
	for _, x := range a {
		if x == s {
			return true
		}
	}
	return false
}
func del(a []string, s string) []string {
	o := []string{}
	for _, x := range a {
		if x != s {
			o = append(o, x)
		}
	}
	return o
}
func now() int64 { return time.Now().Unix() }
func note(to, from, typ, post string) {
	if to == "" || to == from {
		return
	}
	award(from, typ)
	if has(x().Mute[to], from) {
		return
	}
	if nOff(to, typ) {
		if typ == "msg" {
			sse(to, "msg")
		}
		return
	}
	S.Notifs = append(S.Notifs, Notif{to, from, typ, post, now(), false})
	push(to, typ)
	if len(S.Notifs) > 2000 {
		S.Notifs = S.Notifs[1000:]
	}
}
func uv(id string) map[string]any {
	u := S.Users[id]
	if u == nil {
		return map[string]any{"ID": id, "Username": "deleted"}
	}
	seen := u.Seen
	if u.Hide {
		seen = 0
	}
	m := map[string]any{"ID": u.ID, "Username": u.Username, "Verified": u.Verified, "On": now()-u.Seen < 120 && !u.Hide, "Seen": seen, "Av": u.Avatar, "Priv": x().Priv[u.ID], "Pin": x().Pin[u.ID]}
	if c := S.X6.Cos[u.ID]; c != nil {
		if c.Frame != "" {
			m["Fr"] = c.Frame
		}
		if c.Name != "" {
			m["Nc"] = c.Name
		}
		if c.Bub != "" {
			m["Bub"] = c.Bub
		}
		if c.Badge != "" {
			m["Bd"] = c.Badge
		}
	}
	return m
}
func hasExternalSocialLink(text string) bool {
	lower := strings.ToLower(text)
	for _, host := range []string{"tiktok.com", "vm.tiktok.com", "instagram.com", "instagr.am", "facebook.com", "fb.watch", "youtube.com", "youtu.be", "x.com", "twitter.com", "snapchat.com", "pinterest.com", "threads.net", "linkedin.com", "t.me", "telegram.me"} {
		if strings.Contains(lower, host) {
			return true
		}
	}
	return false
}

func pv(p *Post) map[string]any {
	cs := []map[string]any{}
	for _, c := range p.Comments {
		cs = append(cs, map[string]any{"ID": c.ID, "Parent": c.Parent, "U": uv(c.User), "Text": c.Text, "TS": c.TS, "Likes": c.Likes})
	}
	m := map[string]any{"ID": p.ID, "U": uv(p.User), "Text": p.Text, "Media": p.Media, "Kind": p.Kind, "TS": p.TS, "Likes": p.Likes, "C": cs, "More": p.More, "Poll": p.Poll, "Sens": x().Sens[p.ID], "Rep": repOf(p.ID), "Views": x2().Views[p.ID], "PinC": x2().PinC[p.ID], "Comm": p.Comm, "Opt": x3().PO[p.ID]}
	if o := x3().PO[p.ID]; o != nil && o.Duet != "" {
		if d := fp(o.Duet); d != nil {
			m["DuetU"] = uv(d.User)["Username"]
		}
	}
	return m
}
func fpOld(id string) *Post {
	for _, p := range S.Posts {
		if p.ID == id {
			return p
		}
	}
	return nil
}
func fg(id string) *Group {
	for _, g := range S.Groups {
		if g.ID == id {
			return g
		}
	}
	return nil
}
func fc(id string) *Channel {
	for _, c := range S.Channels {
		if c.ID == id {
			return c
		}
	}
	return nil
}
func okMedia(m string) bool { return okMedia2(m) }
func cut(s string) string {
	r := []rune(s)
	if len(r) > 30 {
		return string(r[:30])
	}
	return s
}
func api(path string, f func(u *User, w http.ResponseWriter, r *http.Request)) {
	http.HandleFunc("/api/"+path, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		u := S.Users[S.Sess[r.Header.Get("X-Token")]]
		if u == nil {
			fail(w, 401, "Please log in")
			return
		}
		tok := r.Header.Get("X-Token")
		if ts, k := S.SessTS[tok]; k && now()-ts > 2592000 {
			delete(S.Sess, tok)
			delete(S.SessTS, tok)
			fail(w, 401, "Session expired")
			return
		}
		if !rate(u.ID) {
			fail(w, 429, "Slow down a little")
			return
		}
		if u.Banned {
			fail(w, 403, "This account is banned")
			return
		}
		u.Seen = now()
		// Cloudflare's country header is a country-level signal (not GPS).
		// Only trust it when the app is behind Cloudflare; unknown is shown otherwise.
		if cc := strings.ToUpper(strings.TrimSpace(r.Header.Get("CF-IPCountry"))); len(cc) == 2 && cc != "XX" && cc != "T1" {
			u.Country = cc
		}
		f(u, w, r)
	})
}

func blk(a, b *User) bool { return has(a.Blocked, b.ID) || has(b.Blocked, a.ID) }

var rl = map[string][2]int64{}

func rate(id string) bool {
	t := now() / 60
	v := rl[id]
	if v[0] != t {
		v = [2]int64{t, 0}
	}
	v[1]++
	rl[id] = v
	return v[1] <= 150
}
func alog(u *User, s string) {
	S.Log = append(S.Log, time.Now().Format("2006-01-02 15:04")+" @"+u.Username+": "+s)
	if len(S.Log) > 2000 {
		S.Log = S.Log[1000:]
	}
}

func main() {
	load()
	fillIDs(true)
	fillCIDs()
	modHits = x5().Hits
	go writer()
	go janitor()
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		<-c
		mu.Lock()
		b, _ := json.Marshal(S)
		mu.Unlock()
		flush(b)
		os.Exit(0)
	}()
	admin := env("ADMIN_EMAIL")
	if admin == "" {
		admin = "admin-not-set@invalid.local"
	}
	ok := map[string]bool{"ok": true}
	extra(ok)
	extra2()
	extra3()
	extra4()

	// removes a user and everything that points at them
	purge := func(id string) {
		purgeMore(id)
		delete(S.Users, id)
		ps := S.Posts[:0]
		for _, p := range S.Posts {
			if p.User != id {
				ps = append(ps, p)
			}
		}
		S.Posts = ps
		for t, v := range S.Sess {
			if v == id {
				delete(S.Sess, t)
			}
		}
		for _, o := range S.Users {
			o.Following = del(o.Following, id)
		}
	}

	http.HandleFunc("/api/config", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		nt := S.Notice
		mu.Unlock()
		js(w, map[string]any{"Notice": nt, "Client": env("GOOGLE_CLIENT_ID"), "Fb": map[string]string{"apiKey": env("FIREBASE_API_KEY"), "authDomain": env("FIREBASE_AUTH_DOMAIN"), "projectId": env("FIREBASE_PROJECT_ID"), "appId": env("FIREBASE_APP_ID")}})
	})

	http.HandleFunc("/api/login", func(w http.ResponseWriter, r *http.Request) {
		resp, e := hc.Get("https://oauth2.googleapis.com/tokeninfo?id_token=" + url.QueryEscape(in(r)["credential"]))
		if e != nil {
			fail(w, 502, "Google check failed")
			return
		}
		defer resp.Body.Close()
		var t struct{ Aud, Email, Name, Sub, Email_verified string }
		json.NewDecoder(resp.Body).Decode(&t)
		if resp.StatusCode != 200 || t.Aud != env("GOOGLE_CLIENT_ID") || t.Sub == "" {
			fail(w, 401, "Google sign-in was rejected")
			return
		}
		mu.Lock()
		defer mu.Unlock()
		u := S.Users[t.Sub]
		if u == nil {
			u = &User{ID: t.Sub, Email: t.Email, Name: t.Name, Following: []string{}}
			S.Users[t.Sub] = u
			x().Born[u.ID] = now()
		}
		if t.Email == admin && t.Email_verified == "true" {
			u.Admin, u.Verified = true, true
		}
		tok := uid() + uid()
		S.Sess[tok] = t.Sub
		S.SessTS[tok] = now()
		xdev(tok, r)
		save()
		js(w, map[string]any{"token": tok, "me": pubUser(u)})
	})

	// Email sign-in: Firebase checks the password, we check the Firebase token.
	http.HandleFunc("/api/fblogin", func(w http.ResponseWriter, r *http.Request) {
		b := in(r)
		body, _ := json.Marshal(map[string]string{"idToken": b["token"]})
		resp, e := hc.Post("https://identitytoolkit.googleapis.com/v1/accounts:lookup?key="+url.QueryEscape(env("FIREBASE_API_KEY")), "application/json", bytes.NewReader(body))
		if e != nil {
			fail(w, 502, "Firebase check failed")
			return
		}
		defer resp.Body.Close()
		var t struct {
			Users []struct {
				LocalId, Email string
				EmailVerified  bool
			}
		}
		json.NewDecoder(resp.Body).Decode(&t)
		if resp.StatusCode != 200 || len(t.Users) == 0 {
			fail(w, 401, "Sign-in was rejected")
			return
		}
		f := t.Users[0]
		mu.Lock()
		defer mu.Unlock()
		var u *User
		if f.EmailVerified { // only link to an older account when the email is verified
			for _, o := range S.Users {
				if o.Email == f.Email {
					u = o
					break
				}
			}
		}
		if u == nil {
			u = S.Users[f.LocalId]
		}
		if u == nil {
			n := strings.TrimSpace(b["name"])
			if n == "" || rn(n) > 60 || !old13(b["dob"]) || b["terms"] != "1" {
				fail(w, 400, "Finish creating your account first")
				return
			}
			u = &User{ID: f.LocalId, Email: f.Email, Name: n, DOB: b["dob"], Agreed: now(), Following: []string{}}
			S.Users[u.ID] = u
			x().Born[u.ID] = now()
		}
		x().NoVer[u.ID] = !f.EmailVerified
		if f.EmailVerified && f.Email == admin {
			u.Admin, u.Verified = true, true
		}
		tok := uid() + uid()
		S.Sess[tok] = u.ID
		S.SessTS[tok] = now()
		xdev(tok, r)
		save()
		js(w, map[string]any{"token": tok, "me": pubUser(u)})
	})

	http.HandleFunc("/api/upload", func(w http.ResponseWriter, r *http.Request) {
		u := authUser(r)
		ct := r.Header.Get("Content-Type")
		if u == nil {
			fail(w, 401, "Please log in")
			return
		}
		ct = strings.Split(ct, ";")[0]
		ext, good := okTypes[ct]
		if !good {
			fail(w, 400, "Only images and videos")
			return
		}
		if !vidOK(u, ct, r.Header.Get("X-Scan")) {
			fail(w, 400, "This video needs a safety check first. Refresh the app and try again.")
			return
		}
		name := uid() + "." + ext
		data, good := readUpload(w, r, ct)
		if !good {
			return
		}
		if scanMedia(ct, data) {
			fail(w, 400, "This image breaks Amlink's content rules")
			return
		}
		resp, e := sb("POST", "/storage/v1/object/media/"+name, bytes.NewReader(data), ct)
		if e != nil || resp.StatusCode > 299 {
			fail(w, 500, "Upload failed")
			return
		}
		resp.Body.Close()
		js(w, map[string]string{"url": env("SUPABASE_URL") + "/storage/v1/object/public/media/" + name})
	})

	api("me", func(u *User, w http.ResponseWriter, r *http.Request) { js(w, u) })

	api("username", func(u *User, w http.ResponseWriter, r *http.Request) {
		n := strings.ToLower(strings.TrimSpace(in(r)["u"]))
		if len(n) < 3 || len(n) > 20 {
			fail(w, 400, "Use 3 to 20 characters")
			return
		}
		for _, c := range n {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_') {
				fail(w, 400, "Use letters, numbers and _ only")
				return
			}
		}
		for _, o := range S.Users {
			if o.Username == n && o.ID != u.ID {
				fail(w, 400, "That username is taken")
				return
			}
		}
		u.Username = n
		save()
		js(w, u)
	})

	api("feed", func(u *User, w http.ResponseWriter, r *http.Request) {
		out := []map[string]any{}
		n, _ := strconv.Atoi(r.URL.Query().Get("n"))
		if n < 60 || n > 1000 {
			n = 60
		}
		for i := len(S.Posts) - 1; i >= 0 && len(out) < n; i-- {
			if S.Posts[i].Comm != "" || S.X7.Flag[S.Posts[i].ID] {
				continue
			}
			if o := S.Users[S.Posts[i].User]; o != nil && (o.Banned || blk(u, o) || priv(u, o)) {
				continue
			}
			out = append(out, pv(S.Posts[i]))
		}
		js(w, out)
	})

	api("post", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		if x().NoVer[u.ID] {
			fail(w, 403, "Verify your email first (check your inbox), then log in again")
			return
		}
		if strings.TrimSpace(b["text"]) == "" && b["media"] == "" {
			fail(w, 400, "Write something or add a photo or video")
			return
		}
		if rn(b["text"]) > 2000 || !okMedia(b["media"]) {
			fail(w, 400, "Invalid post")
			return
		}
		k := b["kind"]
		if k != "image" && k != "video" {
			k = ""
		}
		S.Posts = append(S.Posts, &Post{ID: uid(), User: u.ID, Text: b["text"], Media: b["media"], Kind: k, TS: now(), Likes: []string{}})
		save()
		js(w, ok)
	})

	api("like", func(u *User, w http.ResponseWriter, r *http.Request) {
		p := fp(in(r)["id"])
		if p != nil && hid(u, p.User) {
			p = nil
		}
		if p == nil {
			fail(w, 404, "Post not found")
			return
		}
		if has(p.Likes, u.ID) {
			p.Likes = del(p.Likes, u.ID)
		} else {
			p.Likes = append(p.Likes, u.ID)
			note(p.User, u.ID, "like", p.ID)
		}
		save()
		js(w, pv(p))
	})

	api("clike", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		p := fp(b["id"])
		i, e := strconv.Atoi(b["i"])
		if p == nil || e != nil || i < 0 || i >= len(p.Comments) {
			fail(w, 400, "Invalid comment")
			return
		}
		c := &p.Comments[i]
		if has(c.Likes, u.ID) {
			c.Likes = del(c.Likes, u.ID)
		} else {
			c.Likes = append(c.Likes, u.ID)
		}
		save()
		js(w, pv(p))
	})

	api("search", func(u *User, w http.ResponseWriter, r *http.Request) {
		q := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
		out := []map[string]any{}
		for _, o := range S.Users {
			if o.ID == u.ID || o.Username == "" || o.Banned || blk(u, o) {
				continue
			}
			if (q == "" && has(u.Following, o.ID)) || (q != "" && (strings.Contains(o.Username, q) || strings.Contains(strings.ToLower(o.Name), q))) {
				out = append(out, uv(o.ID))
			}
		}
		js(w, out)
	})

	api("msgs", func(u *User, w http.ResponseWriter, r *http.Request) {
		p := r.URL.Query().Get("w")
		out, ch := []Msg{}, false
		for i := range S.Msgs {
			m := &S.Msgs[i]
			if (m.From == u.ID && m.To == p) || (m.From == p && m.To == u.ID) {
				if m.To == u.ID && !m.Read {
					m.Read, ch = true, true
				}
				out = append(out, *m)
			}
		}
		if ch {
			save()
		}
		js(w, out)
	})

	api("msg", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		t := strings.TrimSpace(b["text"])
		if S.Users[b["to"]] == nil || blk(u, S.Users[b["to"]]) || (t == "" && b["media"] == "") || rn(t) > 1000 || !okMedia(b["media"]) {
			fail(w, 400, "Invalid message")
			return
		}
		k := b["kind"]
		if k != "image" && k != "video" && k != "audio" {
			k = ""
		}
		S.Msgs = append(S.Msgs, Msg{From: u.ID, To: b["to"], Text: t, Kind: k, Media: b["media"], TS: now()})
		note(b["to"], u.ID, "msg", "")
		save()
		js(w, ok)
	})

	api("verify", func(u *User, w http.ResponseWriter, r *http.Request) {
		o := S.Users[in(r)["id"]]
		if !u.Admin || o == nil {
			fail(w, 403, "Admins only")
			return
		}
		o.Verified = !o.Verified
		alog(u, "toggled verified on @"+o.Username)
		save()
		js(w, uv(o.ID))
	})

	api("active", func(u *User, w http.ResponseWriter, r *http.Request) {
		u.Hide = !u.Hide
		save()
		js(w, u)
	})

	api("notifs", func(u *User, w http.ResponseWriter, r *http.Request) {
		list, n, mm := r.URL.Query().Get("c") == "", 0, 0
		out := []map[string]any{}
		for i := len(S.Notifs) - 1; i >= 0 && len(out) < 40; i-- {
			x := &S.Notifs[i]
			if x.To != u.ID {
				continue
			}
			if !x.Seen {
				n++
				if x.Type == "msg" {
					mm++
				}
			}
			if list {
				out = append(out, map[string]any{"U": uv(x.From), "Type": x.Type, "TS": x.TS, "New": !x.Seen})
				x.Seen = true
			}
		}
		if list {
			save()
		}
		js(w, map[string]any{"N": n, "M": mm, "L": out})
	})

	// ---------- Admin ----------
	api("admin", func(u *User, w http.ResponseWriter, r *http.Request) {
		if !u.Admin {
			fail(w, 403, "Admins only")
			return
		}
		us := []map[string]any{}
		on, bn, vr, rv := 0, 0, 0, 0
		for _, o := range S.Users {
			m := uv(o.ID)
			m["Banned"], m["Email"], m["Admin"], m["Country"] = o.Banned, o.Email, o.Admin, o.Country
			m["Mod"] = S.X7.Mods[o.ID]
			us = append(us, m)
			if now()-o.Seen < 120 && !o.Hide {
				on++
			}
			if o.Banned {
				bn++
			}
			if o.Verified {
				vr++
			}
		}
		pl := []map[string]any{}
		for i := len(S.Posts) - 1; i >= 0; i-- {
			if S.Posts[i].Kind == "video" {
				rv++
			}
			if len(pl) < 40 {
				pl = append(pl, pv(S.Posts[i]))
			}
		}
		rp := []map[string]any{}
		for _, x := range S.Reports {
			m := map[string]any{"ID": x.ID, "By": uv(x.By), "Reason": x.Reason, "TS": x.TS}
			if p := fp(x.Post); p != nil {
				m["P"] = pv(p)
			}
			rp = append(rp, m)
		}
		countries := map[string]int{}
		for _, o := range S.Users {
			if o.Country != "" {
				countries[o.Country]++
			}
		}
		js(w, map[string]any{"Countries": countries, "Users": len(S.Users), "Posts": len(S.Posts), "Msgs": len(S.Msgs), "L": us, "Online": on, "Banned": bn, "Verified": vr, "Reels": rv, "Groups": len(S.Groups), "Channels": len(S.Channels), "Moments": len(S.Statuses), "PL": pl, "R": rp, "Notice": S.Notice, "Log": S.Log})
	})

	api("ban", func(u *User, w http.ResponseWriter, r *http.Request) {
		o := S.Users[in(r)["id"]]
		if !u.Admin || o == nil || o.Admin {
			fail(w, 403, "Not allowed")
			return
		}
		o.Banned = !o.Banned
		alog(u, "toggled ban on @"+o.Username)
		save()
		js(w, map[string]bool{"Banned": o.Banned})
	})

	api("adminrole", func(u *User, w http.ResponseWriter, r *http.Request) {
		o := S.Users[in(r)["id"]]
		if !u.Admin || o == nil || o.ID == u.ID || o.Email == admin {
			fail(w, 403, "Not allowed")
			return
		}
		o.Admin = !o.Admin
		alog(u, "toggled admin on @"+o.Username)
		if o.Admin {
			o.Verified = true
		}
		save()
		js(w, map[string]bool{"Admin": o.Admin})
	})

	api("deluser", func(u *User, w http.ResponseWriter, r *http.Request) {
		o := S.Users[in(r)["id"]]
		if !u.Admin || o == nil || o.Admin {
			fail(w, 403, "Not allowed")
			return
		}
		alog(u, "deleted user @"+o.Username)
		purge(o.ID)
		save()
		js(w, ok)
	})

	api("notice", func(u *User, w http.ResponseWriter, r *http.Request) {
		t := strings.TrimSpace(in(r)["text"])
		if !u.Admin || rn(t) > 200 {
			fail(w, 403, "Not allowed")
			return
		}
		S.Notice = t
		alog(u, "updated the notice")
		save()
		js(w, ok)
	})

	api("repclr", func(u *User, w http.ResponseWriter, r *http.Request) {
		if !u.Admin {
			fail(w, 403, "Admins only")
			return
		}
		id := in(r)["id"]
		keep := S.Reports[:0]
		for _, x := range S.Reports {
			if x.ID != id {
				keep = append(keep, x)
			}
		}
		S.Reports = keep
		save()
		js(w, ok)
	})

	api("delpost", func(u *User, w http.ResponseWriter, r *http.Request) {
		id := in(r)["id"]
		for i, p := range S.Posts {
			if p.ID == id {
				if p.User != u.ID && !u.Admin && !commMod(p.Comm, u.ID) {
					fail(w, 403, "Not allowed")
					return
				}
				dropFiles(append([]string{p.Media}, p.More...)...)
				if o := x3().PO[p.ID]; o != nil {
					dropFiles(o.Poster)
					delete(x3().PO, p.ID)
				}
				S.Posts = append(S.Posts[:i], S.Posts[i+1:]...)
				save()
				js(w, ok)
				return
			}
		}
		fail(w, 404, "Post not found")
	})

	// ---------- Saved posts, reports, account deletion ----------
	api("save", func(u *User, w http.ResponseWriter, r *http.Request) {
		id := in(r)["id"]
		if fp(id) == nil {
			fail(w, 404, "Post not found")
			return
		}
		if has(u.Saved, id) {
			u.Saved = del(u.Saved, id)
		} else {
			u.Saved = append(u.Saved, id)
		}
		save()
		js(w, u)
	})
	api("saved", func(u *User, w http.ResponseWriter, r *http.Request) {
		out := []map[string]any{}
		for _, id := range u.Saved {
			if p := fp(id); p != nil {
				out = append(out, pv(p))
			}
		}
		js(w, out)
	})
	api("report", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		p := fp(b["id"])
		if p == nil || p.User == u.ID {
			fail(w, 400, "You cannot report this post")
			return
		}
		for _, x := range S.Reports {
			if x.Post == p.ID && x.By == u.ID {
				js(w, ok)
				return
			}
		}
		why := strings.TrimSpace(b["reason"])
		if len(why) > 200 {
			why = trunc(why, 200)
		}
		S.Reports = append(S.Reports, &Report{uid(), p.ID, u.ID, why, now()})
		autoFlag(p.ID)
		save()
		js(w, ok)
	})
	api("delme", func(u *User, w http.ResponseWriter, r *http.Request) {
		if u.Admin {
			fail(w, 403, "Admins cannot delete their own account here")
			return
		}
		purge(u.ID)
		save()
		js(w, ok)
	})

	// ---------- Profiles, friends and online status ----------
	api("profile", func(u *User, w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		if id == "" {
			id = u.ID
		}
		o := S.Users[id]
		if o == nil {
			fail(w, 404, "User not found")
			return
		}
		if (o.Banned && !u.Admin) || (id != u.ID && blk(u, o)) {
			fail(w, 404, "User not found")
			return
		}
		fol, lk, ps := 0, 0, []map[string]any{}
		for _, x := range S.Users {
			if has(x.Following, id) {
				fol++
			}
		}
		for i := len(S.Posts) - 1; i >= 0; i-- {
			if p := S.Posts[i]; p.User == id && p.Comm == "" && !priv(u, o) {
				lk += len(p.Likes)
				if len(ps) < 60 {
					ps = append(ps, pv(p))
				}
			}
		}
		js(w, map[string]any{"U": uv(id), "Name": o.Name, "Bio": o.Bio, "Following": len(o.Following), "Followers": fol, "Likes": lk, "Posts": ps, "IsMe": id == u.ID, "Cover": o.Cover, "Blocked": has(u.Blocked, id)})
	})
	api("bio", func(u *User, w http.ResponseWriter, r *http.Request) {
		bd := in(r)
		b := strings.TrimSpace(bd["bio"])
		if rn(b) > 120 {
			fail(w, 400, "Bio is too long (120 characters max)")
			return
		}
		if n := strings.TrimSpace(bd["name"]); n != "" && rn(n) <= 60 {
			u.Name = n
		}
		u.Bio = b
		if v, k := bd["avatar"]; k && okMedia(v) {
			u.Avatar = v
		}
		if v, k := bd["cover"]; k && okMedia(v) {
			u.Cover = v
		}
		save()
		js(w, u)
	})
	api("friends", func(u *User, w http.ResponseWriter, r *http.Request) {
		out := []map[string]any{}
		for _, o := range S.Users {
			if o.ID == u.ID || o.Username == "" || o.Banned || blk(u, o) {
				continue
			}
			if (now()-o.Seen < 120 && !o.Hide) || has(u.Following, o.ID) {
				out = append(out, uv(o.ID))
			}
		}
		js(w, out)
	})

	// ---------- Moments (24h status) ----------
	api("status", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		k := b["kind"]
		if k != "image" && k != "video" {
			k = ""
		}
		if (strings.TrimSpace(b["text"]) == "" && b["media"] == "") || rn(b["text"]) > 300 || !okMedia(b["media"]) {
			fail(w, 400, "Add text, a photo or a video")
			return
		}
		S.Statuses = append(S.Statuses, &Status{uid(), u.ID, b["text"], b["media"], k, now()})
		save()
		js(w, ok)
	})
	api("statuses", func(u *User, w http.ResponseWriter, r *http.Request) {
		keep, out := S.Statuses[:0], []map[string]any{}
		for _, s := range S.Statuses {
			if now()-s.TS > 86400 {
				continue
			}
			keep = append(keep, s)
			if s.User == u.ID || has(u.Following, s.User) {
				out = append(out, map[string]any{"ID": s.ID, "U": uv(s.User), "Text": s.Text, "Media": s.Media, "Kind": s.Kind, "TS": s.TS})
			}
		}
		S.Statuses = keep
		js(w, out)
	})

	// ---------- Circles (group chats) ----------
	api("gcreate", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		n, m := strings.TrimSpace(b["name"]), []string{u.ID}
		for _, id := range strings.Split(b["members"], ",") {
			if S.Users[id] != nil && !has(m, id) {
				m = append(m, id)
			}
		}
		if n == "" || rn(n) > 40 || len(m) < 2 {
			fail(w, 400, "Name the circle and add at least one person")
			return
		}
		g := &Group{ID: uid(), Name: n, Owner: u.ID, Members: m}
		S.Groups = append(S.Groups, g)
		save()
		js(w, map[string]string{"id": g.ID})
	})
	api("groups", func(u *User, w http.ResponseWriter, r *http.Request) {
		out := []map[string]any{}
		for _, g := range S.Groups {
			if has(g.Members, u.ID) {
				l := ""
				if n := len(g.Msgs); n > 0 {
					l = cut(g.Msgs[n-1].Text)
				}
				out = append(out, map[string]any{"ID": g.ID, "Name": g.Name, "N": len(g.Members), "Last": l})
			}
		}
		js(w, out)
	})
	api("gmsgs", func(u *User, w http.ResponseWriter, r *http.Request) {
		g := fg(r.URL.Query().Get("g"))
		if g == nil || !has(g.Members, u.ID) {
			fail(w, 403, "Not in this circle")
			return
		}
		out, st := []map[string]any{}, len(g.Msgs)-200
		if st < 0 {
			st = 0
		}
		for _, m := range g.Msgs[st:] {
			out = append(out, map[string]any{"U": uv(m.From), "Me": m.From == u.ID, "Text": m.Text, "TS": m.TS})
		}
		js(w, out)
	})
	api("gmsg", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		g, t := fg(b["g"]), strings.TrimSpace(b["text"])
		if g == nil || !has(g.Members, u.ID) || t == "" || rn(t) > 1000 {
			fail(w, 400, "Invalid message")
			return
		}
		g.Msgs = append(g.Msgs, Msg{From: u.ID, To: g.ID, Text: t, TS: now()})
		for _, m := range g.Members {
			note(m, u.ID, "msg", "")
		}
		save()
		js(w, ok)
	})

	// ---------- Broadcasts (channels) ----------
	api("chcreate", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		n := strings.TrimSpace(b["name"])
		if n == "" || rn(n) > 40 || rn(b["desc"]) > 200 {
			fail(w, 400, "Give your broadcast a name")
			return
		}
		S.Channels = append(S.Channels, &Channel{ID: uid(), Name: n, Desc: b["desc"], Owner: u.ID, Subs: []string{u.ID}})
		save()
		js(w, ok)
	})
	api("channels", func(u *User, w http.ResponseWriter, r *http.Request) {
		if id := r.URL.Query().Get("id"); id != "" {
			c := fc(id)
			if c == nil {
				fail(w, 404, "Not found")
				return
			}
			ps := []ChPost{}
			for i := len(c.Posts) - 1; i >= 0; i-- {
				ps = append(ps, c.Posts[i])
			}
			js(w, map[string]any{"Name": c.Name, "Desc": c.Desc, "Mine": c.Owner == u.ID, "Posts": ps})
			return
		}
		out := []map[string]any{}
		for _, c := range S.Channels {
			l := ""
			if n := len(c.Posts); n > 0 {
				l = cut(c.Posts[n-1].Text)
			}
			out = append(out, map[string]any{"ID": c.ID, "Name": c.Name, "N": len(c.Subs), "Sub": has(c.Subs, u.ID), "Mine": c.Owner == u.ID, "Last": l})
		}
		js(w, out)
	})
	api("chsub", func(u *User, w http.ResponseWriter, r *http.Request) {
		c := fc(in(r)["id"])
		if c == nil || c.Owner == u.ID {
			fail(w, 400, "Invalid broadcast")
			return
		}
		if has(c.Subs, u.ID) {
			c.Subs = del(c.Subs, u.ID)
		} else {
			c.Subs = append(c.Subs, u.ID)
		}
		save()
		js(w, ok)
	})
	api("chpost", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		c, t := fc(b["id"]), strings.TrimSpace(b["text"])
		if c == nil || c.Owner != u.ID || t == "" || rn(t) > 500 {
			fail(w, 400, "Only the owner can post an update")
			return
		}
		c.Posts = append(c.Posts, ChPost{t, now()})
		for _, s := range c.Subs {
			note(s, u.ID, "msg", "")
		}
		save()
		js(w, ok)
	})

	api("onep", func(u *User, w http.ResponseWriter, r *http.Request) {
		p := fp(r.URL.Query().Get("id"))
		if p != nil && hid(u, p.User) {
			p = nil
		}
		if p == nil {
			fail(w, 404, "Post not found")
			return
		}
		js(w, pv(p))
	})
	api("editpost", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		p, t := fp(b["id"]), strings.TrimSpace(b["text"])
		if p == nil || p.User != u.ID || rn(t) > 2000 || (t == "" && p.Media == "") {
			fail(w, 400, "Cannot edit this post")
			return
		}
		p.Text = t
		save()
		js(w, pv(p))
	})
	api("delcomment", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		p := fp(b["id"])
		i, e := strconv.Atoi(b["i"])
		if p == nil || e != nil || i < 0 || i >= len(p.Comments) {
			fail(w, 400, "Invalid comment")
			return
		}
		if p.Comments[i].User != u.ID && p.User != u.ID && !u.Admin {
			fail(w, 403, "Not allowed")
			return
		}
		p.Comments = append(p.Comments[:i], p.Comments[i+1:]...)
		save()
		js(w, pv(p))
	})
	api("explore", func(u *User, w http.ResponseWriter, r *http.Request) {
		q := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
		out := []map[string]any{}
		for i := len(S.Posts) - 1; i >= 0 && len(out) < 40; i-- {
			p := S.Posts[i]
			if p.Comm != "" {
				continue
			}
			if o := S.Users[p.User]; o != nil && (o.Banned || blk(u, o) || priv(u, o)) {
				continue
			}
			if q == "" || strings.Contains(strings.ToLower(p.Text), q) {
				out = append(out, pv(p))
			}
		}
		js(w, out)
	})
	api("block", func(u *User, w http.ResponseWriter, r *http.Request) {
		id := in(r)["id"]
		if id == u.ID || S.Users[id] == nil {
			fail(w, 400, "Invalid user")
			return
		}
		if has(u.Blocked, id) {
			u.Blocked = del(u.Blocked, id)
		} else {
			u.Blocked = append(u.Blocked, id)
			u.Following = del(u.Following, id)
		}
		save()
		js(w, u)
	})
	api("flist", func(u *User, w http.ResponseWriter, r *http.Request) {
		id, t := r.URL.Query().Get("id"), r.URL.Query().Get("t")
		o := S.Users[id]
		if o == nil {
			fail(w, 404, "User not found")
			return
		}
		out := []map[string]any{}
		for _, x := range S.Users {
			if x.Username == "" || blk(u, x) {
				continue
			}
			if (t == "following" && has(o.Following, x.ID)) || (t != "following" && has(x.Following, id)) {
				out = append(out, uv(x.ID))
			}
		}
		js(w, out)
	})
	api("inbox", func(u *User, w http.ResponseWriter, r *http.Request) {
		type cv struct {
			id   string
			last Msg
			un   int
		}
		m := map[string]*cv{}
		for _, x := range S.Msgs {
			if x.From != u.ID && x.To != u.ID {
				continue
			}
			p := x.To
			if x.From != u.ID {
				p = x.From
			}
			c := m[p]
			if c == nil {
				c = &cv{id: p}
				m[p] = c
			}
			c.last = x
			if x.To == u.ID && !x.Read {
				c.un++
			}
		}
		list := []*cv{}
		for _, c := range m {
			if o := S.Users[c.id]; o != nil && !blk(u, o) {
				list = append(list, c)
			}
		}
		sort.Slice(list, func(a, b int) bool { return list[a].last.TS > list[b].last.TS })
		out := []map[string]any{}
		for _, c := range list {
			l := cut(c.last.Text)
			if l == "" {
				l = "Media"
			}
			out = append(out, map[string]any{"U": uv(c.id), "Last": l, "TS": c.last.TS, "Un": c.un, "Mine": c.last.From == u.ID})
		}
		js(w, out)
	})
	api("export", func(u *User, w http.ResponseWriter, r *http.Request) {
		mine := []map[string]any{}
		for _, p := range S.Posts {
			if p.User == u.ID {
				mine = append(mine, pv(p))
			}
		}
		js(w, map[string]any{"Account": u, "Posts": mine})
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		if strings.HasPrefix(p, "/google") && strings.HasSuffix(p, ".html") && !strings.Contains(p[1:], "/") {
			http.ServeFile(w, r, p[1:])
			return
		}
		if xstatic(w, r) {
			return
		}
		if p != "/" {
			http.NotFound(w, r)
			return
		}
		serveIndex(w, r)
	})

	port := env("PORT")
	if port == "" {
		port = "8080"
	}
	(&http.Server{Addr: ":" + port, Handler: edge(modMW(http.DefaultServeMux)), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute}).ListenAndServe()
}

// ---------- v4 additions ----------

type Poll struct {
	Opts  []string
	Votes map[string]int
}
type URep struct {
	ID, Kind, Target, Post, By, Reason, Text string
	Idx                                      int
	TS                                       int64
}
type Extra struct {
	Priv   map[string]bool
	Req    map[string][]string
	Pin    map[string]string
	Mute   map[string][]string
	GAdm   map[string][]string
	MView  map[string][]string
	MReact map[string]map[string]string
	Dev    map[string]*Dev
	NoVer  map[string]bool
	Born   map[string]int64
	Susp   map[string]int64
	Sens   map[string]bool
	Words  []string
	URep   []*URep
}

var hub = map[string][]chan string{}
var atRe = regexp.MustCompile(`@(\w{3,20})`)
var tagRe = regexp.MustCompile(`#\w+`)

func x() *Extra {
	e := &S.X
	if e.Priv == nil {
		e.Priv = map[string]bool{}
	}
	if e.Req == nil {
		e.Req = map[string][]string{}
	}
	if e.Pin == nil {
		e.Pin = map[string]string{}
	}
	if e.Mute == nil {
		e.Mute = map[string][]string{}
	}
	if e.GAdm == nil {
		e.GAdm = map[string][]string{}
	}
	if e.MView == nil {
		e.MView = map[string][]string{}
	}
	if e.Dev == nil {
		e.Dev = map[string]*Dev{}
	}
	if e.NoVer == nil {
		e.NoVer = map[string]bool{}
	}
	if e.Born == nil {
		e.Born = map[string]int64{}
	}
	if e.Susp == nil {
		e.Susp = map[string]int64{}
	}
	if e.Sens == nil {
		e.Sens = map[string]bool{}
	}
	if e.MReact == nil {
		e.MReact = map[string]map[string]string{}
	}
	return e
}
func push(to, ev string) {
	if strings.HasPrefix(ev, "game:") || strings.HasPrefix(ev, "match:") {
		webPushTo(to, "yourmove")
	} else {
		webPushTo(to, ev)
	}
	for _, c := range hub[to] {
		select {
		case c <- ev:
		default:
		}
	}
}
func priv(v, o *User) bool {
	return o != nil && v.ID != o.ID && x().Priv[o.ID] && !has(v.Following, o.ID)
}

var xfiles = map[string][2]string{
	"/sw.js":         {"text/javascript", swJS},
	"/manifest.json": {"application/manifest+json", `{"name":"Amlink","short_name":"Amlink","start_url":"/","scope":"/","display":"standalone","background_color":"#000000","theme_color":"#000000","icons":[{"src":"/icon.svg","sizes":"any","type":"image/svg+xml","purpose":"any maskable"}]}`},
	"/icon.svg":      {"image/svg+xml", `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#ffd978"/><stop offset=".35" stop-color="#8fe3ff"/><stop offset=".7" stop-color="#e3a6ff"/><stop offset="1" stop-color="#ffd978"/></linearGradient></defs><rect width="512" height="512" fill="#000"/><text x="256" y="350" font-family="Arial,sans-serif" font-size="300" font-weight="700" text-anchor="middle" fill="url(#g)">A</text></svg>`},
}

func xstatic(w http.ResponseWriter, r *http.Request) bool {
	f, k := xfiles[r.URL.Path]
	if !k {
		return false
	}
	w.Header().Set("Content-Type", f[0])
	w.Write([]byte(f[1]))
	return true
}
func mentions(u *User, text, post string) {
	seen := map[string]bool{}
	for _, m := range atRe.FindAllStringSubmatch(strings.ToLower(text), 8) {
		for _, o := range S.Users {
			if o.Username == m[1] && !seen[o.ID] && !blk(u, o) {
				seen[o.ID] = true
				note(o.ID, u.ID, "mention", post)
			}
		}
	}
}
func fst(id string) *Status {
	for _, s := range S.Statuses {
		if s.ID == id {
			return s
		}
	}
	return nil
}

func extra(ok map[string]bool) {
	// ---- posts: polls, multiple photos, mentions ----
	api("post2", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		t := strings.TrimSpace(b["text"])
		if x().NoVer[u.ID] {
			fail(w, 403, "Verify your email first (check your inbox), then log in again")
			return
		}
		var media []string
		for _, m := range strings.Split(b["media"], ",") {
			if m != "" {
				if !okMedia(m) {
					fail(w, 400, "Invalid post")
					return
				}
				media = append(media, m)
			}
		}
		var pl *Poll
		if b["poll"] != "" {
			var o []string
			for _, s := range strings.Split(b["poll"], "|") {
				if s = strings.TrimSpace(s); s != "" && rn(s) <= 40 {
					o = append(o, s)
				}
			}
			if len(o) < 2 || len(o) > 4 {
				fail(w, 400, "A poll needs 2 to 4 options")
				return
			}
			pl = &Poll{o, map[string]int{}}
		}
		if (t == "" && len(media) == 0 && pl == nil) || rn(t) > 2000 || len(media) > 10 {
			fail(w, 400, "Invalid post")
			return
		}
		k := b["kind"]
		if k != "image" && k != "video" {
			k = ""
		}
		p := &Post{ID: uid(), User: u.ID, Text: t, Kind: k, TS: now(), Likes: []string{}, Poll: pl}
		if len(media) > 0 {
			p.Media, p.More = media[0], media[1:]
		}
		S.Posts = append(S.Posts, p)
		if b["sens"] == "1" {
			x().Sens[p.ID] = true
		}
		mentions(u, t, p.ID)
		save()
		js(w, ok)
	})
	api("vote", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		p := fp(b["id"])
		i, e := strconv.Atoi(b["i"])
		if p == nil || p.Poll == nil || e != nil || i < 0 || i >= len(p.Poll.Opts) || hid(u, p.User) {
			fail(w, 400, "Invalid vote")
			return
		}
		if p.Poll.Votes == nil {
			p.Poll.Votes = map[string]int{}
		}
		if _, v := p.Poll.Votes[u.ID]; !v {
			p.Poll.Votes[u.ID] = i
			save()
		}
		js(w, pv(p))
	})
	api("comment2", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		p, t := fp(b["id"]), strings.TrimSpace(b["text"])
		if p == nil || t == "" || rn(t) > 300 || hid(u, p.User) {
			fail(w, 400, "Invalid comment")
			return
		}
		p.Comments = append(p.Comments, Comment{ID: uid(), User: u.ID, Text: t, TS: now()})
		note(p.User, u.ID, "comment", p.ID)
		mentions(u, t, p.ID)
		save()
		js(w, pv(p))
	})
	api("byname", func(u *User, w http.ResponseWriter, r *http.Request) {
		n := strings.ToLower(in(r)["u"])
		for _, o := range S.Users {
			if o.Username == n && n != "" {
				js(w, map[string]string{"id": o.ID})
				return
			}
		}
		fail(w, 404, "User not found")
	})

	// ---- notifications that know who/what to open ----
	api("notifs2", func(u *User, w http.ResponseWriter, r *http.Request) {
		out := []map[string]any{}
		for i := len(S.Notifs) - 1; i >= 0 && len(out) < 40; i-- {
			n := &S.Notifs[i]
			if n.To != u.ID {
				continue
			}
			out = append(out, map[string]any{"U": uv(n.From), "Type": n.Type, "Post": n.Post, "TS": n.TS, "New": !n.Seen})
			n.Seen = true
		}
		save()
		js(w, out)
	})
	api("nclear", func(u *User, w http.ResponseWriter, r *http.Request) {
		keep := S.Notifs[:0]
		for _, n := range S.Notifs {
			if n.To != u.ID {
				keep = append(keep, n)
			}
		}
		S.Notifs = keep
		save()
		js(w, ok)
	})

	// ---- real-time: server-sent events + typing ----
	http.HandleFunc("/api/stream", func(w http.ResponseWriter, r *http.Request) {
		fl, can := w.(http.Flusher)
		mu.Lock()
		id := takeTicket(r.URL.Query().Get("k"))
		mu.Unlock()
		if id == "" || !can {
			http.Error(w, "no", 401)
			return
		}
		ch := make(chan string, 64)
		mu.Lock()
		hub[id] = append(hub[id], ch)
		mu.Unlock()
		defer func() {
			mu.Lock()
			l := hub[id]
			for i, c := range l {
				if c == ch {
					hub[id] = append(l[:i:i], l[i+1:]...)
					break
				}
			}
			mu.Unlock()
		}()
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no")
		fmt.Fprint(w, ": hi\n\n")
		fl.Flush()
		tk := time.NewTicker(25 * time.Second)
		defer tk.Stop()
		for {
			select {
			case m := <-ch:
				fmt.Fprintf(w, "data: %s\n\n", m)
				fl.Flush()
			case <-tk.C:
				mu.Lock()
				if o := S.Users[id]; o != nil {
					if o.Banned {
						mu.Unlock()
						return
					}
					o.Seen = now()
				}
				mu.Unlock()
				fmt.Fprint(w, ": p\n\n")
				fl.Flush()
			case <-r.Context().Done():
				return
			}
		}
	})
	api("typing", func(u *User, w http.ResponseWriter, r *http.Request) {
		if o := S.Users[in(r)["to"]]; o != nil && !blk(u, o) {
			push(o.ID, "typing:"+u.ID)
		}
		js(w, ok)
	})
	api("unsend", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		ts, _ := strconv.ParseInt(b["ts"], 10, 64)
		for i, m := range S.Msgs {
			if m.From == u.ID && m.To == b["to"] && m.TS == ts {
				S.Msgs = append(S.Msgs[:i], S.Msgs[i+1:]...)
				break
			}
		}
		save()
		js(w, ok)
	})

	// ---- moments: views, reactions, replies ----
	api("mview", func(u *User, w http.ResponseWriter, r *http.Request) {
		id := in(r)["id"]
		if s := fst(id); s != nil && s.User != u.ID && !has(x().MView[id], u.ID) {
			x().MView[id] = append(x().MView[id], u.ID)
			save()
		}
		js(w, ok)
	})
	api("mreact", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		s := fst(b["id"])
		if s == nil || s.User == u.ID || !strings.Contains("❤️😂😮😢🔥👏", b["e"]) || b["e"] == "" {
			fail(w, 400, "Invalid reaction")
			return
		}
		if x().MReact[s.ID] == nil {
			x().MReact[s.ID] = map[string]string{}
		}
		x().MReact[s.ID][u.ID] = b["e"]
		note(s.User, u.ID, "mreact", "")
		save()
		js(w, ok)
	})
	api("mreply", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		s, t := fst(b["id"]), strings.TrimSpace(b["text"])
		if s == nil || t == "" || rn(t) > 500 || S.Users[s.User] == nil || blk(u, S.Users[s.User]) {
			fail(w, 400, "Invalid reply")
			return
		}
		S.Msgs = append(S.Msgs, Msg{From: u.ID, To: s.User, Text: "↩ Moment reply: " + t, TS: now()})
		note(s.User, u.ID, "msg", "")
		save()
		js(w, ok)
	})
	api("mstats", func(u *User, w http.ResponseWriter, r *http.Request) {
		s := fst(in(r)["id"])
		if s == nil || s.User != u.ID {
			fail(w, 403, "Not allowed")
			return
		}
		v, rc := []map[string]any{}, []map[string]any{}
		for _, id := range x().MView[s.ID] {
			v = append(v, uv(id))
		}
		for id, e := range x().MReact[s.ID] {
			rc = append(rc, map[string]any{"U": uv(id), "E": e})
		}
		js(w, map[string]any{"V": v, "R": rc})
	})

	// ---- circle management ----
	api("ginfo", func(u *User, w http.ResponseWriter, r *http.Request) {
		g := fg(r.URL.Query().Get("g"))
		if g == nil || !has(g.Members, u.ID) {
			fail(w, 403, "Not in this circle")
			return
		}
		m := []map[string]any{}
		for _, id := range g.Members {
			role := "member"
			if id == g.Owner {
				role = "owner"
			} else if has(x().GAdm[g.ID], id) {
				role = "admin"
			}
			m = append(m, map[string]any{"U": uv(id), "Role": role})
		}
		js(w, map[string]any{"Name": g.Name, "Owner": u.ID == g.Owner, "CanEdit": u.ID == g.Owner || has(x().GAdm[g.ID], u.ID), "M": m})
	})
	api("gmanage", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		g := fg(b["g"])
		if g == nil || !has(g.Members, u.ID) {
			fail(w, 403, "Not in this circle")
			return
		}
		e, id := x(), b["id"]
		adm := u.ID == g.Owner || has(e.GAdm[g.ID], u.ID)
		switch b["op"] {
		case "rename":
			n := strings.TrimSpace(b["name"])
			if !adm || n == "" || rn(n) > 40 {
				fail(w, 403, "Not allowed")
				return
			}
			g.Name = n
		case "add":
			o := S.Users[id]
			if !adm || o == nil || has(g.Members, id) || blk(u, o) {
				fail(w, 403, "Not allowed")
				return
			}
			g.Members = append(g.Members, id)
			note(id, u.ID, "msg", "")
		case "remove":
			if !adm || id == g.Owner || id == u.ID || (has(e.GAdm[g.ID], id) && u.ID != g.Owner) {
				fail(w, 403, "Not allowed")
				return
			}
			g.Members = del(g.Members, id)
			e.GAdm[g.ID] = del(e.GAdm[g.ID], id)
		case "admin":
			if u.ID != g.Owner || id == u.ID || !has(g.Members, id) {
				fail(w, 403, "Not allowed")
				return
			}
			if has(e.GAdm[g.ID], id) {
				e.GAdm[g.ID] = del(e.GAdm[g.ID], id)
			} else {
				e.GAdm[g.ID] = append(e.GAdm[g.ID], id)
			}
		case "leave":
			g.Members = del(g.Members, u.ID)
			e.GAdm[g.ID] = del(e.GAdm[g.ID], u.ID)
			if len(g.Members) == 0 {
				keep := S.Groups[:0]
				for _, h := range S.Groups {
					if h != g {
						keep = append(keep, h)
					}
				}
				S.Groups = keep
			} else if g.Owner == u.ID {
				g.Owner = g.Members[0]
			}
		default:
			fail(w, 400, "Unknown action")
			return
		}
		save()
		js(w, ok)
	})

	// ---- private accounts and follow requests ----
	api("follow2", func(u *User, w http.ResponseWriter, r *http.Request) {
		id := in(r)["id"]
		o, e := S.Users[id], x()
		if o == nil || id == u.ID || blk(u, o) {
			fail(w, 400, "Invalid user")
			return
		}
		if has(u.Following, id) {
			u.Following = del(u.Following, id)
		} else if has(e.Req[id], u.ID) {
			e.Req[id] = del(e.Req[id], u.ID)
		} else if e.Priv[id] {
			e.Req[id] = append(e.Req[id], u.ID)
			note(id, u.ID, "freq", "")
		} else {
			u.Following = append(u.Following, id)
			note(id, u.ID, "follow", "")
		}
		save()
		js(w, u)
	})
	api("xme", func(u *User, w http.ResponseWriter, r *http.Request) {
		e := x()
		req, sent := []map[string]any{}, []string{}
		for _, id := range e.Req[u.ID] {
			req = append(req, uv(id))
		}
		for t, l := range e.Req {
			if has(l, u.ID) {
				sent = append(sent, t)
			}
		}
		js(w, map[string]any{"Priv": e.Priv[u.ID], "Req": req, "Sent": sent, "Muted": e.Mute[u.ID]})
	})
	api("setpriv", func(u *User, w http.ResponseWriter, r *http.Request) {
		e := x()
		e.Priv[u.ID] = !e.Priv[u.ID]
		if !e.Priv[u.ID] {
			for _, id := range e.Req[u.ID] {
				if o := S.Users[id]; o != nil && !has(o.Following, u.ID) {
					o.Following = append(o.Following, u.ID)
				}
			}
			delete(e.Req, u.ID)
		}
		save()
		js(w, map[string]bool{"Priv": e.Priv[u.ID]})
	})
	api("reqact", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		e, id := x(), b["id"]
		if !has(e.Req[u.ID], id) {
			fail(w, 404, "No such request")
			return
		}
		e.Req[u.ID] = del(e.Req[u.ID], id)
		if o := S.Users[id]; o != nil && b["v"] == "1" && !has(o.Following, u.ID) {
			o.Following = append(o.Following, u.ID)
			note(id, u.ID, "facc", "")
		}
		save()
		js(w, ok)
	})

	// ---- reporting users and comments ----
	api("report2", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		k, why := b["kind"], strings.TrimSpace(b["reason"])
		if len(why) > 200 {
			why = trunc(why, 200)
		}
		rp := &URep{ID: uid(), Kind: k, By: u.ID, Reason: why, TS: now()}
		if k == "user" {
			if S.Users[b["id"]] == nil || b["id"] == u.ID {
				fail(w, 400, "Invalid report")
				return
			}
			rp.Target = b["id"]
		} else if k == "comment" {
			p := fp(b["id"])
			i, e := strconv.Atoi(b["i"])
			if p == nil || e != nil || i < 0 || i >= len(p.Comments) || p.Comments[i].User == u.ID {
				fail(w, 400, "Invalid report")
				return
			}
			c := []rune(p.Comments[i].Text)
			if len(c) > 100 {
				c = c[:100]
			}
			rp.Target, rp.Post, rp.Idx, rp.Text = p.Comments[i].User, p.ID, i, string(c)
		} else {
			fail(w, 400, "Invalid report")
			return
		}
		for _, o := range x().URep {
			if o.By == u.ID && o.Kind == k && o.Target == rp.Target && o.Post == rp.Post && o.Idx == rp.Idx {
				js(w, ok)
				return
			}
		}
		x().URep = append(x().URep, rp)
		save()
		js(w, ok)
	})
	api("ureports", func(u *User, w http.ResponseWriter, r *http.Request) {
		if !u.Admin {
			fail(w, 403, "Admins only")
			return
		}
		out := []map[string]any{}
		for _, o := range x().URep {
			out = append(out, map[string]any{"ID": o.ID, "Kind": o.Kind, "By": uv(o.By), "To": uv(o.Target), "Text": o.Text, "Reason": o.Reason, "TS": o.TS})
		}
		js(w, out)
	})
	api("ucact", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		if !u.Admin {
			fail(w, 403, "Admins only")
			return
		}
		keep := []*URep{}
		for _, o := range x().URep {
			if o.ID != b["id"] {
				keep = append(keep, o)
				continue
			}
			switch b["op"] {
			case "ban":
				if t := S.Users[o.Target]; t != nil && !t.Admin {
					t.Banned = true
					alog(u, "banned @"+t.Username+" from a report")
				}
			case "delc":
				if p := fp(o.Post); p != nil && o.Idx < len(p.Comments) && p.Comments[o.Idx].Text != "" && strings.HasPrefix(p.Comments[o.Idx].Text, o.Text) {
					p.Comments = append(p.Comments[:o.Idx], p.Comments[o.Idx+1:]...)
				}
			}
		}
		S.X.URep = keep
		save()
		js(w, ok)
	})

	// ---- discovery, pin, mute ----
	api("discover", func(u *User, w http.ResponseWriter, r *http.Request) {
		cnt := map[string]int{}
		for _, o := range S.Users {
			for _, f := range o.Following {
				cnt[f]++
			}
		}
		type sc struct {
			id string
			s  int
		}
		l := []sc{}
		for _, o := range S.Users {
			if o.ID == u.ID || o.Username == "" || o.Banned || has(u.Following, o.ID) || blk(u, o) {
				continue
			}
			s := cnt[o.ID]
			for _, f := range u.Following {
				if fo := S.Users[f]; fo != nil && has(fo.Following, o.ID) {
					s += 5
				}
			}
			l = append(l, sc{o.ID, s})
		}
		sort.Slice(l, func(a, b int) bool { return l[a].s > l[b].s })
		ppl := boosted(u)
		have := map[string]bool{}
		for _, m := range ppl {
			have[m["ID"].(string)] = true
		}
		for i := 0; i < len(l) && len(ppl) < 8; i++ {
			if !have[l[i].id] {
				ppl = append(ppl, uv(l[i].id))
			}
		}
		tags, cut7 := map[string]int{}, now()-7*86400
		for _, p := range S.Posts {
			if p.TS < cut7 || p.Comm != "" || x().Priv[p.User] {
				continue
			}
			seen := map[string]bool{}
			for _, t := range tagRe.FindAllString(strings.ToLower(p.Text), -1) {
				if !seen[t] {
					seen[t] = true
					tags[t]++
				}
			}
		}
		type tc struct {
			T string
			N int
		}
		tl := []tc{}
		for t, n := range tags {
			tl = append(tl, tc{t, n})
		}
		sort.Slice(tl, func(a, b int) bool { return tl[a].N > tl[b].N || (tl[a].N == tl[b].N && tl[a].T < tl[b].T) })
		if len(tl) > 8 {
			tl = tl[:8]
		}
		js(w, map[string]any{"People": ppl, "Tags": tl})
	})
	api("pin", func(u *User, w http.ResponseWriter, r *http.Request) {
		id := in(r)["id"]
		p := fp(id)
		if p == nil || p.User != u.ID {
			fail(w, 400, "Cannot pin this post")
			return
		}
		if x().Pin[u.ID] == id {
			delete(x().Pin, u.ID)
		} else {
			x().Pin[u.ID] = id
		}
		save()
		js(w, map[string]string{"Pin": x().Pin[u.ID]})
	})
	api("mute", func(u *User, w http.ResponseWriter, r *http.Request) {
		id, e := in(r)["id"], x()
		if id == u.ID || S.Users[id] == nil {
			fail(w, 400, "Invalid user")
			return
		}
		m := !has(e.Mute[u.ID], id)
		if m {
			e.Mute[u.ID] = append(e.Mute[u.ID], id)
		} else {
			e.Mute[u.ID] = del(e.Mute[u.ID], id)
		}
		save()
		js(w, map[string]bool{"Muted": m})
	})
}

// ---------- v5: fixes ----------
var (
	hc      = &http.Client{Timeout: 15 * time.Second}
	hu      = &http.Client{Timeout: 3 * time.Minute}
	saveSig = make(chan struct{}, 1)
	saveVer = 0
	pver    = -1
	pidx    = map[string]*Post{}
	okTypes = map[string]string{"image/jpeg": "jpg", "image/png": "png", "image/gif": "gif", "image/webp": "webp", "image/avif": "avif", "image/heic": "heic", "image/heif": "heif", "video/mp4": "mp4", "video/webm": "webm", "video/quicktime": "mov", "video/3gpp": "3gp", "audio/webm": "weba", "audio/mp4": "m4a", "audio/mpeg": "mp3", "audio/ogg": "ogg", "audio/wav": "wav", "audio/aac": "aac"}
)

func rn(s string) int { return utf8.RuneCountInString(s) }
func trunc(s string, n int) string {
	r := []rune(s)
	if len(r) > n {
		return string(r[:n])
	}
	return s
}

// save is called while holding mu; the state is written at most every 1.5 seconds
func save() {
	fillIDs(false)
	saveVer++
	select {
	case saveSig <- struct{}{}:
	default:
	}
}
func writer() {
	for range saveSig {
		time.Sleep(1500 * time.Millisecond)
		flushParts()
	}
}
func flush(b []byte) {
	flushParts()
}

// fp finds a post through an index that is rebuilt whenever the state changes
var pxN int
var pxLast string

func fp(id string) *Post {
	n := len(S.Posts)
	last := ""
	if n > 0 {
		last = S.Posts[n-1].ID
	}
	if n != pxN || last != pxLast || len(pidx) != n {
		pidx = make(map[string]*Post, n)
		for _, p := range S.Posts {
			pidx[p.ID] = p
		}
		pxN, pxLast = n, last
	}
	return pidx[id]
}

// hid: true when viewer u must not see or interact with content by user id
func hid(u *User, id string) bool {
	o := S.Users[id]
	return o == nil || o.Banned || blk(u, o) || priv(u, o)
}

// authUser checks the session, expiry, ban and rate limit (used by upload)
func authUser(r *http.Request) *User {
	mu.Lock()
	defer mu.Unlock()
	tok := r.Header.Get("X-Token")
	u := S.Users[S.Sess[tok]]
	if u == nil || u.Banned {
		return nil
	}
	if ts, k := S.SessTS[tok]; k && now()-ts > 2592000 {
		return nil
	}
	if !rate(u.ID) {
		return nil
	}
	u.Seen = now()
	return u
}

// janitor clears expired sessions, old rate-limit entries and stale moment data
func janitor() {
	for range time.Tick(10 * time.Minute) {
		mu.Lock()
		t := now()
		for tok := range S.Sess {
			ts, k := S.SessTS[tok]
			if !k {
				S.SessTS[tok] = t
			} else if t-ts > 2592000 {
				delete(S.Sess, tok)
				delete(S.SessTS, tok)
			}
		}
		for tok := range S.SessTS {
			if _, k := S.Sess[tok]; !k {
				delete(S.SessTS, tok)
			}
		}
		for id, v := range rl {
			if v[0] < t/60-2 {
				delete(rl, id)
			}
		}
		for tok := range x().Dev {
			if _, k := S.Sess[tok]; !k {
				delete(x().Dev, tok)
			}
		}
		live := map[string]bool{}
		for _, s := range S.Statuses {
			live[s.ID] = true
		}
		for id := range x().MView {
			if !live[id] {
				delete(x().MView, id)
			}
		}
		for id := range x().MReact {
			if !live[id] {
				delete(x().MReact, id)
			}
		}
		save()
		mu.Unlock()
	}
}

// purgeMore removes everything else that belongs to a deleted account (call before deleting the user)
func purgeMore(id string) {
	purgeX5(id)
	purgeX7(id)
	purgeX8(id)
	e := x()
	var files []string
	if u := S.Users[id]; u != nil {
		files = append(files, u.Avatar, u.Cover)
	}
	for _, p := range S.Posts {
		if p.User == id {
			files = append(files, p.Media)
			files = append(files, p.More...)
			if o := x3().PO[p.ID]; o != nil {
				files = append(files, o.Poster)
				delete(x3().PO, p.ID)
			}
			continue
		}
		cs := p.Comments[:0:0]
		for _, c := range p.Comments {
			if c.User != id {
				c.Likes = del(c.Likes, id)
				cs = append(cs, c)
			}
		}
		p.Comments = cs
		p.Likes = del(p.Likes, id)
		if p.Poll != nil {
			delete(p.Poll.Votes, id)
		}
	}
	ks := S.Statuses[:0]
	for _, s := range S.Statuses {
		if s.User == id {
			files = append(files, s.Media)
		} else {
			ks = append(ks, s)
		}
	}
	S.Statuses = ks
	km := S.Msgs[:0]
	for _, m := range S.Msgs {
		if m.From == id || m.To == id {
			files = append(files, m.Media)
			continue
		}
		km = append(km, m)
	}
	S.Msgs = km
	kn := S.Notifs[:0]
	for _, n := range S.Notifs {
		if n.To != id && n.From != id {
			kn = append(kn, n)
		}
	}
	S.Notifs = kn
	kg := S.Groups[:0]
	for _, g := range S.Groups {
		if !has(g.Members, id) {
			kg = append(kg, g)
			continue
		}
		g.Members = del(g.Members, id)
		e.GAdm[g.ID] = del(e.GAdm[g.ID], id)
		if len(g.Members) == 0 {
			continue
		}
		if g.Owner == id {
			g.Owner = g.Members[0]
		}
		kg = append(kg, g)
	}
	S.Groups = kg
	kc := S.Channels[:0]
	for _, c := range S.Channels {
		if c.Owner == id {
			continue
		}
		c.Subs = del(c.Subs, id)
		kc = append(kc, c)
	}
	S.Channels = kc
	delete(e.Priv, id)
	delete(e.Req, id)
	delete(e.Pin, id)
	delete(e.Mute, id)
	delete(e.NoVer, id)
	delete(e.Born, id)
	delete(e.Susp, id)
	delete(S.X2.Push, id)
	purgeComms(id)
	for k, l := range e.Req {
		e.Req[k] = del(l, id)
	}
	for k, l := range e.Mute {
		e.Mute[k] = del(l, id)
	}
	ku := []*URep{}
	for _, r := range e.URep {
		if r.By != id && r.Target != id {
			ku = append(ku, r)
		}
	}
	e.URep = ku
	go func(fs []string) {
		pre := env("SUPABASE_URL") + "/storage/v1/object/public/media/"
		for _, f := range fs {
			if n := strings.TrimPrefix(f, pre); n != f && n != "" && !strings.Contains(n, "/") {
				if r, err := sb("DELETE", "/storage/v1/object/media/"+n, nil, ""); err == nil {
					r.Body.Close()
				}
			}
		}
	}(files)
}

// ---------- v6: For You ranking ----------
func extra2() {
	api("foryou", func(u *User, w http.ResponseWriter, r *http.Request) {
		vid := r.URL.Query().Get("k") == "video"
		aff := map[string]int{}
		for _, p := range S.Posts {
			if has(p.Likes, u.ID) {
				aff[p.User]++
			}
		}
		type sp struct {
			p *Post
			s float64
		}
		l := []sp{}
		t := float64(now())
		for _, p := range S.Posts {
			if p.Comm != "" || (vid && p.Kind != "video") || hid(u, p.User) || S.X7.Flag[p.ID] {
				continue
			}
			h := (t - float64(p.TS)) / 3600
			e := float64(len(p.Likes)) + 2*float64(len(p.Comments))
			if p.Poll != nil {
				e += float64(len(p.Poll.Votes))
			}
			s := (1 + e) / math.Pow(h+2, 1.3)
			if has(u.Following, p.User) {
				s *= 2.5
			}
			s *= 1 + 0.3*float64(aff[p.User])
			if p.User == u.ID {
				s *= 0.5
			}
			s *= interestBoost(u.ID, p)
			l = append(l, sp{p, s})
		}
		sort.Slice(l, func(a, b int) bool { return l[a].s > l[b].s })
		out := []map[string]any{}
		for i := 0; i < len(l) && i < 60; i++ {
			out = append(out, pv(l[i].p))
		}
		js(w, out)
	})
}

// ---------- v7: security and moderation ----------
type Dev struct {
	UA, IP string
	TS     int64
}

func tid(t string) string {
	h := sha256.Sum256([]byte(t))
	return hex.EncodeToString(h[:6])
}
func cip(r *http.Request) string {
	if f := r.Header.Get("X-Forwarded-For"); f != "" {
		parts := strings.Split(f, ",")
		hops := 0
		if n, e := strconv.Atoi(env("PROXY_HOPS")); e == nil && n >= 1 {
			hops = n
		}
		if hops == 0 {
			return strings.TrimSpace(parts[0])
		}
		if hops > len(parts) {
			hops = len(parts)
		}
		return strings.TrimSpace(parts[len(parts)-hops])
	}
	a := r.RemoteAddr
	if i := strings.LastIndex(a, ":"); i > 0 {
		a = a[:i]
	}
	return a
}

// xdev records the device for a new session and alerts the user about a login from a new device (call with mu held)
func xdev(tok string, r *http.Request) {
	id := S.Sess[tok]
	ua := trunc(r.UserAgent(), 160)
	had, known := false, false
	for t, d := range x().Dev {
		if S.Sess[t] == id {
			had = true
			if d.UA == ua {
				known = true
			}
		}
	}
	x().Dev[tok] = &Dev{ua, cip(r), now()}
	if had && !known {
		S.Notifs = append(S.Notifs, Notif{id, id, "login", "", now(), false})
		push(id, "login")
	}
}

var modPaths = map[string]string{"/api/post": "p", "/api/post2": "p", "/api/status": "p", "/api/chpost": "p", "/api/comment": "c", "/api/comment2": "c", "/api/comment3": "c", "/api/msg": "m", "/api/gmsg": "m", "/api/bio": "t", "/api/editpost": "t", "/api/chcreate": "t", "/api/gcreate": "t"}
var modHits = map[string][]int64{}

func modMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cl, k := modPaths[r.URL.Path]; k && r.Method == "POST" {
			b, _ := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
			r.Body = io.NopCloser(bytes.NewReader(b))
			if code, msg := modCheck(r, b, cl); msg != "" {
				fail(w, code, msg)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
func badText(s string, words []string) bool {
	l := strings.ToLower(s)
	for _, w := range words {
		if w != "" && strings.Contains(l, w) {
			return true
		}
	}
	return false
}
func modCheck(r *http.Request, b []byte, cl string) (int, string) {
	mu.Lock()
	defer mu.Unlock()
	u := S.Users[S.Sess[r.Header.Get("X-Token")]]
	if u == nil || u.Admin {
		return 0, ""
	}
	e := x()
	if t := e.Susp[u.ID]; t > now() {
		return 403, fmt.Sprintf("Your account is suspended from posting for about %d more hours", (t-now())/3600+1)
	}
	var m map[string]any
	json.Unmarshal(b, &m)
	for k, v := range m {
		if s, ok := v.(string); ok && k != "media" && k != "avatar" && k != "cover" && k != "id" && k != "g" && k != "to" && k != "token" && k != "parent" && badText(s, e.Words) {
			return 400, "That contains words that are not allowed on Amlink"
		}
	}
	if born, k := e.Born[u.ID]; k && cl != "t" && now()-born < 86400 {
		lim := map[string]int{"p": 5, "c": 30, "m": 40}[cl]
		key := u.ID + cl
		h := modHits[key][:0:0]
		for _, t := range modHits[key] {
			if now()-t < 3600 {
				h = append(h, t)
			}
		}
		if len(h) >= lim {
			modHits[key] = h
			return 429, "New accounts have posting limits during the first day. Try again later."
		}
		modHits[key] = append(h, now())
	}
	return 0, ""
}

func extra3() {
	ok := map[string]bool{"ok": true}
	api("sessions", func(u *User, w http.ResponseWriter, r *http.Request) {
		cur := r.Header.Get("X-Token")
		out := []map[string]any{}
		for t, id := range S.Sess {
			if id != u.ID {
				continue
			}
			m := map[string]any{"ID": tid(t), "Me": t == cur, "UA": "", "IP": "", "TS": S.SessTS[t]}
			if d := x().Dev[t]; d != nil {
				m["UA"], m["IP"], m["TS"] = d.UA, d.IP, d.TS
			}
			out = append(out, m)
		}
		sort.Slice(out, func(a, b int) bool { return out[a]["TS"].(int64) > out[b]["TS"].(int64) })
		js(w, out)
	})
	api("revoke", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		cur := r.Header.Get("X-Token")
		for t, o := range S.Sess {
			if o == u.ID && (tid(t) == b["id"] || (b["all"] == "1" && (t != cur || b["me"] == "1"))) {
				delete(S.Sess, t)
				delete(S.SessTS, t)
				delete(x().Dev, t)
			}
		}
		save()
		js(w, ok)
	})
	api("sens", func(u *User, w http.ResponseWriter, r *http.Request) {
		id := in(r)["id"]
		p := fp(id)
		if p == nil || (p.User != u.ID && !u.Admin) {
			fail(w, 403, "Not allowed")
			return
		}
		x().Sens[id] = !x().Sens[id]
		save()
		js(w, map[string]bool{"Sens": x().Sens[id]})
	})
	api("modq", func(u *User, w http.ResponseWriter, r *http.Request) {
		if !u.Admin && !isMod(u.ID) {
			fail(w, 403, "Admins only")
			return
		}
		q := []map[string]any{}
		for _, rp := range S.Reports {
			m := map[string]any{"ID": rp.ID, "Kind": "post", "By": uv(rp.By), "To": uv(""), "Reason": rp.Reason, "TS": rp.TS, "Text": ""}
			if p := fp(rp.Post); p != nil {
				m["To"], m["Text"] = uv(p.User), trunc(p.Text, 80)
			}
			q = append(q, m)
		}
		for _, o := range x().URep {
			q = append(q, map[string]any{"ID": o.ID, "Kind": o.Kind, "By": uv(o.By), "To": uv(o.Target), "Reason": o.Reason, "TS": o.TS, "Text": o.Text})
		}
		ws := x().Words
		if ws == nil {
			ws = []string{}
		}
		js(w, map[string]any{"Q": q, "Words": ws})
	})
	api("modwords", func(u *User, w http.ResponseWriter, r *http.Request) {
		if !u.Admin {
			fail(w, 403, "Admins only")
			return
		}
		var ws []string
		for _, s := range strings.Split(strings.ToLower(in(r)["w"]), ",") {
			if s = strings.TrimSpace(s); s != "" && rn(s) <= 40 && len(ws) < 200 {
				ws = append(ws, s)
			}
		}
		x().Words = ws
		alog(u, "updated blocked words")
		save()
		js(w, ok)
	})
	api("modact", func(u *User, w http.ResponseWriter, r *http.Request) {
		if !u.Admin && !isMod(u.ID) {
			fail(w, 403, "Admins only")
			return
		}
		b := in(r)
		id, op := b["id"], b["op"]
		if !u.Admin && (op == "ban" || op == "susp7") {
			fail(w, 403, "Only an admin can do that")
			return
		}
		var target, post, ctext string
		cidx, found := -1, false
		for _, rp := range S.Reports {
			if rp.ID == id {
				found, post = true, rp.Post
				if p := fp(post); p != nil {
					target = p.User
				}
			}
		}
		for _, o := range x().URep {
			if o.ID == id {
				found, target, post, ctext, cidx = true, o.Target, o.Post, o.Text, o.Idx
				if o.Kind == "message" {
					cidx = -2
				}
			}
		}
		if !found {
			fail(w, 404, "Report not found")
			return
		}
		t, e := S.Users[target], x()
		switch op {
		case "dismiss":
			if cidx == -1 {
				delete(x7().Flag, post)
			}
		case "delete":
			if cidx == -2 {
				for i := range S.Msgs {
					if S.Msgs[i].ID == post {
						dropFiles(S.Msgs[i].Media)
						S.Msgs = append(S.Msgs[:i], S.Msgs[i+1:]...)
						break
					}
				}
			} else if cidx >= 0 {
				if p := fp(post); p != nil && cidx < len(p.Comments) && strings.HasPrefix(p.Comments[cidx].Text, ctext) {
					p.Comments = append(p.Comments[:cidx], p.Comments[cidx+1:]...)
				}
			} else {
				for i, p := range S.Posts {
					if p.ID == post {
						S.Posts = append(S.Posts[:i], S.Posts[i+1:]...)
						break
					}
				}
			}
		case "sens":
			if cidx < 0 {
				e.Sens[post] = true
			}
		case "warn", "susp1", "susp7", "ban":
			if t != nil && !t.Admin {
				typ := "warn"
				switch op {
				case "susp1":
					e.Susp[target], typ = now()+86400, "susp"
				case "susp7":
					e.Susp[target], typ = now()+7*86400, "susp"
				case "ban":
					t.Banned = true
				}
				if op != "ban" {
					S.Notifs = append(S.Notifs, Notif{target, target, typ, "", now(), false})
					push(target, typ)
				}
				alog(u, op+" on @"+t.Username)
			}
		default:
			fail(w, 400, "Unknown action")
			return
		}
		keep := S.Reports[:0]
		for _, rp := range S.Reports {
			if rp.ID != id {
				keep = append(keep, rp)
			}
		}
		S.Reports = keep
		ku := []*URep{}
		for _, o := range e.URep {
			if o.ID != id {
				ku = append(ku, o)
			}
		}
		e.URep = ku
		save()
		js(w, ok)
	})
}

// =====================================================================
// ---------- v8: fixes and new features ----------
// =====================================================================

// Ext2 holds the v8 data.
type Ext2 struct {
	Views         map[string]int
	Rep           map[string]string        // repost id -> original post id
	MW            map[string][]string      // muted words per user
	Hid           map[string][]string      // "not interested" post ids per user
	Tags          map[string][]string      // followed hashtags per user
	Push          map[string][]string      // web push endpoints per user
	PinC          map[string]string        // post id -> pinned comment id
	VReq          map[string]*VRq          // verification requests by user id
	Comms         []*Comm                  // communities
	CommElections map[string]*CommElection // monthly community elections
	Disap         map[string]int64         // disappearing-message timer per chat (seconds)
}

var (
	mediaRe = regexp.MustCompile(`^[0-9a-f]{24}\.[a-z0-9]{2,5}$`)
	tagOne  = regexp.MustCompile(`^#\w{1,30}$`)
	partH   = map[string][32]byte{}
	partMu  sync.Mutex
	oldGone bool
)

func init() {
	modPaths["/api/msg2"], modPaths["/api/gmsg2"], modPaths["/api/repost"], modPaths["/api/mact"] = "m", "m", "p", "t"
	vapidInit()
	extra5()
	extra6()
	extra7()
}

func x2() *Ext2 {
	e := &S.X2
	if e.Views == nil {
		e.Views = map[string]int{}
	}
	if e.Rep == nil {
		e.Rep = map[string]string{}
	}
	if e.MW == nil {
		e.MW = map[string][]string{}
	}
	if e.Hid == nil {
		e.Hid = map[string][]string{}
	}
	if e.Tags == nil {
		e.Tags = map[string][]string{}
	}
	if e.PinC == nil {
		e.PinC = map[string]string{}
	}
	if e.VReq == nil {
		e.VReq = map[string]*VRq{}
	}
	if e.CommElections == nil {
		e.CommElections = map[string]*CommElection{}
	}
	if e.Disap == nil {
		e.Disap = map[string]int64{}
	}
	return e
}

// FIX 1: never send DOB / consent time to the browser
func pubUser(u *User) *User {
	c := *u
	c.DOB, c.Agreed = "", 0
	return &c
}

// FIX 2: strict media URL check (no path tricks)
func okMedia2(m string) bool {
	if m == "" {
		return true
	}
	p := env("SUPABASE_URL") + "/storage/v1/object/public/media/"
	return strings.HasPrefix(m, p) && mediaRe.MatchString(strings.TrimPrefix(m, p))
}

// FIX 3: read the upload (30 MB max) and reject files that are really text/html/scripts
func readUpload(w http.ResponseWriter, r *http.Request, ct string) ([]byte, bool) {
	b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 30<<20))
	if err != nil || len(b) == 0 {
		fail(w, 400, "Upload is empty or too big (30 MB max)")
		return nil, false
	}
	n := len(b)
	if n > 512 {
		n = 512
	}
	s := http.DetectContentType(b[:n])
	if strings.HasPrefix(s, "text/") || (strings.HasPrefix(s, "application/") && s != "application/octet-stream" && s != "application/ogg") {
		fail(w, 400, "That is not a real image, video or audio file")
		return nil, false
	}
	return b, true
}

// FIX 4: every message gets an id (old ones are filled in once at start)
func fillIDs(all bool) {
	for i := len(S.Msgs) - 1; i >= 0; i-- {
		if S.Msgs[i].ID == "" {
			S.Msgs[i].ID = uid()
		} else if !all {
			break
		}
	}
}

// FIX 5: the state is saved as 4 separate rows and only changed rows are written
func rawJSON(raw json.RawMessage) []byte {
	if len(raw) > 0 && raw[0] == '"' {
		var t string
		json.Unmarshal(raw, &t)
		return []byte(t)
	}
	return raw
}
func loadParts() bool {
	rows, e := neon("SELECT key, value::text AS v FROM store WHERE key LIKE 's\\_%'")
	if e != nil || len(rows) == 0 {
		return false
	}
	m := map[string][]byte{}
	for _, r := range rows {
		var k string
		json.Unmarshal(r["key"], &k)
		m[k] = rawJSON(r["v"])
	}
	if m["s_rest"] == nil {
		return false
	}
	json.Unmarshal(m["s_rest"], &S)
	if b := m["s_users"]; b != nil {
		json.Unmarshal(b, &S.Users)
	}
	if b := m["s_posts"]; b != nil {
		json.Unmarshal(b, &S.Posts)
	}
	if b := m["s_msgs"]; b != nil {
		json.Unmarshal(b, &S.Msgs)
	}
	return true
}
func flushParts() {
	if env("DATABASE_URL") == "" {
		mu.Lock()
		b, _ := json.Marshal(S)
		mu.Unlock()
		if os.WriteFile("state.json.tmp", b, 0644) == nil {
			os.Rename("state.json.tmp", "state.json")
		}
		return
	}
	mu.Lock()
	var us, ps, ms []rowT
	for id, u := range S.Users {
		b, _ := json.Marshal(u)
		us = append(us, rowT{id, 0, b})
	}
	for _, p := range S.Posts {
		b, _ := json.Marshal(p)
		ps = append(ps, rowT{p.ID, p.TS, b})
	}
	for _, m := range S.Msgs {
		b, _ := json.Marshal(m)
		ms = append(ms, rowT{m.ID, m.TS, b})
	}
	r := S
	r.Users, r.Posts, r.Msgs = nil, nil, nil
	rest, _ := json.Marshal(r)
	mu.Unlock()
	partMu.Lock()
	defer partMu.Unlock()
	all := syncTable("app_users", us, rowH["app_users"])
	all = syncTable("app_posts", ps, rowH["app_posts"]) && all
	all = syncTable("app_msgs", ms, rowH["app_msgs"]) && all
	h := sha256.Sum256(rest)
	if partH["s_rest"] != h {
		if _, e := neon("INSERT INTO store(key,value) VALUES('s_rest',$1::jsonb) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value", string(rest)); e != nil {
			all = false
		} else {
			partH["s_rest"] = h
		}
	}
	if all && !rowsMarked { // marker last: only set once every table is safely written
		if _, e := neon("INSERT INTO store(key,value) VALUES('rows_v1','1'::jsonb) ON CONFLICT (key) DO NOTHING"); e == nil {
			rowsMarked = true
		}
	}
	if all && !oldGone { // the old single-row copy is removed once the new rows are safe
		if _, e := neon("DELETE FROM store WHERE key='state'"); e == nil {
			oldGone = true
		}
	}
}

// ---------- storage clean-up ----------
func dropFiles(fs ...string) {
	pre := env("SUPABASE_URL") + "/storage/v1/object/public/media/"
	var names []string
	for _, f := range fs {
		if n := strings.TrimPrefix(f, pre); n != f && mediaRe.MatchString(n) {
			names = append(names, n)
		}
	}
	if len(names) == 0 {
		return
	}
	go func() {
		for _, n := range names {
			if r, err := sb("DELETE", "/storage/v1/object/media/"+n, nil, ""); err == nil {
				r.Body.Close()
			}
		}
	}()
}
func v8janitor() {
	for range time.Tick(10 * time.Minute) {
		mu.Lock()
		var fs []string
		ks := S.Statuses[:0]
		for _, s := range S.Statuses {
			if now()-s.TS > 86400 {
				if !hlKeep(s.Media) {
					fs = append(fs, s.Media)
				}
			} else {
				ks = append(ks, s)
			}
		}
		S.Statuses = ks
		kn := S.Notifs[:0]
		for _, n := range S.Notifs {
			if now()-n.TS < 30*86400 {
				kn = append(kn, n)
			}
		}
		S.Notifs = kn
		save()
		mu.Unlock()
		dropFiles(fs...)
	}
}

// ---------- features ----------
func repOf(id string) any {
	o := x2().Rep[id]
	if o == "" {
		return nil
	}
	p := fp(o)
	if p == nil || x().Priv[p.User] {
		return map[string]any{"Gone": true}
	}
	return map[string]any{"ID": p.ID, "U": uv(p.User), "Text": p.Text, "Media": p.Media, "Kind": p.Kind}
}

func extra4() {
	ok := map[string]bool{"ok": true}
	go v8janitor()

	// circles with photos / videos / voice
	api("gmsg2", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		g, t := fg(b["g"]), strings.TrimSpace(b["text"])
		k := b["kind"]
		if k != "image" && k != "video" && k != "audio" {
			k = ""
		}
		if g == nil || !has(g.Members, u.ID) || (t == "" && b["media"] == "") || rn(t) > 1000 || !okMedia(b["media"]) {
			fail(w, 400, "Invalid message")
			return
		}
		g.Msgs = append(g.Msgs, Msg{From: u.ID, To: g.ID, Text: t, Kind: k, Media: b["media"], TS: now()})
		puzzleCheck(g, u, t)
		for _, m := range g.Members {
			note(m, u.ID, "msg", "")
		}
		save()
		js(w, ok)
	})
	api("gmsgs2", func(u *User, w http.ResponseWriter, r *http.Request) {
		g := fg(r.URL.Query().Get("g"))
		if g == nil || !has(g.Members, u.ID) {
			fail(w, 403, "Not in this circle")
			return
		}
		out, st := []map[string]any{}, len(g.Msgs)-200
		if st < 0 {
			st = 0
		}
		for _, m := range g.Msgs[st:] {
			out = append(out, map[string]any{"U": uv(m.From), "Me": m.From == u.ID, "Text": m.Text, "TS": m.TS, "Media": m.Media, "Kind": m.Kind})
		}
		js(w, out)
	})

	// chat: reply, react, edit, unsend (by id)
	api("msg2", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		t, o := strings.TrimSpace(b["text"]), S.Users[b["to"]]
		k := b["kind"]
		if k != "image" && k != "video" && k != "audio" {
			k = ""
		}
		if o == nil || blk(u, o) || (t == "" && b["media"] == "") || rn(t) > 1000 || !okMedia(b["media"]) {
			fail(w, 400, "Invalid message")
			return
		}
		rp := b["reply"]
		if rp != "" {
			found := false
			for i := len(S.Msgs) - 1; i >= 0 && i > len(S.Msgs)-500; i-- {
				if S.Msgs[i].ID == rp {
					found = true
					break
				}
			}
			if !found {
				rp = ""
			}
		}
		S.Msgs = append(S.Msgs, Msg{From: u.ID, To: o.ID, Text: t, Kind: k, Media: b["media"], TS: now(), Reply: rp, Exp: dexp(u.ID, o.ID)})
		note(o.ID, u.ID, "msg", "")
		save()
		js(w, ok)
	})
	api("mact", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		for i := range S.Msgs {
			m := &S.Msgs[i]
			if m.ID != b["id"] || (m.From != u.ID && m.To != u.ID) {
				continue
			}
			other := m.To
			if m.From != u.ID {
				other = m.From
			}
			switch b["op"] {
			case "react":
				e := b["e"]
				if e == "" || !strings.Contains("❤️😂😮😢🔥👍", e) {
					fail(w, 400, "Invalid reaction")
					return
				}
				if m.React == nil {
					m.React = map[string]string{}
				}
				if m.React[u.ID] == e {
					delete(m.React, u.ID)
				} else {
					m.React[u.ID] = e
				}
			case "edit":
				t := strings.TrimSpace(b["text"])
				if m.From != u.ID || m.Media != "" || t == "" || rn(t) > 1000 {
					fail(w, 400, "Cannot edit this message")
					return
				}
				m.Text, m.Edited = t, true
			case "unsend":
				if m.From != u.ID {
					fail(w, 403, "Not allowed")
					return
				}
				dropFiles(m.Media)
				S.Msgs = append(S.Msgs[:i], S.Msgs[i+1:]...)
			default:
				fail(w, 400, "Unknown action")
				return
			}
			push(other, "msg")
			save()
			js(w, ok)
			return
		}
		fail(w, 404, "Message not found")
	})

	// reposts and quote posts
	api("repost", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		o, t := fp(b["id"]), strings.TrimSpace(b["text"])
		if o == nil || hid(u, o.User) || rn(t) > 500 || x().NoVer[u.ID] {
			fail(w, 400, "You cannot repost this")
			return
		}
		if q := fp(x2().Rep[o.ID]); q != nil {
			o = q
		}
		p := &Post{ID: uid(), User: u.ID, Text: t, TS: now(), Likes: []string{}}
		S.Posts = append(S.Posts, p)
		x2().Rep[p.ID] = o.ID
		note(o.User, u.ID, "repost", o.ID)
		mentions(u, t, p.ID)
		save()
		js(w, ok)
	})

	// Detect outbound social-platform links in captions. Such posts remain on the
	// creator profile and Following feed, but are excluded from For You discovery.
	// This is a distribution rule, not deletion or account punishment.
	// (The matcher intentionally targets known external social/video destinations.)
	//
	// Viral ranking below emphasizes engagement rates and completion signals while
	// retaining recency, creator affinity and exposure for newer creators.
	//
	// helper is local to this file to avoid changing the persisted post schema.

	// Reels recommendation feed: ranks public video posts using recency, follows,
	// creator affinity and aggregate engagement. No external tracking is used.
	api("reelsfeed", func(u *User, w http.ResponseWriter, r *http.Request) {
		limit, _ := strconv.Atoi(r.URL.Query().Get("n"))
		if limit < 1 || limit > 100 {
			limit = 60
		}
		e := x10()
		followingOnly := r.URL.Query().Get("mode") == "following"
		type scored struct {
			p     *Post
			score float64
		}
		ranked := []scored{}
		nowTS := now()
		for _, p := range S.Posts {
			if p.Kind != "video" || p.Media == "" || p.Comm != "" || x7().Flag[p.ID] || p.User == "" {
				continue
			}
			// Keep external-platform promotion links out of For You recommendations.
			// They remain visible on the creator's profile and to followers.
			if !followingOnly && hasExternalSocialLink(p.Text) {
				continue
			}
			if followingOnly && p.User != u.ID && !has(u.Following, p.User) {
				continue
			}
			owner := S.Users[p.User]
			if owner == nil || owner.Banned || blk(u, owner) || priv(u, owner) {
				continue
			}
			if p.User != u.ID && has(u.Blocked, p.User) {
				continue
			}
			age := float64(nowTS - p.TS)
			if age < 0 {
				age = 0
			}
			views := e.Views[p.ID]
			if views < 1 {
				views = 1
			}
			likes, comments, shares := len(p.Likes), len(p.Comments), e.Shares[p.ID]
			// Rate-based engagement lets a new creator's well-performing reel compete
			// with established accounts instead of ranking only by raw totals.
			engagementRate := float64(likes*3+comments*5+shares*4) / float64(views)
			if engagementRate > 2.5 {
				engagementRate = 2.5
			}
			score := 28.0/(1.0+age/43200.0) + engagementRate*34
			score += float64(e.CreatorAffinity[u.ID][p.User]) * 2.2
			score += float64(likes)*0.08 + float64(comments)*0.18 + float64(shares)*0.22
			if has(u.Following, p.User) {
				score += 12
			}
			if p.User == u.ID {
				score -= 1000
			}
			if signal := e.Signals[u.ID][p.ID]; signal < 0 {
				score += float64(signal) * 7
			} else if signal > 0 {
				score += float64(signal) * 1.2
			}
			// Small discovery boost prevents new accounts from being buried.
			if views < 100 {
				score += 8
			}
			ranked = append(ranked, scored{p, score})
		}
		sort.SliceStable(ranked, func(i, j int) bool { return ranked[i].score > ranked[j].score })
		if len(ranked) > limit {
			ranked = ranked[:limit]
		}
		out := []map[string]any{}
		for _, v := range ranked {
			out = append(out, pv(v.p))
		}
		js(w, out)
	})

	// Count a reel view once per viewer per 24 hours to limit refresh inflation.
	api("reelview", func(u *User, w http.ResponseWriter, r *http.Request) {
		id := in(r)["id"]
		p := fp(id)
		if p == nil || p.Kind != "video" || hid(u, p.User) {
			fail(w, 404, "Reel not found")
			return
		}
		e := x10()
		key := u.ID + "|" + id
		if now()-e.Seen[key] >= 86400 {
			e.Seen[key] = now()
			e.Views[id]++
			if u.ID != p.User {
				if e.CreatorAffinity[u.ID] == nil {
					e.CreatorAffinity[u.ID] = map[string]int{}
				}
				e.CreatorAffinity[u.ID][p.User]++
			}
			save()
		}
		js(w, map[string]int{"Views": e.Views[id]})
	})

	// Record explicit reel engagement for recommendation ranking and creator analytics.
	api("reelsignal", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		p := fp(b["id"])
		kind := b["kind"]
		weights := map[string]int{"like": 3, "comment": 4, "save": 5, "share": 3, "skip": -1, "complete": 2}
		wgt, ok := weights[kind]
		if p == nil || p.Kind != "video" || !ok || hid(u, p.User) {
			fail(w, 400, "Invalid reel engagement")
			return
		}
		e := x10()
		if e.Signals[u.ID] == nil {
			e.Signals[u.ID] = map[string]int{}
		}
		v := e.Signals[u.ID][p.ID] + wgt
		if v > 100 {
			v = 100
		}
		if v < -20 {
			v = -20
		}
		e.Signals[u.ID][p.ID] = v
		if u.ID != p.User {
			if e.CreatorAffinity[u.ID] == nil {
				e.CreatorAffinity[u.ID] = map[string]int{}
			}
			e.CreatorAffinity[u.ID][p.User] += wgt
			if e.CreatorAffinity[u.ID][p.User] > 100 {
				e.CreatorAffinity[u.ID][p.User] = 100
			}
			if e.CreatorAffinity[u.ID][p.User] < -20 {
				e.CreatorAffinity[u.ID][p.User] = -20
			}
		}
		if kind == "share" {
			e.Shares[p.ID]++
		}
		save()
		js(w, map[string]bool{"ok": true})
	})

	// views (batched by the browser) shown to the post owner
	api("pview", func(u *User, w http.ResponseWriter, r *http.Request) {
		for i, id := range strings.Split(in(r)["ids"], ",") {
			if i >= 30 {
				break
			}
			if p := fp(id); p != nil && p.User != u.ID && !hid(u, p.User) {
				x2().Views[id]++
				reachAdd(id, u.ID)
			}
		}
		js(w, ok)
	})

	// muted words, not interested, followed tags
	api("v8me", func(u *User, w http.ResponseWriter, r *http.Request) {
		e := x2()
		js(w, map[string]any{"W": e.MW[u.ID], "Hid": e.Hid[u.ID], "Tags": e.Tags[u.ID]})
	})
	api("mw", func(u *User, w http.ResponseWriter, r *http.Request) {
		if v, k := in(r)["w"]; k {
			var ws []string
			for _, s := range strings.Split(strings.ToLower(v), ",") {
				if s = strings.TrimSpace(s); s != "" && rn(s) <= 30 && len(ws) < 50 {
					ws = append(ws, s)
				}
			}
			x2().MW[u.ID] = ws
			save()
		}
		js(w, map[string]any{"W": x2().MW[u.ID]})
	})
	api("hidepost", func(u *User, w http.ResponseWriter, r *http.Request) {
		id := in(r)["id"]
		h := x2().Hid[u.ID]
		if fp(id) != nil && !has(h, id) {
			h = append(h, id)
			if len(h) > 500 {
				h = h[1:]
			}
			x2().Hid[u.ID] = h
			save()
		}
		js(w, ok)
	})
	api("tagfol", func(u *User, w http.ResponseWriter, r *http.Request) {
		t := strings.ToLower(strings.TrimSpace(in(r)["t"]))
		if !tagOne.MatchString(t) {
			fail(w, 400, "Invalid tag")
			return
		}
		e := x2()
		if has(e.Tags[u.ID], t) {
			e.Tags[u.ID] = del(e.Tags[u.ID], t)
		} else if len(e.Tags[u.ID]) < 50 {
			e.Tags[u.ID] = append(e.Tags[u.ID], t)
		}
		save()
		js(w, map[string]any{"Tags": e.Tags[u.ID]})
	})
}

// ---------- service worker (shows a notification for every push) ----------
const swJS = `const C='amlink-v5';
self.addEventListener('install',e=>{self.skipWaiting();e.waitUntil(caches.open(C).then(c=>c.add('/')).catch(()=>{}))});
self.addEventListener('activate',e=>e.waitUntil(caches.keys().then(k=>Promise.all(k.filter(x=>x!=C).map(x=>caches.delete(x)))).then(()=>clients.claim())));
self.addEventListener('fetch',e=>{if(e.request.mode=='navigate')e.respondWith(fetch(e.request).catch(()=>caches.match('/')))});
self.addEventListener('push',e=>{let d={};try{d=e.data?e.data.json():{}}catch(x){}e.waitUntil(self.registration.showNotification(d.title||'Amlink',{body:d.body||'You have new activity',icon:'/icon.svg',badge:'/icon.svg',tag:'amlink'}))});
self.addEventListener('notificationclick',e=>{e.notification.close();e.waitUntil(clients.matchAll({type:'window',includeUncontrolled:true}).then(l=>{for(const c of l){if('focus' in c)return c.focus()}return clients.openWindow('/')}))});
`

// ---------- web push (VAPID, no payload, so no extra library is needed) ----------
var (
	vKey      *ecdsa.PrivateKey
	vPub      string
	pushLast  = map[string]int64{}
	pushHosts = []string{"fcm.googleapis.com", ".push.services.mozilla.com", ".push.apple.com", ".notify.windows.com"}
	pushCl    = &http.Client{Timeout: 10 * time.Second}
	pushEv    = map[string]bool{"like": true, "comment": true, "reply": true, "follow": true, "msg": true, "mention": true, "freq": true, "facc": true, "repost": true, "mreact": true, "login": true, "warn": true, "susp": true, "vok": true, "vno": true, "missed": true}
)

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }
func pad32(b []byte) []byte {
	if len(b) >= 32 {
		return b
	}
	o := make([]byte, 32)
	copy(o[32-len(b):], b)
	return o
}

func vapidInit() {
	if pb, e := base64.RawURLEncoding.DecodeString(env("VAPID_PRIVATE")); e == nil && len(pb) == 32 {
		k := &ecdsa.PrivateKey{D: new(big.Int).SetBytes(pb)}
		k.PublicKey.Curve = elliptic.P256()
		k.PublicKey.X, k.PublicKey.Y = k.PublicKey.Curve.ScalarBaseMult(pb)
		vKey = k
	} else {
		k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		vKey = k
		log.Println("VAPID_PRIVATE is not set. Add this to your Render environment so alerts keep working after a restart:")
		log.Println("VAPID_PRIVATE=" + b64(pad32(k.D.Bytes())))
	}
	vPub = b64(elliptic.Marshal(elliptic.P256(), vKey.PublicKey.X, vKey.PublicKey.Y))
}

func pushOK(ep string) bool {
	u, e := url.Parse(ep)
	if e != nil || u.Scheme != "https" || u.User != nil || len(ep) > 600 {
		return false
	}
	h := strings.ToLower(u.Hostname())
	for _, s := range pushHosts {
		if h == s || (strings.HasPrefix(s, ".") && strings.HasSuffix(h, s)) {
			return true
		}
	}
	return false
}

func vapidAuth(ep string) (string, error) {
	u, e := url.Parse(ep)
	if e != nil {
		return "", e
	}
	sub := env("ADMIN_EMAIL")
	if sub == "" {
		sub = "admin@example.com"
	}
	cl, _ := json.Marshal(map[string]any{"aud": u.Scheme + "://" + u.Host, "exp": time.Now().Add(12 * time.Hour).Unix(), "sub": "mailto:" + sub})
	signing := b64([]byte(`{"typ":"JWT","alg":"ES256"}`)) + "." + b64(cl)
	sum := sha256.Sum256([]byte(signing))
	r, s, e := ecdsa.Sign(rand.Reader, vKey, sum[:])
	if e != nil {
		return "", e
	}
	sig := append(pad32(r.Bytes()), pad32(s.Bytes())...)
	return "vapid t=" + signing + "." + b64(sig) + ", k=" + vPub, nil
}

// webPushTo is called from push() while mu is held. Only users with no open app get a push.
func webPushTo(to, ev string) {
	if !pushEv[ev] || len(hub[to]) > 0 {
		return
	}
	eps := S.X2.Push[to]
	if len(eps) == 0 || now()-pushLast[to] < 15 {
		return
	}
	pushLast[to] = now()
	cp := append([]string(nil), eps...)
	go func() {
		for _, ep := range cp {
			sendPush(to, ep)
		}
	}()
}

func sendPush(user, ep string) {
	if !pushOK(ep) {
		return
	}
	auth, e := vapidAuth(ep)
	if e != nil {
		return
	}
	req, e := http.NewRequest("POST", ep, nil)
	if e != nil {
		return
	}
	req.Header.Set("Authorization", auth)
	req.Header.Set("TTL", "86400")
	req.Header.Set("Urgency", "normal")
	resp, e := pushCl.Do(req)
	if e != nil {
		return
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	resp.Body.Close()
	if resp.StatusCode == 404 || resp.StatusCode == 410 {
		mu.Lock()
		e2 := x2()
		if e2.Push != nil {
			e2.Push[user] = del(e2.Push[user], ep)
		}
		save()
		mu.Unlock()
	}
}

// ---------- comment ids (needed for replies) ----------
func fillCIDs() {
	for _, p := range S.Posts {
		for i := range p.Comments {
			if p.Comments[i].ID == "" {
				p.Comments[i].ID = uid()
			}
		}
	}
}

// ---------- link previews (safe against server-side request forgery) ----------
type lpEnt struct {
	d  map[string]string
	ts int64
}

var (
	lpMu    sync.Mutex
	lpCache = map[string]lpEnt{}
	metaRe  = regexp.MustCompile(`(?is)<meta\s[^>]*>`)
	attrRe  = regexp.MustCompile(`(?is)([\w:-]+)\s*=\s*(?:"([^"]*)"|'([^']*)')`)
	titleRe = regexp.MustCompile(`(?is)<title[^>]*>(.*?)</title>`)
	lpClean = strings.NewReplacer("'", "%27", "(", "%28", ")", "%29", "\"", "%22", "\\", "%5C", " ", "%20", "<", "%3C", ">", "%3E")
)

func publicIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
		return false
	}
	if v4 := ip.To4(); v4 != nil && v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127 {
		return false
	}
	return true
}

func lpClient() *http.Client {
	d := &net.Dialer{Timeout: 5 * time.Second, Control: func(network, address string, c syscall.RawConn) error {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return err
		}
		ip := net.ParseIP(host)
		if ip == nil || !publicIP(ip) {
			return errors.New("blocked address")
		}
		return nil
	}}
	tr := &http.Transport{DialContext: d.DialContext, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 6 * time.Second, DisableKeepAlives: true}
	return &http.Client{Timeout: 8 * time.Second, Transport: tr, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 3 {
			return errors.New("too many redirects")
		}
		return nil
	}}
}

func parseMeta(body string) map[string]string {
	m := map[string]string{}
	for _, tag := range metaRe.FindAllString(body, -1) {
		key, val := "", ""
		for _, a := range attrRe.FindAllStringSubmatch(tag, -1) {
			v := a[2]
			if v == "" {
				v = a[3]
			}
			switch strings.ToLower(a[1]) {
			case "property", "name":
				key = strings.ToLower(v)
			case "content":
				val = v
			}
		}
		if key != "" && val != "" {
			if _, k := m[key]; !k {
				m[key] = html.UnescapeString(val)
			}
		}
	}
	return m
}

func first(m map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := strings.TrimSpace(m[k]); v != "" {
			return v
		}
	}
	return ""
}

func fetchPreview(raw string) (map[string]string, error) {
	u, e := url.Parse(raw)
	if e != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || len(raw) > 500 {
		return nil, errors.New("bad url")
	}
	if p := u.Port(); p != "" && p != "80" && p != "443" {
		return nil, errors.New("bad port")
	}
	req, _ := http.NewRequest("GET", u.String(), nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; AmlinkBot/1.0)")
	req.Header.Set("Accept", "text/html,application/xhtml+xml")
	resp, e := lpClient().Do(req)
	if e != nil {
		return nil, e
	}
	defer resp.Body.Close()
	if resp.StatusCode > 299 || !strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "html") {
		return nil, errors.New("no preview")
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 512<<10))
	body := string(b)
	m := parseMeta(body)
	title := first(m, "og:title", "twitter:title")
	if title == "" {
		if t := titleRe.FindStringSubmatch(body); t != nil {
			title = strings.TrimSpace(html.UnescapeString(t[1]))
		}
	}
	desc := first(m, "og:description", "twitter:description", "description")
	img := first(m, "og:image", "twitter:image")
	final := resp.Request.URL
	if img != "" {
		if iu, e := final.Parse(img); e == nil && (iu.Scheme == "http" || iu.Scheme == "https") {
			img = lpClean.Replace(iu.String())
		} else {
			img = ""
		}
	}
	site := first(m, "og:site_name")
	if site == "" {
		site = strings.TrimPrefix(strings.ToLower(final.Hostname()), "www.")
	}
	if title == "" && desc == "" && img == "" {
		return nil, errors.New("no preview")
	}
	return map[string]string{"url": final.String(), "title": trunc(title, 120), "desc": trunc(desc, 200), "image": img, "site": trunc(site, 60)}, nil
}

// ---------- safety scan (OpenAI moderation, free). Set OPENAI_API_KEY to turn on. ----------
// modBlocked returns true when any of the images (data URLs) should be blocked.
// If the key is missing or the check fails, nothing is blocked.
func modBlocked(urls []string) bool {
	key := env("OPENAI_API_KEY")
	if key == "" || len(urls) == 0 {
		return false
	}
	items := []any{}
	for _, u := range urls {
		items = append(items, map[string]any{"type": "image_url", "image_url": map[string]string{"url": u}})
	}
	body, _ := json.Marshal(map[string]any{"model": "omni-moderation-latest", "input": items})
	req, _ := http.NewRequest("POST", "https://api.openai.com/v1/moderations", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)
	resp, e := hc.Do(req)
	if e != nil {
		log.Println("scan failed:", e)
		return false
	}
	defer resp.Body.Close()
	var o struct {
		Results []struct {
			Categories map[string]bool    `json:"categories"`
			Scores     map[string]float64 `json:"category_scores"`
		} `json:"results"`
	}
	json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&o)
	if resp.StatusCode != 200 || len(o.Results) == 0 {
		log.Println("scan status:", resp.StatusCode)
		return false
	}
	for _, r := range o.Results {
		c, s := r.Categories, r.Scores
		if c["sexual/minors"] || s["sexual/minors"] > 0.2 || c["sexual"] || s["sexual"] > 0.7 || c["violence/graphic"] || s["violence/graphic"] > 0.7 {
			return true
		}
	}
	return false
}

func scanMedia(ct string, data []byte) bool {
	if env("OPENAI_API_KEY") == "" || len(data) > 15<<20 {
		return false
	}
	switch ct {
	case "image/jpeg", "image/png", "image/webp", "image/gif":
	default:
		return false
	}
	return modBlocked([]string{"data:" + ct + ";base64," + base64.StdEncoding.EncodeToString(data)})
}

// ---------- routes ----------
func extra5() {
	ok := map[string]bool{"ok": true}

	http.HandleFunc("/api/pushkey", func(w http.ResponseWriter, r *http.Request) {
		js(w, map[string]string{"key": vPub})
	})

	api("pushsub", func(u *User, w http.ResponseWriter, r *http.Request) {
		ep := in(r)["endpoint"]
		if !pushOK(ep) {
			fail(w, 400, "This browser is not supported for alerts")
			return
		}
		e := x2()
		if e.Push == nil {
			e.Push = map[string][]string{}
		}
		for k, l := range e.Push {
			if k != u.ID && has(l, ep) {
				e.Push[k] = del(l, ep)
			}
		}
		if !has(e.Push[u.ID], ep) {
			l := append(e.Push[u.ID], ep)
			if len(l) > 5 {
				l = l[len(l)-5:]
			}
			e.Push[u.ID] = l
			save()
		}
		js(w, ok)
	})

	api("pushoff", func(u *User, w http.ResponseWriter, r *http.Request) {
		e := x2()
		if e.Push != nil {
			e.Push[u.ID] = del(e.Push[u.ID], in(r)["endpoint"])
			save()
		}
		js(w, ok)
	})

	// threaded replies: replies to a reply attach to the top comment (two levels, like Instagram)
	api("comment3", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		p, t := fp(b["id"]), strings.TrimSpace(b["text"])
		if p == nil || t == "" || rn(t) > 300 || hid(u, p.User) {
			fail(w, 400, "Invalid comment")
			return
		}
		if !canComment(u, p) {
			fail(w, 403, "The author limited who can comment on this post")
			return
		}
		fillCIDs()
		c := Comment{ID: uid(), User: u.ID, Text: t, TS: now()}
		replyTo := ""
		if pr := b["parent"]; pr != "" {
			for _, o := range p.Comments {
				if o.ID == pr {
					c.Parent = o.ID
					if o.Parent != "" {
						c.Parent = o.Parent
					}
					replyTo = o.User
					break
				}
			}
		}
		p.Comments = append(p.Comments, c)
		if ru := S.Users[replyTo]; replyTo != "" && ru != nil && !blk(u, ru) {
			note(replyTo, u.ID, "reply", p.ID)
			if p.User != replyTo {
				note(p.User, u.ID, "comment", p.ID)
			}
		} else {
			note(p.User, u.ID, "comment", p.ID)
		}
		mentions(u, t, p.ID)
		save()
		js(w, pv(p))
	})

	// link preview: the slow network fetch runs without holding the big lock
	http.HandleFunc("/api/preview", func(w http.ResponseWriter, r *http.Request) {
		if authUser(r) == nil {
			fail(w, 429, "Slow down a little")
			return
		}
		raw := strings.TrimSpace(r.URL.Query().Get("url"))
		lpMu.Lock()
		if c, k := lpCache[raw]; k && now()-c.ts < 6*3600 {
			lpMu.Unlock()
			if c.d == nil {
				fail(w, 404, "No preview")
				return
			}
			js(w, c.d)
			return
		}
		lpMu.Unlock()
		d, e := fetchPreview(raw)
		lpMu.Lock()
		if len(lpCache) > 500 {
			lpCache = map[string]lpEnt{}
		}
		lpCache[raw] = lpEnt{d, now()}
		lpMu.Unlock()
		if e != nil {
			fail(w, 404, "No preview")
			return
		}
		js(w, d)
	})
}

// =====================================================================
// ---------- v12: row-per-item storage, video scan, pinned comments, chat search ----------
// =====================================================================

var (
	rowTables  = []string{"app_users", "app_posts", "app_msgs"}
	rowH       = map[string]map[string][32]byte{"app_users": {}, "app_posts": {}, "app_msgs": {}}
	rowsMarked bool
	vtMu       sync.Mutex
	vToks      = map[string]vTokEnt{}
)

type rowT struct {
	id string
	ts int64
	b  []byte
}
type vTokEnt struct {
	user string
	exp  int64
}

// pageRows reads a table in pages of 500 so one response never gets too big
func pageRows(tbl string, each func(raw []byte) error) error {
	last := "0"
	for {
		rows, e := neon("SELECT n::text AS n, data::text AS d FROM "+tbl+" WHERE n > $1::bigint ORDER BY n LIMIT 500", last)
		if e != nil {
			return e
		}
		for _, r := range rows {
			if err := each(rawJSON(r["d"])); err != nil {
				return err
			}
			last = string(rawJSON(r["n"]))
		}
		if len(rows) < 500 {
			return nil
		}
	}
}

// loadTables returns (false, nil) when the database has not been moved to rows yet.
func loadTables() (bool, error) {
	m, e := neon("SELECT 1 AS x FROM store WHERE key='rows_v1'")
	if e != nil {
		return false, e
	}
	if len(m) == 0 {
		return false, nil
	}
	rr, e := neon("SELECT value::text AS v FROM store WHERE key='s_rest'")
	if e != nil || len(rr) == 0 {
		return false, errors.New("the rest row is missing")
	}
	if err := json.Unmarshal(rawJSON(rr[0]["v"]), &S); err != nil {
		return false, err
	}
	S.Users, S.Posts, S.Msgs = map[string]*User{}, nil, nil
	if err := pageRows("app_users", func(b []byte) error {
		var u User
		if err := json.Unmarshal(b, &u); err != nil {
			return err
		}
		S.Users[u.ID] = &u
		return nil
	}); err != nil {
		return false, err
	}
	if err := pageRows("app_posts", func(b []byte) error {
		var p Post
		if err := json.Unmarshal(b, &p); err != nil {
			return err
		}
		S.Posts = append(S.Posts, &p)
		return nil
	}); err != nil {
		return false, err
	}
	if err := pageRows("app_msgs", func(b []byte) error {
		var mm Msg
		if err := json.Unmarshal(b, &mm); err != nil {
			return err
		}
		S.Msgs = append(S.Msgs, mm)
		return nil
	}); err != nil {
		return false, err
	}
	rowsMarked = true
	primeRows()
	return true, nil
}

// primeRows remembers what is already in the database so only changed rows are written
func primeRows() {
	for id, u := range S.Users {
		b, _ := json.Marshal(u)
		rowH["app_users"][id] = sha256.Sum256(b)
	}
	for _, p := range S.Posts {
		b, _ := json.Marshal(p)
		rowH["app_posts"][p.ID] = sha256.Sum256(b)
	}
	for _, m := range S.Msgs {
		b, _ := json.Marshal(m)
		rowH["app_msgs"][m.ID] = sha256.Sum256(b)
	}
}

// syncTable writes only new or changed rows and deletes rows that no longer exist
func syncTable(tbl string, rows []rowT, hs map[string][32]byte) bool {
	type chg struct {
		id   string
		h    [32]byte
		item map[string]any
	}
	cur := make(map[string]bool, len(rows))
	var list []chg
	for _, r := range rows {
		if r.id == "" || cur[r.id] {
			continue
		}
		cur[r.id] = true
		h := sha256.Sum256(r.b)
		if o, k := hs[r.id]; k && o == h {
			continue
		}
		list = append(list, chg{r.id, h, map[string]any{"id": r.id, "ts": r.ts, "data": json.RawMessage(r.b)}})
	}
	good := true
	for i := 0; i < len(list); i += 150 {
		j := i + 150
		if j > len(list) {
			j = len(list)
		}
		items := make([]map[string]any, 0, j-i)
		for _, c := range list[i:j] {
			items = append(items, c.item)
		}
		b, _ := json.Marshal(items)
		if _, e := neon("INSERT INTO "+tbl+"(id,ts,data) SELECT t.e->>'id', (t.e->>'ts')::bigint, t.e->'data' FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY AS t(e,o) ORDER BY t.o ON CONFLICT (id) DO UPDATE SET ts=EXCLUDED.ts, data=EXCLUDED.data", string(b)); e != nil {
			log.Println("save failed for", tbl, e)
			good = false
			break
		}
		for _, c := range list[i:j] {
			hs[c.id] = c.h
		}
	}
	var gone []string
	for id := range hs {
		if !cur[id] {
			gone = append(gone, id)
		}
	}
	for i := 0; i < len(gone); i += 400 {
		j := i + 400
		if j > len(gone) {
			j = len(gone)
		}
		b, _ := json.Marshal(gone[i:j])
		if _, e := neon("DELETE FROM "+tbl+" WHERE id IN (SELECT jsonb_array_elements_text($1::jsonb))", string(b)); e != nil {
			good = false
			break
		}
		for _, id := range gone[i:j] {
			delete(hs, id)
		}
	}
	return good
}

// vidOK: when the scan is on, a video upload needs a one-time token from /api/vscan
func vidOK(u *User, ct, tok string) bool {
	if !strings.HasPrefix(ct, "video/") || env("OPENAI_API_KEY") == "" {
		return true
	}
	vtMu.Lock()
	defer vtMu.Unlock()
	e, k := vToks[tok]
	if !k || e.user != u.ID || e.exp < now() {
		return false
	}
	delete(vToks, tok)
	return true
}

func extra6() {
	ok := map[string]bool{"ok": true}

	// the browser sends 3 still frames of the video; if they pass, it gets a one-time upload token
	http.HandleFunc("/api/vscan", func(w http.ResponseWriter, r *http.Request) {
		u := authUser(r)
		if u == nil {
			fail(w, 429, "Slow down a little")
			return
		}
		var b struct{ Frames []string }
		if json.NewDecoder(http.MaxBytesReader(w, r.Body, 3<<20)).Decode(&b) != nil || len(b.Frames) < 1 || len(b.Frames) > 4 {
			fail(w, 400, "We could not check this video")
			return
		}
		for _, f := range b.Frames {
			if !strings.HasPrefix(f, "data:image/jpeg;base64,") || len(f) > 700000 {
				fail(w, 400, "We could not check this video")
				return
			}
		}
		if modBlocked(b.Frames) {
			fail(w, 400, "This video breaks Amlink's content rules")
			return
		}
		tok := uid() + uid()
		vtMu.Lock()
		for t, e := range vToks {
			if e.exp < now() {
				delete(vToks, t)
			}
		}
		vToks[tok] = vTokEnt{u.ID, now() + 600}
		vtMu.Unlock()
		js(w, map[string]string{"token": tok})
	})

	// pin one top-level comment on your own post
	api("pinc", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		p := fp(b["id"])
		if p == nil || p.User != u.ID {
			fail(w, 400, "Only the post owner can pin a comment")
			return
		}
		fillCIDs()
		found := false
		for _, c := range p.Comments {
			if c.ID == b["cid"] {
				if c.Parent != "" {
					fail(w, 400, "Pin a main comment, not a reply")
					return
				}
				found = true
				break
			}
		}
		if !found {
			fail(w, 404, "Comment not found")
			return
		}
		e := x2()
		if e.PinC[p.ID] == b["cid"] {
			delete(e.PinC, p.ID)
		} else {
			e.PinC[p.ID] = b["cid"]
		}
		save()
		js(w, pv(p))
	})

	// search inside one conversation
	api("msearch", func(u *User, w http.ResponseWriter, r *http.Request) {
		p := r.URL.Query().Get("w")
		q := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
		out := []map[string]any{}
		o := S.Users[p]
		if rn(q) < 2 || o == nil || blk(u, o) {
			js(w, out)
			return
		}
		for i := len(S.Msgs) - 1; i >= 0 && len(out) < 40; i-- {
			m := S.Msgs[i]
			if !((m.From == u.ID && m.To == p) || (m.From == p && m.To == u.ID)) {
				continue
			}
			if strings.Contains(strings.ToLower(m.Text), q) {
				out = append(out, map[string]any{"ID": m.ID, "Me": m.From == u.ID, "TS": m.TS, "Text": trunc(m.Text, 120)})
			}
		}
		js(w, out)
	})
	_ = ok
}

// =====================================================================
// ---------- v13: verification requests, communities, disappearing messages, calls ----------
// =====================================================================

type VRq struct {
	Reason, Link, Status string
	TS                   int64
}
type Comm struct {
	ID, Name, Desc, Rules, Owner                             string
	Category, Tags, Welcome, Announcement, Pinned, President string
	TS                                                       int64
	Mods, Members, Banned                                    []string
}
type CommElection struct {
	Period     string
	Candidates []string
	Votes      map[string]string // voter ID -> candidate ID
}

func fcm(id string) *Comm {
	for _, c := range S.X2.Comms {
		if c.ID == id {
			return c
		}
	}
	return nil
}
func commMod(cid, uid string) bool {
	if cid == "" {
		return false
	}
	c := fcm(cid)
	return c != nil && (c.Owner == uid || has(c.Mods, uid))
}
func dropCommPosts(cid string) {
	var fs []string
	ps := S.Posts[:0]
	for _, p := range S.Posts {
		if p.Comm == cid {
			fs = append(fs, p.Media)
			fs = append(fs, p.More...)
			continue
		}
		ps = append(ps, p)
	}
	S.Posts = ps
	dropFiles(fs...)
}

// purgeComms removes a deleted account from every community (call with mu held)
func purgeComms(id string) {
	delete(S.X2.VReq, id)
	keep := S.X2.Comms[:0]
	for _, c := range S.X2.Comms {
		c.Members, c.Mods, c.Banned = del(c.Members, id), del(c.Mods, id), del(c.Banned, id)
		if c.Owner == id {
			if len(c.Mods) > 0 {
				c.Owner = c.Mods[0]
			} else if len(c.Members) > 0 {
				c.Owner = c.Members[0]
			} else {
				dropCommPosts(c.ID)
				continue
			}
		}
		keep = append(keep, c)
	}
	S.X2.Comms = keep
}

func pairKey(a, b string) string {
	if a > b {
		a, b = b, a
	}
	return a + "|" + b
}

// dexp gives the expiry time for a new message in this chat (0 = never)
func dexp(a, b string) int64 {
	if s := x2().Disap[pairKey(a, b)]; s > 0 {
		return now() + s
	}
	return 0
}

func extra7() {
	ok := map[string]bool{"ok": true}
	modPaths["/api/commcreate"], modPaths["/api/commpost"], modPaths["/api/vreq"] = "t", "p", "t"

	// messages that reached their timer are removed every 30 seconds
	go func() {
		for range time.Tick(30 * time.Second) {
			mu.Lock()
			var fs []string
			keep := S.Msgs[:0]
			changed := false
			for _, m := range S.Msgs {
				if m.Exp > 0 && m.Exp <= now() {
					fs = append(fs, m.Media)
					changed = true
					continue
				}
				keep = append(keep, m)
			}
			S.Msgs = keep
			if changed {
				save()
			}
			mu.Unlock()
			dropFiles(fs...)
		}
	}()

	// ---------- verification requests ----------
	api("vreq", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		why, link := strings.TrimSpace(b["reason"]), strings.TrimSpace(b["link"])
		if u.Verified {
			fail(w, 400, "You are already verified")
			return
		}
		if rn(why) < 10 || rn(why) > 300 {
			fail(w, 400, "Write 10 to 300 characters about why you should be verified")
			return
		}
		if link != "" && (len(link) > 200 || !(strings.HasPrefix(link, "https://") || strings.HasPrefix(link, "http://"))) {
			fail(w, 400, "The link must start with http:// or https://")
			return
		}
		e := x2()
		if o := e.VReq[u.ID]; o != nil {
			if o.Status == "pending" {
				fail(w, 400, "Your request is already under review")
				return
			}
			if o.Status == "rejected" && now()-o.TS < 7*86400 {
				fail(w, 400, "You can apply again in a few days")
				return
			}
		}
		e.VReq[u.ID] = &VRq{why, link, "pending", now()}
		save()
		js(w, ok)
	})
	api("vstatus", func(u *User, w http.ResponseWriter, r *http.Request) {
		st, wait := "none", int64(0)
		if o := x2().VReq[u.ID]; o != nil {
			st = o.Status
			if st == "rejected" {
				wait = 7*86400 - (now() - o.TS)
				if wait < 0 {
					wait = 0
				}
			}
		}
		js(w, map[string]any{"Status": st, "Wait": wait})
	})
	api("vreqs", func(u *User, w http.ResponseWriter, r *http.Request) {
		if !u.Admin {
			fail(w, 403, "Admins only")
			return
		}
		type it struct {
			id string
			q  *VRq
		}
		l := []it{}
		for id, q := range x2().VReq {
			if q.Status == "pending" && S.Users[id] != nil {
				l = append(l, it{id, q})
			}
		}
		sort.Slice(l, func(a, b int) bool { return l[a].q.TS < l[b].q.TS })
		out := []map[string]any{}
		for _, i := range l {
			out = append(out, map[string]any{"U": uv(i.id), "Reason": i.q.Reason, "Link": i.q.Link, "TS": i.q.TS})
		}
		js(w, out)
	})
	api("vact", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		o, e := S.Users[b["id"]], x2()
		q := e.VReq[b["id"]]
		if !u.Admin || o == nil || q == nil || q.Status != "pending" {
			fail(w, 403, "Not allowed")
			return
		}
		if b["v"] == "1" {
			o.Verified = true
			delete(e.VReq, o.ID)
			note(o.ID, u.ID, "vok", "")
			alog(u, "approved verification for @"+o.Username)
		} else {
			q.Status, q.TS = "rejected", now()
			note(o.ID, u.ID, "vno", "")
			alog(u, "rejected verification for @"+o.Username)
		}
		save()
		js(w, ok)
	})

	// ---------- communities ----------
	api("commcreate", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		n, d, ru := strings.TrimSpace(b["name"]), strings.TrimSpace(b["desc"]), strings.TrimSpace(b["rules"])
		if rn(n) < 3 || rn(n) > 40 || rn(d) > 200 || rn(ru) > 500 || rn(b["welcome"]) > 300 {
			fail(w, 400, "The name must be 3 to 40 characters")
			return
		}
		owned := 0
		for _, c := range S.X2.Comms {
			if strings.EqualFold(c.Name, n) {
				fail(w, 400, "That name is taken")
				return
			}
			if c.Owner == u.ID {
				owned++
			}
		}
		if owned >= 5 {
			fail(w, 400, "You can own up to 5 communities")
			return
		}
		cat := strings.TrimSpace(b["category"])
		allowedCats := map[string]bool{"General": true, "Technology": true, "Gaming": true, "Education": true, "Business": true, "Entertainment": true, "Lifestyle": true, "Sports": true, "Creators": true, "Local": true}
		if !allowedCats[cat] {
			cat = "General"
		}
		tags := strings.TrimSpace(b["tags"])
		if len(tags) > 240 {
			tags = tags[:240]
		}
		c := &Comm{ID: uid(), Name: n, Desc: d, Rules: ru, Owner: u.ID, TS: now(), Members: []string{u.ID}, Category: cat, Tags: tags, Welcome: strings.TrimSpace(b["welcome"])}
		S.X2.Comms = append(S.X2.Comms, c)
		save()
		js(w, map[string]string{"id": c.ID})
	})
	api("comms", func(u *User, w http.ResponseWriter, r *http.Request) {
		q := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
		list := []*Comm{}
		for _, c := range S.X2.Comms {
			if q == "" || strings.Contains(strings.ToLower(c.Name), q) || strings.Contains(strings.ToLower(c.Desc), q) || strings.Contains(strings.ToLower(c.Tags), q) || strings.Contains(strings.ToLower(c.Category), q) {
				list = append(list, c)
			}
		}
		sort.Slice(list, func(a, b int) bool { return len(list[a].Members) > len(list[b].Members) })
		out := []map[string]any{}
		for i := 0; i < len(list); i++ {
			c := list[i]
			out = append(out, map[string]any{"ID": c.ID, "Name": c.Name, "Desc": c.Desc, "N": len(c.Members), "Joined": has(c.Members, u.ID), "Category": c.Category, "Tags": c.Tags, "Announcement": c.Announcement})
		}
		js(w, out)
	})
	api("comm", func(u *User, w http.ResponseWriter, r *http.Request) {
		c := fcm(r.URL.Query().Get("id"))
		if c == nil {
			fail(w, 404, "Community not found")
			return
		}
		ps := []map[string]any{}
		pinnedIncluded := false
		for i := len(S.Posts) - 1; i >= 0 && len(ps) < 60; i-- {
			if p := S.Posts[i]; p.Comm == c.ID && !hid(u, p.User) {
				ps = append(ps, pv(p))
				if p.ID == c.Pinned {
					pinnedIncluded = true
				}
			}
		}
		if c.Pinned != "" && !pinnedIncluded {
			for _, p := range S.Posts {
				if p.ID == c.Pinned && p.Comm == c.ID && !hid(u, p.User) {
					ps = append([]map[string]any{pv(p)}, ps...)
					break
				}
			}
		}
		mod := c.Owner == u.ID || has(c.Mods, u.ID)
		res := map[string]any{"Name": c.Name, "Desc": c.Desc, "Rules": c.Rules, "Welcome": c.Welcome, "Category": c.Category, "Tags": c.Tags, "Announcement": c.Announcement, "Pinned": c.Pinned, "N": len(c.Members), "Joined": has(c.Members, u.ID), "Owner": c.Owner == u.ID, "Mod": mod, "Posts": ps}
		if mod {
			m := []map[string]any{}
			for _, id := range c.Members {
				role := "member"
				if id == c.Owner {
					role = "owner"
				} else if has(c.Mods, id) {
					role = "mod"
				}
				m = append(m, map[string]any{"U": uv(id), "Role": role})
			}
			res["M"] = m
		}
		js(w, res)
	})
	api("commjoin", func(u *User, w http.ResponseWriter, r *http.Request) {
		c := fcm(in(r)["id"])
		if c == nil {
			fail(w, 404, "Community not found")
			return
		}
		if has(c.Banned, u.ID) {
			fail(w, 403, "You were removed from this community")
			return
		}
		if has(c.Members, u.ID) {
			if c.Owner == u.ID {
				fail(w, 400, "Owners cannot leave. Delete the community instead")
				return
			}
			c.Members, c.Mods = del(c.Members, u.ID), del(c.Mods, u.ID)
		} else {
			c.Members = append(c.Members, u.ID)
		}
		save()
		js(w, ok)
	})
	api("commpost", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		c := fcm(b["id"])
		if c == nil || !has(c.Members, u.ID) {
			fail(w, 403, "Join this community to post")
			return
		}
		if x().NoVer[u.ID] {
			fail(w, 403, "Verify your email first (check your inbox), then log in again")
			return
		}
		t := strings.TrimSpace(b["text"])
		var media []string
		for _, m := range strings.Split(b["media"], ",") {
			if m != "" {
				if !okMedia(m) {
					fail(w, 400, "Invalid post")
					return
				}
				media = append(media, m)
			}
		}
		if (t == "" && len(media) == 0) || rn(t) > 2000 || len(media) > 10 {
			fail(w, 400, "Invalid post")
			return
		}
		k := b["kind"]
		if k != "image" && k != "video" {
			k = ""
		}
		p := &Post{ID: uid(), User: u.ID, Text: t, Kind: k, TS: now(), Likes: []string{}, Comm: c.ID}
		if len(media) > 0 {
			p.Media, p.More = media[0], media[1:]
		}
		S.Posts = append(S.Posts, p)
		mentions(u, t, p.ID)
		save()
		js(w, ok)
	})
	api("commact", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		c := fcm(b["id"])
		if c == nil {
			fail(w, 404, "Community not found")
			return
		}
		owner := c.Owner == u.ID
		mod := owner || has(c.Mods, u.ID)
		id := b["uid"]
		switch b["op"] {
		case "edit":
			d, ru := strings.TrimSpace(b["desc"]), strings.TrimSpace(b["rules"])
			name, welcome := strings.TrimSpace(b["name"]), strings.TrimSpace(b["welcome"])
			cat := strings.TrimSpace(b["category"])
			allowedCats := map[string]bool{"General": true, "Technology": true, "Gaming": true, "Education": true, "Business": true, "Entertainment": true, "Lifestyle": true, "Sports": true, "Creators": true, "Local": true}
			tags := strings.TrimSpace(b["tags"])
			if !mod || rn(d) > 200 || rn(ru) > 500 || rn(welcome) > 300 || len(tags) > 240 || (cat != "" && !allowedCats[cat]) || (name != "" && (rn(name) < 3 || rn(name) > 40)) {
				fail(w, 400, "Check the community details and try again")
				return
			}
			if name != "" {
				for _, other := range S.X2.Comms {
					if other != c && strings.EqualFold(other.Name, name) {
						fail(w, 400, "That name is taken")
						return
					}
				}
				c.Name = name
			}
			c.Desc, c.Rules, c.Welcome = d, ru, welcome
			if cat != "" {
				c.Category = cat
			}
			c.Tags = tags
		case "announce":
			msg := strings.TrimSpace(b["text"])
			if !mod || rn(msg) > 500 {
				fail(w, 403, "Only moderators can publish an announcement (max 500 characters)")
				return
			}
			c.Announcement = msg
		case "pin":
			pid := strings.TrimSpace(b["post"])
			found := false
			if !mod {
				fail(w, 403, "Only moderators can pin posts")
				return
			}
			for _, p := range S.Posts {
				if p.ID == pid && p.Comm == c.ID {
					found = true
					break
				}
			}
			if !found {
				fail(w, 404, "Community post not found")
				return
			}
			c.Pinned = pid
		case "unpin":
			if !mod {
				fail(w, 403, "Only moderators can unpin posts")
				return
			}
			c.Pinned = ""
		case "mod":
			if !owner || id == c.Owner || !has(c.Members, id) {
				fail(w, 403, "Not allowed")
				return
			}
			if has(c.Mods, id) {
				c.Mods = del(c.Mods, id)
			} else {
				c.Mods = append(c.Mods, id)
			}
		case "kick":
			if !mod || id == c.Owner || id == u.ID || !has(c.Members, id) || (has(c.Mods, id) && !owner) {
				fail(w, 403, "Not allowed")
				return
			}
			c.Members, c.Mods = del(c.Members, id), del(c.Mods, id)
			if !has(c.Banned, id) {
				c.Banned = append(c.Banned, id)
			}
		case "del":
			if !owner && !u.Admin {
				fail(w, 403, "Not allowed")
				return
			}
			dropCommPosts(c.ID)
			keep := S.X2.Comms[:0]
			for _, h := range S.X2.Comms {
				if h != c {
					keep = append(keep, h)
				}
			}
			S.X2.Comms = keep
		default:
			fail(w, 400, "Unknown action")
			return
		}
		save()
		js(w, ok)
	})

	// ---------- monthly community elections ----------
	api("commelection", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		c := fcm(r.URL.Query().Get("id"))
		if c == nil {
			c = fcm(b["id"])
		}
		if c == nil {
			fail(w, 404, "Community not found")
			return
		}
		period := time.Now().UTC().Format("2006-01")
		e := x2()
		election := e.CommElections[c.ID]
		if election == nil || election.Period != period {
			election = &CommElection{Period: period, Candidates: []string{}, Votes: map[string]string{}}
			e.CommElections[c.ID] = election
		}
		followers, engagement := 0, 0
		for _, other := range S.Users {
			if has(other.Following, u.ID) {
				followers++
			}
		}
		for _, p := range S.Posts {
			if p.Comm == c.ID && p.TS >= now()-30*86400 {
				if p.User == u.ID {
					engagement += 1 + len(p.Likes) + len(p.Comments)
				} else {
					for _, cm := range p.Comments {
						if cm.User == u.ID {
							engagement++
						}
					}
				}
			}
		}
		joined := has(c.Members, u.ID)
		canNominate := joined && followers >= 100 && engagement >= 500 && !has(election.Candidates, u.ID)
		switch b["op"] {
		case "nominate":
			if !joined {
				fail(w, 403, "Join this community first")
				return
			}
			if followers < 100 || engagement < 500 {
				fail(w, 403, "You need 100 followers and 500 community engagement points to nominate")
				return
			}
			if !has(election.Candidates, u.ID) {
				election.Candidates = append(election.Candidates, u.ID)
			}
			save()
			js(w, map[string]bool{"ok": true})
			return
		case "vote":
			candidate := b["candidate"]
			if !joined || !has(election.Candidates, candidate) || candidate == u.ID {
				fail(w, 400, "Join the community and choose a valid candidate")
				return
			}
			election.Votes[u.ID] = candidate
			save()
			js(w, map[string]bool{"ok": true})
			return
		}
		type candidateView struct {
			U     map[string]any
			Votes int
		}
		counts := map[string]int{}
		for _, candidate := range election.Votes {
			counts[candidate]++
		}
		candidates := []candidateView{}
		for _, id := range election.Candidates {
			if S.Users[id] != nil {
				candidates = append(candidates, candidateView{uv(id), counts[id]})
			}
		}
		if c.President == "" {
			best := ""
			max := 0
			for id, n := range counts {
				if n > max {
					best, max = id, n
				}
			}
			if best != "" {
				c.President = best
				save()
			}
		}
		var president any
		if c.President != "" && S.Users[c.President] != nil {
			president = uv(c.President)
		}
		js(w, map[string]any{"Period": period, "President": president, "Candidates": candidates, "Joined": joined, "CanNominate": canNominate, "Eligible": joined && followers >= 100 && engagement >= 500, "Followers": followers, "Engagement": engagement, "MyVote": election.Votes[u.ID]})
	})

	// ---------- disappearing messages ----------
	api("disap", func(u *User, w http.ResponseWriter, r *http.Request) {
		js(w, map[string]int64{"Secs": x2().Disap[pairKey(u.ID, r.URL.Query().Get("w"))]})
	})
	api("disapset", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		o := S.Users[b["to"]]
		secs, _ := strconv.ParseInt(b["secs"], 10, 64)
		if o == nil || blk(u, o) || (secs != 0 && secs != 86400 && secs != 604800) {
			fail(w, 400, "Invalid setting")
			return
		}
		e, key, label := x2(), pairKey(u.ID, o.ID), "off"
		if secs == 0 {
			delete(e.Disap, key)
		} else {
			e.Disap[key] = secs
			label = "24 hours"
			if secs == 604800 {
				label = "7 days"
			}
		}
		S.Msgs = append(S.Msgs, Msg{From: u.ID, To: o.ID, Text: "⏱ Disappearing messages: " + label, TS: now()})
		note(o.ID, u.ID, "msg", "")
		save()
		js(w, ok)
	})

	// ---------- voice and video calls (WebRTC): the server only passes the setup messages along ----------
	api("callsig", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		o := S.Users[b["to"]]
		t := b["type"]
		if o == nil || o.Banned || blk(u, o) {
			fail(w, 400, "You cannot call this person")
			return
		}
		if !has(u.Following, o.ID) && !has(o.Following, u.ID) {
			fail(w, 403, "Follow each other to call")
			return
		}
		switch t {
		case "offer", "answer", "ice", "end", "busy", "decline":
		default:
			fail(w, 400, "Invalid call message")
			return
		}
		if len(b["data"]) > 20000 {
			fail(w, 400, "Invalid call message")
			return
		}
		msg, _ := json.Marshal(map[string]any{"from": u.ID, "type": t, "data": b["data"], "U": uv(u.ID)})
		if t == "offer" && len(hub[o.ID]) == 0 {
			note(o.ID, u.ID, "missed", "")
		}
		push(o.ID, "call:"+string(msg))
		js(w, ok)
	})
	api("ice", func(u *User, w http.ResponseWriter, r *http.Request) {
		js(w, map[string]string{"url": env("TURN_URL"), "user": env("TURN_USER"), "pass": env("TURN_PASS")})
	})
}

// =====================================================================
// ---------- v14: studio options, scheduled posts, collections, comment controls ----------
// PASTE THIS AT THE VERY BOTTOM OF main.go
// =====================================================================

type POpt struct {
	Poster string
	Alt    string
	CM     int     // who can comment: 0 everyone, 1 people the author follows, 2 nobody
	Dur    float64 // length in seconds of an edited video
	Duet   string  // id of the reel this one is a duet with
}
type SchP struct {
	ID, User, Text, Media, Kind, Poll string
	More                              []string
	At                                int64
	Sens                              bool
	Opt                               POpt
}
type Ext3 struct {
	PO   map[string]*POpt
	Sch  []*SchP
	Coll map[string]map[string][]string
}

func x3() *Ext3 {
	e := &S.X3
	if e.PO == nil {
		e.PO = map[string]*POpt{}
	}
	if e.Coll == nil {
		e.Coll = map[string]map[string][]string{}
	}
	return e
}

// canComment: the post owner chooses who may comment
func canComment(u *User, p *Post) bool {
	if p.User == u.ID || u.Admin {
		return true
	}
	o := x3().PO[p.ID]
	if o == nil || o.CM == 0 {
		return true
	}
	if o.CM == 2 {
		return false
	}
	a := S.Users[p.User]
	return a != nil && has(a.Following, u.ID)
}

// buildPost turns a studio request into a real post (call with mu held)
func buildPost(s *SchP) *Post {
	p := &Post{ID: uid(), User: s.User, Text: s.Text, Kind: s.Kind, TS: now(), Likes: []string{}}
	if s.Poll != "" {
		var o []string
		for _, v := range strings.Split(s.Poll, "|") {
			if v = strings.TrimSpace(v); v != "" {
				o = append(o, v)
			}
		}
		if len(o) >= 2 && len(o) <= 4 {
			p.Poll = &Poll{o, map[string]int{}}
		}
	}
	if s.Media != "" {
		p.Media, p.More = s.Media, s.More
	}
	S.Posts = append(S.Posts, p)
	if s.Kind == "video" {
		award(s.User, "reel")
	} else {
		award(s.User, "post")
	}
	if s.Sens {
		x().Sens[p.ID] = true
	}
	if s.Opt.Poster != "" || s.Opt.Alt != "" || s.Opt.CM != 0 || s.Opt.Dur > 0 || s.Opt.Duet != "" {
		o := s.Opt
		x3().PO[p.ID] = &o
	}
	if u := S.Users[s.User]; u != nil {
		mentions(u, s.Text, p.ID)
	}
	return p
}

func init() {
	modPaths["/api/post4"], modPaths["/api/colladd"] = "p", "t"
	extra8()
}

func extra8() {
	ok := map[string]bool{"ok": true}

	// scheduled posts go live from here, checked every 20 seconds
	go func() {
		for range time.Tick(20 * time.Second) {
			mu.Lock()
			e := x3()
			keep := e.Sch[:0]
			changed := false
			for _, s := range e.Sch {
				if s.At > now() {
					keep = append(keep, s)
					continue
				}
				if u := S.Users[s.User]; u != nil && !u.Banned {
					buildPost(s)
				}
				changed = true
			}
			e.Sch = keep
			if changed {
				save()
			}
			mu.Unlock()
		}
	}()

	// studio: post or reel with cover, alt text, comment control, poll, schedule
	api("post4", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		if x().NoVer[u.ID] {
			fail(w, 403, "Verify your email first (check your inbox), then log in again")
			return
		}
		t := strings.TrimSpace(b["text"])
		var media []string
		for _, m := range strings.Split(b["media"], ",") {
			if m != "" {
				if !okMedia(m) {
					fail(w, 400, "Invalid post")
					return
				}
				media = append(media, m)
			}
		}
		k := b["kind"]
		if k != "image" && k != "video" {
			k = ""
		}
		if k == "" && len(media) > 0 {
			k = "image"
		}
		poll := ""
		if b["poll"] != "" {
			n := 0
			for _, s := range strings.Split(b["poll"], "|") {
				if s = strings.TrimSpace(s); s != "" && rn(s) <= 40 {
					n++
				}
			}
			if n < 2 || n > 4 {
				fail(w, 400, "A poll needs 2 to 4 options")
				return
			}
			poll = b["poll"]
		}
		poster := b["poster"]
		if !okMedia(poster) {
			fail(w, 400, "Invalid post")
			return
		}
		cm, _ := strconv.Atoi(b["cm"])
		if cm < 0 || cm > 2 {
			cm = 0
		}
		dur, _ := strconv.ParseFloat(b["dur"], 64)
		if dur < 0 || dur > 600 || dur != dur {
			dur = 0
		}
		duet := ""
		if d := fp(b["duet"]); d != nil && d.Kind == "video" && !hid(u, d.User) {
			duet = d.ID
		}
		if (t == "" && len(media) == 0 && poll == "") || rn(t) > 2000 || len(media) > 10 {
			fail(w, 400, "Invalid post")
			return
		}
		s := &SchP{ID: uid(), User: u.ID, Text: t, Kind: k, Poll: poll, Sens: b["sens"] == "1", Opt: POpt{poster, trunc(strings.TrimSpace(b["alt"]), 200), cm, dur, duet}}
		if len(media) > 0 {
			s.Media, s.More = media[0], media[1:]
		}
		at, _ := strconv.ParseInt(b["at"], 10, 64)
		if at > now()+90 {
			if at > now()+60*86400 {
				fail(w, 400, "You can schedule up to 60 days ahead")
				return
			}
			n := 0
			for _, o := range x3().Sch {
				if o.User == u.ID {
					n++
				}
			}
			if n >= 20 {
				fail(w, 400, "You can have 20 scheduled posts at a time")
				return
			}
			s.At = at
			x3().Sch = append(x3().Sch, s)
			save()
			js(w, map[string]any{"ok": true, "scheduled": at})
			return
		}
		p := buildPost(s)
		if duet != "" {
			if d := fp(duet); d != nil {
				note(d.User, u.ID, "duet", d.ID)
			}
		}
		save()
		js(w, map[string]any{"ok": true, "id": p.ID})
	})
	api("sched", func(u *User, w http.ResponseWriter, r *http.Request) {
		out := []map[string]any{}
		for _, s := range x3().Sch {
			if s.User == u.ID {
				out = append(out, map[string]any{"ID": s.ID, "At": s.At, "Text": s.Text, "Kind": s.Kind, "Media": s.Media})
			}
		}
		js(w, out)
	})
	api("schedel", func(u *User, w http.ResponseWriter, r *http.Request) {
		id := in(r)["id"]
		e := x3()
		keep := []*SchP{}
		for _, s := range e.Sch {
			if !(s.ID == id && s.User == u.ID) {
				keep = append(keep, s)
			}
		}
		e.Sch = keep
		save()
		js(w, ok)
	})

	// collections for saved posts
	api("coll", func(u *User, w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		cs := x3().Coll[u.ID]
		names := make([]string, 0, len(cs))
		for n := range cs {
			names = append(names, n)
		}
		sort.Strings(names)
		out := []map[string]any{}
		for _, n := range names {
			cover := ""
			for _, pid := range cs[n] {
				if p := fp(pid); p != nil {
					if p.Kind == "video" {
						if o := x3().PO[p.ID]; o != nil {
							cover = o.Poster
						}
					} else {
						cover = p.Media
					}
					if cover != "" {
						break
					}
				}
			}
			out = append(out, map[string]any{"Name": n, "N": len(cs[n]), "Has": has(cs[n], id), "Cover": cover})
		}
		js(w, out)
	})
	api("colladd", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		n := strings.TrimSpace(b["name"])
		p := fp(b["id"])
		if rn(n) < 1 || rn(n) > 30 || p == nil || hid(u, p.User) {
			fail(w, 400, "Invalid collection")
			return
		}
		e := x3()
		if e.Coll[u.ID] == nil {
			e.Coll[u.ID] = map[string][]string{}
		}
		cs := e.Coll[u.ID]
		if _, k := cs[n]; !k && len(cs) >= 20 {
			fail(w, 400, "You can have up to 20 collections")
			return
		}
		had := has(cs[n], p.ID)
		if had {
			cs[n] = del(cs[n], p.ID)
		} else if len(cs[n]) < 500 {
			cs[n] = append(cs[n], p.ID)
		}
		if cs[n] == nil {
			cs[n] = []string{}
		}
		save()
		js(w, map[string]bool{"Has": !had})
	})
	api("colldel", func(u *User, w http.ResponseWriter, r *http.Request) {
		if cs := x3().Coll[u.ID]; cs != nil {
			delete(cs, in(r)["name"])
			save()
		}
		js(w, ok)
	})
	api("collget", func(u *User, w http.ResponseWriter, r *http.Request) {
		out := []map[string]any{}
		for _, id := range x3().Coll[u.ID][r.URL.Query().Get("name")] {
			if p := fp(id); p != nil && !hid(u, p.User) {
				out = append(out, pv(p))
			}
		}
		js(w, out)
	})
}

// =====================================================================
// ---------- Batch 1: XP, streaks, coins, badges, challenges, arcade ----------
// =====================================================================

type GP struct {
	ID                      string
	XP, Coins, Streak, Best int
	Day, Claim, CDay        string
	Badges, Done            []string
	Cnt                     map[string]int
}
type Ext4 struct {
	G  map[string]*GP
	Sc map[string]map[string]int
}

func x4() *Ext4 {
	e := &S.X4
	if e.G == nil {
		e.G = map[string]*GP{}
	}
	if e.Sc == nil {
		e.Sc = map[string]map[string]int{}
	}
	return e
}
func today() string  { return time.Now().UTC().Format("2006-01-02") }
func lvl(xp int) int { return int(math.Sqrt(float64(xp)/50)) + 1 }

func gpf(id string) *GP {
	e := x4()
	g := e.G[id]
	if g == nil {
		g = &GP{}
		e.G[id] = g
	}
	g.ID = id
	if g.Cnt == nil {
		g.Cnt = map[string]int{}
	}
	if g.CDay != today() { // new day: reset daily counters, keep lifetime ones (t_*)
		n := map[string]int{}
		for k, v := range g.Cnt {
			if strings.HasPrefix(k, "t_") {
				n[k] = v
			}
		}
		g.Cnt, g.CDay, g.Done = n, today(), nil
	}
	return g
}

var chal = []struct {
	K  string
	N  int
	XP int
	T  string
}{{"post", 1, 40, "Post something"}, {"comment", 3, 30, "Comment 3 times"}, {"like", 5, 20, "Like 5 posts"}}

var xpTab = map[string]int{"post": 15, "reel": 25, "comment": 5, "like": 1, "follow": 3, "msg": 1, "reply": 4, "play": 2}

func badge(g *GP, b string) {
	if !has(g.Badges, b) {
		g.Badges = append(g.Badges, b)
		g.Coins += 20
		ledger(g.ID, 20, "badge", "Achievement badge unlocked")
	}
}
func checkBadges(g *GP) {
	if g.Cnt["t_post"] >= 1 {
		badge(g, "first_post")
	}
	if g.Cnt["t_reel"] >= 1 {
		badge(g, "first_reel")
	}
	if g.Cnt["t_comment"] >= 50 {
		badge(g, "chatty")
	}
	if g.Streak >= 7 {
		badge(g, "streak7")
	}
	if lvl(g.XP) >= 5 {
		badge(g, "level5")
	}
}

// award is called with mu held
func award(id, kind string) {
	xp, k := xpTab[kind]
	if !k || S.Users[id] == nil {
		return
	}
	g := gpf(id)
	g.Cnt[kind]++
	g.Cnt["t_"+kind]++
	if g.Cnt[kind] <= 30 { // daily cap per action, stops farming
		g.XP += xp
	}
	for _, c := range chal {
		if c.K == kind && g.Cnt[kind] >= c.N && !has(g.Done, c.K) {
			g.Done = append(g.Done, c.K)
			g.XP += c.XP
			g.Coins += 10
			ledger(id, 10, "challenge", "Daily challenge: "+c.T)
		}
	}
	checkBadges(g)
}

func touch(id string) {
	g := gpf(id)
	t := today()
	if g.Day == t {
		return
	}
	y := time.Now().UTC().AddDate(0, 0, -1).Format("2006-01-02")
	if g.Day == y {
		g.Streak++
	} else {
		g.Streak = 1
	}
	g.Day = t
	if g.Streak > g.Best {
		g.Best = g.Streak
	}
	g.XP += 10
	checkBadges(g)
}

func init() { extra9() }

func extra9() {
	caps := map[string]int{"snake": 2000, "memory": 1000, "streetracer": 100000, "skyparkour": 100000, "lostisland": 100000, "neonrunner": 100000}

	api("gme", func(u *User, w http.ResponseWriter, r *http.Request) {
		touch(u.ID)
		g := gpf(u.ID)
		l := lvl(g.XP)
		ch := []map[string]any{}
		for _, c := range chal {
			h := g.Cnt[c.K]
			if h > c.N {
				h = c.N
			}
			ch = append(ch, map[string]any{"T": c.T, "N": c.N, "Have": h, "Done": has(g.Done, c.K), "XP": c.XP})
		}
		save()
		js(w, map[string]any{"XP": g.XP, "Level": l, "From": 50 * (l - 1) * (l - 1), "Next": 50 * l * l, "Coins": g.Coins, "Streak": g.Streak, "Best": g.Best, "Badges": g.Badges, "Claim": g.Claim != today(), "Chal": ch, "Inf": u.Admin})
	})

	api("gclaim", func(u *User, w http.ResponseWriter, r *http.Request) {
		g := gpf(u.ID)
		if g.Claim == today() {
			fail(w, 400, "You already claimed today. Come back tomorrow")
			return
		}
		b := make([]byte, 1)
		rand.Read(b)
		prizes := []int{10, 10, 15, 20, 25, 50, 100}
		p := prizes[int(b[0])%len(prizes)]
		st := g.Streak
		if st > 7 {
			st = 7
		}
		p += st * 5
		g.Coins += p
		ledger(u.ID, p, "reward", "Daily reward")
		g.Claim = today()
		save()
		js(w, map[string]int{"Won": p, "Coins": g.Coins})
	})

	api("gscore", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		game := b["game"]
		sc, _ := strconv.Atoi(b["score"])
		mx, k := caps[game]
		if !k || sc <= 0 || sc > mx {
			fail(w, 400, "Score rejected")
			return
		}
		tk, tokOK := gTok[b["tok"]]
		delete(gTok, b["tok"])
		el := int(now() - tk.ts)
		if !tokOK || tk.user != u.ID || tk.game != game || el < 3 {
			fail(w, 400, "Score rejected. Start a new game and try again")
			return
		}
		if game == "snake" && (sc%10 != 0 || float64(el) < float64(sc/10)*0.4) {
			fail(w, 400, "Score rejected")
			return
		}
		if strings.HasPrefix(game, "streetracer") || game == "streetracer" || game == "skyparkour" || game == "lostisland" || game == "neonrunner" {
			if el < 8 || sc > el*450 {
				fail(w, 400, "Score rejected. Play a full round and submit again")
				return
			}
		}
		if game == "memory" {
			lim := 820 - 5*(el-2)
			if lim < 50 {
				lim = 50
			}
			if sc > lim {
				fail(w, 400, "Score rejected")
				return
			}
		}
		e := x4()
		if e.Sc[game] == nil {
			e.Sc[game] = map[string]int{}
		}
		old := e.Sc[game][u.ID]
		isNew := sc > old
		if isNew {
			e.Sc[game][u.ID] = sc
			old = sc
		}
		award(u.ID, "play")
		save()
		js(w, map[string]any{"Best": old, "New": isNew})
	})

	api("gboard", func(u *User, w http.ResponseWriter, r *http.Request) {
		game := r.URL.Query().Get("g")
		type row struct {
			id string
			v  int
		}
		l := []row{}
		if game == "xp" {
			for id, g := range x4().G {
				l = append(l, row{id, g.XP})
			}
		} else if game == "wins" {
			for id, g := range x4().G {
				if n := g.Cnt["t_gwin"]; n > 0 {
					l = append(l, row{id, n})
				}
			}
		} else {
			for id, v := range x4().Sc[game] {
				l = append(l, row{id, v})
			}
		}
		sort.Slice(l, func(a, c int) bool { return l[a].v > l[c].v })
		// The Top 1 badge is a live title: it follows the current global leader per 3D game.
		if game == "streetracer" || game == "skyparkour" || game == "lostisland" || game == "neonrunner" {
			badgeID := "top1_" + game
			for _, gp := range x4().G {
				filtered := gp.Badges[:0]
				for _, b := range gp.Badges {
					if b != badgeID {
						filtered = append(filtered, b)
					}
				}
				gp.Badges = filtered
			}
			if len(l) > 0 {
				if leader := x4().G[l[0].id]; leader != nil && !has(leader.Badges, badgeID) {
					leader.Badges = append(leader.Badges, badgeID)
				}
			}
			save()
		}
		out := []map[string]any{}
		for _, it := range l {
			if o := S.Users[it.id]; o == nil || o.Banned || o.Username == "" || (r.URL.Query().Get("f") == "1" && it.id != u.ID && !has(u.Following, it.id)) {
				continue
			}
			out = append(out, map[string]any{"U": uv(it.id), "V": it.v})
			if len(out) >= 20 {
				break
			}
		}
		js(w, out)
	})
}

// =====================================================================
// ---------- Batch A: performance and security fixes, links, highlights, insights, settings ----------
// =====================================================================

type HLItem struct {
	Media, Kind, Text string
	TS                int64
}
type HL struct {
	ID, Title string
	TS        int64
	Items     []HLItem
}
type Ext5 struct {
	HL    map[string][]*HL
	NS    map[string]map[string]bool // notification groups switched OFF
	NoRR  map[string]bool            // read receipts off
	Reach map[string]int
	GSeen map[string]map[string]int64
	Hits  map[string][]int64 // new-account posting limits, kept across restarts
}

func x5() *Ext5 {
	e := &S.X5
	if e.HL == nil {
		e.HL = map[string][]*HL{}
	}
	if e.NS == nil {
		e.NS = map[string]map[string]bool{}
	}
	if e.NoRR == nil {
		e.NoRR = map[string]bool{}
	}
	if e.Reach == nil {
		e.Reach = map[string]int{}
	}
	if e.GSeen == nil {
		e.GSeen = map[string]map[string]int64{}
	}
	if e.Hits == nil {
		e.Hits = map[string][]int64{}
	}
	return e
}

var nGroups = map[string]string{"like": "likes", "mreact": "likes", "comment": "comments", "reply": "comments", "follow": "follows", "freq": "follows", "facc": "follows", "msg": "messages", "mention": "mentions", "repost": "reposts"}

func nOff(to, typ string) bool {
	g, k := nGroups[typ]
	return k && x5().NS[to][g]
}
func sse(to, ev string) {
	for _, c := range hub[to] {
		select {
		case c <- ev:
		default:
		}
	}
}

// ---------- message index ----------
var (
	pairIx = map[string][]int{}
	userIx = map[string][]int{}
	mxN    int
	mxLast string
)

func msgIdx() { // call with mu held
	n := len(S.Msgs)
	if mxN > n || (mxN > 0 && S.Msgs[mxN-1].ID != mxLast) {
		pairIx, userIx, mxN, mxLast = map[string][]int{}, map[string][]int{}, 0, ""
	}
	for i := mxN; i < n; i++ {
		m := &S.Msgs[i]
		k := pairKey(m.From, m.To)
		pairIx[k] = append(pairIx[k], i)
		userIx[m.From] = append(userIx[m.From], i)
		if m.To != m.From {
			userIx[m.To] = append(userIx[m.To], i)
		}
	}
	mxN = n
	if n > 0 {
		mxLast = S.Msgs[n-1].ID
	}
}

// ---------- hashtag index ----------
var (
	tagIx  = map[string][]*Post{}
	tgN    int
	tgLast string
	tgAt   int64
)

func tagIdx() {
	n := len(S.Posts)
	last := ""
	if n > 0 {
		last = S.Posts[n-1].ID
	}
	if n == tgN && last == tgLast && now()-tgAt < 60 {
		return
	}
	tagIx = map[string][]*Post{}
	for _, p := range S.Posts {
		if p.Comm != "" || p.Text == "" {
			continue
		}
		seen := map[string]bool{}
		for _, t := range tagRe.FindAllString(strings.ToLower(p.Text), -1) {
			if !seen[t] {
				seen[t] = true
				tagIx[t] = append(tagIx[t], p)
			}
		}
	}
	tgN, tgLast, tgAt = n, last, now()
}

// ---------- post reach (approximate unique viewers) ----------
var (
	reachSet = map[string]map[string]struct{}{}
	reachN   int
)

func reachAdd(pid, uid string) {
	s := reachSet[pid]
	if s == nil {
		if reachN > 200000 {
			reachSet, reachN = map[string]map[string]struct{}{}, 0
		}
		s = map[string]struct{}{}
		reachSet[pid] = s
	}
	if _, k := s[uid]; k || len(s) >= 2000 {
		return
	}
	s[uid] = struct{}{}
	reachN++
	x5().Reach[pid]++
}

// ---------- highlights ----------
func hlKeep(m string) bool {
	if m == "" {
		return false
	}
	for _, l := range S.X5.HL {
		for _, h := range l {
			for _, it := range h.Items {
				if it.Media == m {
					return true
				}
			}
		}
	}
	return false
}
func statusUses(m string) bool {
	for _, s := range S.Statuses {
		if s.Media == m {
			return true
		}
	}
	return false
}
func purgeX5(id string) {
	e := x5()
	var fs []string
	for _, h := range e.HL[id] {
		for _, it := range h.Items {
			fs = append(fs, it.Media)
		}
	}
	delete(e.HL, id)
	delete(e.NS, id)
	delete(e.NoRR, id)
	for _, m := range e.GSeen {
		delete(m, id)
	}
	dropFiles(fs...)
}

// ---------- one-time live-stream tickets: the login token never appears in a URL ----------
type sTixE struct {
	user string
	exp  int64
}

var sTix = map[string]sTixE{}

func takeTicket(k string) string { // call with mu held
	e, ok := sTix[k]
	delete(sTix, k)
	if !ok || e.exp < now() {
		return ""
	}
	return e.user
}

// ---------- edge: path aliases, pretty links, optional per-IP limit ----------
var aliasPaths = map[string]string{"/api/msgs": "/api/msgs3", "/api/inbox": "/api/inbox3", "/api/search": "/api/search3", "/api/explore": "/api/explore3", "/api/msearch": "/api/msearch3"}
var (
	ipMu  sync.Mutex
	ipHit = map[string][2]int64{}
)

func ipAllow(key string, lim int64) bool {
	t := now() / 60
	ipMu.Lock()
	defer ipMu.Unlock()
	if len(ipHit) > 50000 {
		ipHit = map[string][2]int64{}
	}
	v := ipHit[key]
	if v[0] != t {
		v = [2]int64{t, 0}
	}
	v[1]++
	ipHit[key] = v
	return v[1] <= lim
}

// edge: per-IP limits are OFF unless you set IP_LIMIT (requests per minute per IP)
func edge(next http.Handler) http.Handler {
	lim, _ := strconv.ParseInt(env("IP_LIMIT"), 10, 64)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		if r.Method == "GET" || r.Method == "HEAD" {
			if p == "/healthz" {
				w.Write([]byte("ok"))
				return
			}
			if p == "/" || strings.HasPrefix(p, "/u/") || strings.HasPrefix(p, "/tag/") || strings.HasPrefix(p, "/p/") || strings.HasPrefix(p, "/g/") {
				servePage(w, r)
				return
			}
		}
		if lim > 0 && strings.HasPrefix(p, "/api/") && p != "/api/stream" {
			ip := cip(r)
			if !ipAllow("a|"+ip, lim) || ((p == "/api/login" || p == "/api/fblogin") && !ipAllow("l|"+ip, 30)) {
				fail(w, 429, "Too many requests. Try again in a minute.")
				return
			}
		}
		if a, k := aliasPaths[p]; k {
			r.URL.Path = a
		}
		next.ServeHTTP(w, r)
	})
}

// ---------- shareable pages with link previews ----------
var (
	ogUser = regexp.MustCompile(`^\w{3,20}$`)
	ogTag  = regexp.MustCompile(`^\w{1,30}$`)
	ogID   = regexp.MustCompile(`^[0-9a-f]{24}$`)
)

func ogTags(r *http.Request) string {
	p := r.URL.Path
	n, tag, pid, gm := "", "", "", ""
	switch {
	case strings.HasPrefix(p, "/u/"):
		n = p[3:]
	case strings.HasPrefix(p, "/tag/"):
		tag = p[5:]
	case strings.HasPrefix(p, "/p/"):
		pid = p[3:]
	case strings.HasPrefix(p, "/g/"):
		gm = p[3:]
	default:
		n, pid = r.URL.Query().Get("u"), r.URL.Query().Get("p")
	}
	n = strings.ToLower(n)
	if !ogUser.MatchString(n) {
		n = ""
	}
	if !ogTag.MatchString(tag) {
		tag = ""
	}
	if !ogID.MatchString(pid) {
		pid = ""
	}
	title, desc, img := "", "", ""
	mu.Lock()
	if n != "" {
		for _, o := range S.Users {
			if o.Username == n && !o.Banned {
				title = o.Name + " (@" + o.Username + ") on Amlink"
				if !x().Priv[o.ID] {
					desc, img = o.Bio, o.Avatar
				}
				if desc == "" {
					desc = "Follow @" + o.Username + " on Amlink"
				}
				break
			}
		}
	} else if gm == "snake" || gm == "memory" || gm == "word" {
		names := map[string]string{"snake": "Snake", "memory": "Memory Match", "word": "the Daily Word"}
		from, sc := r.URL.Query().Get("from"), r.URL.Query().Get("s")
		title, desc = "Play "+names[gm]+" on Amlink", "Play games with friends on Amlink"
		if v, e := strconv.Atoi(sc); e == nil && v > 0 && v < 100000 && ogUser.MatchString(from) {
			title, desc = "Can you beat @"+from+" at "+names[gm]+"?", "@"+from+" scored "+strconv.Itoa(v)+". Play on Amlink and try to beat it."
		}
	} else if tag != "" {
		title, desc = "#"+tag+" on Amlink", "See posts tagged #"+tag+" on Amlink"
	} else if pid != "" {
		if q := fp(pid); q != nil && q.Comm == "" {
			if o := S.Users[q.User]; o != nil && !o.Banned && !x().Priv[o.ID] {
				title, desc = "@"+o.Username+" on Amlink", trunc(q.Text, 160)
				if q.Kind == "image" {
					img = q.Media
				} else if op := x3().PO[q.ID]; op != nil {
					img = op.Poster
				}
			}
		}
	}
	mu.Unlock()
	if title == "" {
		return ""
	}
	s := `<meta property="og:site_name" content="Amlink"><meta property="og:title" content="` + html.EscapeString(title) + `"><meta property="og:description" content="` + html.EscapeString(desc) + `"><meta name="twitter:card" content="summary">`
	if img != "" {
		s += `<meta property="og:image" content="` + html.EscapeString(img) + `">`
	}
	return s
}

// servePage sends index.html (the whole app lives in that one file) with link-preview tags added for /u/, /tag/ and /p/ links
func servePage(w http.ResponseWriter, r *http.Request) {
	b, err := os.ReadFile("index.html")
	if err != nil {
		http.ServeFile(w, r, "index.html")
		return
	}
	s := string(b)
	if og := ogTags(r); og != "" {
		if i := strings.Index(s, "</head>"); i >= 0 {
			s = s[:i] + og + s[i:]
		}
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Write([]byte(s))
}

// serveIndex is kept because the "/" route calls it
func serveIndex(w http.ResponseWriter, r *http.Request) { servePage(w, r) }

func init() {
	modPaths["/api/hladd"] = "t"
	extraB()
	go func() { // keeps a free Render instance awake
		base := env("RENDER_EXTERNAL_URL")
		if base == "" {
			return
		}
		for range time.Tick(10 * time.Minute) {
			if r, e := hc.Get(base + "/healthz"); e == nil {
				r.Body.Close()
			}
		}
	}()
}

func extraB() {
	ok := map[string]bool{"ok": true}

	api("streamticket", func(u *User, w http.ResponseWriter, r *http.Request) {
		for k, e := range sTix {
			if e.exp < now() {
				delete(sTix, k)
			}
		}
		k := uid() + uid()
		sTix[k] = sTixE{u.ID, now() + 60}
		js(w, map[string]string{"k": k})
	})

	// ----- indexed chat reads -----
	api("msgs3", func(u *User, w http.ResponseWriter, r *http.Request) {
		p := r.URL.Query().Get("w")
		out, ch := []Msg{}, false
		noRR := x5().NoRR[u.ID]
		msgIdx()
		for _, i := range pairIx[pairKey(u.ID, p)] {
			m := &S.Msgs[i]
			if m.To == u.ID && !m.Rd {
				m.Rd, ch = true, true
			}
			if m.To == u.ID && !m.Read && !noRR {
				m.Read, ch = true, true
			}
			c := *m
			if m.From == u.ID && noRR {
				c.Read = false
			}
			out = append(out, c)
		}
		if ch {
			save()
		}
		js(w, out)
	})
	api("inbox3", func(u *User, w http.ResponseWriter, r *http.Request) {
		type cv struct {
			id   string
			last Msg
			un   int
		}
		m := map[string]*cv{}
		msgIdx()
		for _, i := range userIx[u.ID] {
			mm := S.Msgs[i]
			p := mm.To
			if mm.From != u.ID {
				p = mm.From
			}
			c := m[p]
			if c == nil {
				c = &cv{id: p}
				m[p] = c
			}
			c.last = mm
			if mm.To == u.ID && !mm.Read && !mm.Rd {
				c.un++
			}
		}
		list := []*cv{}
		for _, c := range m {
			if o := S.Users[c.id]; o != nil && !blk(u, o) {
				list = append(list, c)
			}
		}
		sort.Slice(list, func(a, b int) bool { return list[a].last.TS > list[b].last.TS })
		out := []map[string]any{}
		for _, c := range list {
			l := cut(c.last.Text)
			if l == "" {
				l = "Media"
			}
			if strings.HasPrefix(c.last.Text, "🎮 ") {
				l = "🎮 Game challenge"
			}
			out = append(out, map[string]any{"U": uv(c.id), "Last": l, "TS": c.last.TS, "Un": c.un, "Mine": c.last.From == u.ID})
		}
		js(w, out)
	})
	api("msearch3", func(u *User, w http.ResponseWriter, r *http.Request) {
		p := r.URL.Query().Get("w")
		q := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
		out := []map[string]any{}
		o := S.Users[p]
		if rn(q) < 2 || o == nil || blk(u, o) {
			js(w, out)
			return
		}
		msgIdx()
		l := pairIx[pairKey(u.ID, p)]
		for k := len(l) - 1; k >= 0 && len(out) < 40; k-- {
			m := S.Msgs[l[k]]
			if strings.Contains(strings.ToLower(m.Text), q) {
				out = append(out, map[string]any{"ID": m.ID, "Me": m.From == u.ID, "TS": m.TS, "Text": trunc(m.Text, 120)})
			}
		}
		js(w, out)
	})

	// ----- ranked search and hashtag index -----
	api("search3", func(u *User, w http.ResponseWriter, r *http.Request) {
		q := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
		type hit struct {
			id string
			s  int
		}
		hs := []hit{}
		for _, o := range S.Users {
			if o.ID == u.ID || o.Username == "" || o.Banned || blk(u, o) {
				continue
			}
			sc := 0
			switch {
			case q == "":
				if !has(u.Following, o.ID) {
					continue
				}
				sc = 1
			case o.Username == q:
				sc = 100
			case strings.HasPrefix(o.Username, q):
				sc = 80
			case strings.Contains(o.Username, q):
				sc = 50
			case strings.Contains(strings.ToLower(o.Name), q):
				sc = 30
			default:
				continue
			}
			if has(u.Following, o.ID) {
				sc += 5
			}
			if o.Verified {
				sc += 3
			}
			hs = append(hs, hit{o.ID, sc})
		}
		sort.Slice(hs, func(a, b int) bool {
			if hs[a].s != hs[b].s {
				return hs[a].s > hs[b].s
			}
			return hs[a].id < hs[b].id
		})
		lim := 30
		if q == "" {
			lim = 200
		}
		out := []map[string]any{}
		for i := 0; i < len(hs) && i < lim; i++ {
			out = append(out, uv(hs[i].id))
		}
		js(w, out)
	})
	api("explore3", func(u *User, w http.ResponseWriter, r *http.Request) {
		q := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
		out := []map[string]any{}
		vis := func(p *Post) bool {
			if p.Comm != "" || S.X7.Flag[p.ID] {
				return false
			}
			o := S.Users[p.User]
			return !(o != nil && (o.Banned || blk(u, o) || priv(u, o)))
		}
		if len(q) > 1 && q[0] == '#' && tagOne.MatchString(q) {
			tagIdx()
			l := tagIx[q]
			for i := len(l) - 1; i >= 0 && len(out) < 40; i-- {
				p := l[i]
				if fp(p.ID) != p || !vis(p) || !strings.Contains(strings.ToLower(p.Text), q) {
					continue
				}
				out = append(out, pv(p))
			}
			js(w, out)
			return
		}
		seenP := map[string]bool{}
		if q != "" { // whole-word matches come from the word index
			if toks := wordTok.FindAllString(q, -1); len(toks) > 0 {
				wordIdx()
				l := wordIx[toks[0]]
				for i := len(l) - 1; i >= 0 && len(out) < 40; i-- {
					p := l[i]
					if fp(p.ID) != p || !vis(p) || !strings.Contains(strings.ToLower(p.Text), q) {
						continue
					}
					seenP[p.ID] = true
					out = append(out, pv(p))
				}
			}
		}
		scanned := 0 // partial words are found by a scan of the newest 5000 posts
		for i := len(S.Posts) - 1; i >= 0 && len(out) < 40 && scanned < 5000; i-- {
			scanned++
			p := S.Posts[i]
			if seenP[p.ID] || !vis(p) {
				continue
			}
			if q == "" || strings.Contains(strings.ToLower(p.Text), q) {
				out = append(out, pv(p))
			}
		}
		js(w, out)
	})

	// ----- backups and audit log (admin only) -----
	http.HandleFunc("/api/backup", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		u := S.Users[S.Sess[r.Header.Get("X-Token")]]
		if u == nil || !u.Admin || u.Banned {
			mu.Unlock()
			fail(w, 403, "Admins only")
			return
		}
		c := S
		c.Sess, c.SessTS = nil, nil // login tokens are never exported
		c.X.Dev = nil
		c.X2.Push = nil
		b, _ := json.Marshal(c)
		alog(u, "downloaded a backup")
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Disposition", `attachment; filename="amlink-backup.json"`)
		w.Write(b)
	})
	api("auditlog", func(u *User, w http.ResponseWriter, r *http.Request) {
		if !u.Admin {
			fail(w, 403, "Admins only")
			return
		}
		js(w, S.Log)
	})

	// ----- highlights -----
	api("hlist", func(u *User, w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		o := S.Users[id]
		out := []map[string]any{}
		if o == nil || o.Banned || (id != u.ID && (blk(u, o) || priv(u, o))) {
			js(w, out)
			return
		}
		for _, h := range x5().HL[id] {
			cover := ""
			if len(h.Items) > 0 && h.Items[0].Kind != "video" {
				cover = h.Items[0].Media
			}
			out = append(out, map[string]any{"ID": h.ID, "Title": h.Title, "Cover": cover, "Items": h.Items})
		}
		js(w, out)
	})
	api("hladd", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		t := strings.TrimSpace(b["title"])
		if rn(t) < 1 || rn(t) > 20 {
			fail(w, 400, "Give the highlight a short title (up to 20 characters)")
			return
		}
		e := x5()
		if len(e.HL[u.ID]) >= 10 {
			fail(w, 400, "You can have up to 10 highlights")
			return
		}
		var items []HLItem
		seen := map[string]bool{}
		for _, id := range strings.Split(b["ids"], ",") {
			if s := fst(id); s != nil && s.User == u.ID && !seen[id] && len(items) < 30 {
				seen[id] = true
				items = append(items, HLItem{s.Media, s.Kind, s.Text, s.TS})
			}
		}
		if len(items) == 0 {
			fail(w, 400, "Pick at least one moment")
			return
		}
		e.HL[u.ID] = append(e.HL[u.ID], &HL{ID: uid(), Title: t, TS: now(), Items: items})
		save()
		js(w, ok)
	})
	api("hldel", func(u *User, w http.ResponseWriter, r *http.Request) {
		id := in(r)["id"]
		e := x5()
		keep, gone := []*HL{}, []string{}
		for _, h := range e.HL[u.ID] {
			if h.ID == id {
				for _, it := range h.Items {
					gone = append(gone, it.Media)
				}
			} else {
				keep = append(keep, h)
			}
		}
		e.HL[u.ID] = keep
		var fs []string
		for _, m := range gone {
			if m != "" && !hlKeep(m) && !statusUses(m) {
				fs = append(fs, m)
			}
		}
		dropFiles(fs...)
		save()
		js(w, ok)
	})

	// ----- per-post insights -----
	api("pstats", func(u *User, w http.ResponseWriter, r *http.Request) {
		p := fp(r.URL.Query().Get("id"))
		if p == nil || p.User != u.ID {
			fail(w, 403, "Not allowed")
			return
		}
		saves, reposts, votes := 0, 0, 0
		for _, o := range S.Users {
			if has(o.Saved, p.ID) {
				saves++
			}
		}
		for _, o := range x2().Rep {
			if o == p.ID {
				reposts++
			}
		}
		if p.Poll != nil {
			votes = len(p.Poll.Votes)
		}
		js(w, map[string]any{"Views": x2().Views[p.ID], "Reach": x5().Reach[p.ID], "Likes": len(p.Likes), "Comments": len(p.Comments), "Saves": saves, "Reposts": reposts, "Votes": votes})
	})

	// ----- notification settings and read receipts -----
	api("nsget", func(u *User, w http.ResponseWriter, r *http.Request) {
		e := x5()
		off := map[string]bool{}
		for k, v := range e.NS[u.ID] {
			off[k] = v
		}
		js(w, map[string]any{"Off": off, "RR": !e.NoRR[u.ID]})
	})
	api("nsset", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		k, on := b["k"], b["on"] == "1"
		e := x5()
		if k == "RR" {
			if on {
				delete(e.NoRR, u.ID)
			} else {
				e.NoRR[u.ID] = true
			}
		} else {
			good := false
			for _, g := range nGroups {
				if g == k {
					good = true
				}
			}
			if !good {
				fail(w, 400, "Unknown setting")
				return
			}
			if e.NS[u.ID] == nil {
				e.NS[u.ID] = map[string]bool{}
			}
			if on {
				delete(e.NS[u.ID], k)
			} else {
				e.NS[u.ID][k] = true
			}
		}
		save()
		js(w, ok)
	})
	api("gseen", func(u *User, w http.ResponseWriter, r *http.Request) {
		g := fg(r.URL.Query().Get("g"))
		if g == nil || !has(g.Members, u.ID) {
			fail(w, 403, "Not in this circle")
			return
		}
		e := x5()
		if !e.NoRR[u.ID] {
			if e.GSeen[g.ID] == nil {
				e.GSeen[g.ID] = map[string]int64{}
			}
			e.GSeen[g.ID][u.ID] = now()
		}
		var lt int64
		for i := len(g.Msgs) - 1; i >= 0; i-- {
			if g.Msgs[i].From == u.ID {
				lt = g.Msgs[i].TS
				break
			}
		}
		by := []map[string]any{}
		if lt > 0 && !e.NoRR[u.ID] {
			for id, ts := range e.GSeen[g.ID] {
				if id != u.ID && ts >= lt && has(g.Members, id) {
					by = append(by, uv(id))
				}
			}
			sort.Slice(by, func(a, b int) bool { return by[a]["Username"].(string) < by[b]["Username"].(string) })
		}
		js(w, map[string]any{"By": by})
	})

	// ----- report a message -----
	api("mreport", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		why := trunc(strings.TrimSpace(b["reason"]), 200)
		for i := len(S.Msgs) - 1; i >= 0; i-- {
			m := &S.Msgs[i]
			if m.ID != b["id"] {
				continue
			}
			if m.To != u.ID || S.Users[m.From] == nil {
				break
			}
			for _, o := range x().URep {
				if o.By == u.ID && o.Kind == "message" && o.Post == m.ID {
					js(w, ok)
					return
				}
			}
			txt := trunc(m.Text, 100)
			if txt == "" {
				txt = "[photo, video or voice note]"
			}
			x().URep = append(x().URep, &URep{ID: uid(), Kind: "message", Target: m.From, Post: m.ID, By: u.ID, Reason: why, Text: txt, TS: now()})
			save()
			js(w, ok)
			return
		}
		fail(w, 400, "You cannot report this message")
	})
}

// =====================================================================
// ---------- Batch 2: multiplayer games, daily word, shop, invites, interests ----------
// =====================================================================

type Cos struct {
	Own              []string
	Frame, Name, Bub string
	Badge            string
	GameSkin         string
}
type Game struct {
	Tour                              string // set when the game belongs to a cup
	Aux, Qi, A1, A2, Lq, La1, La2, Mv int    // checkers chain, trivia state, move counter
	QTS                               int64
	F1, F2, Sh1, Sh2, Bx, Qs          []int // battleship fleets and shots, dots-and-boxes owners, trivia questions
	ID, Kind, A, B, Turn, Status, Win string
	Board                             []int
	P1, P2, L1, L2                    string // rock-paper-scissors picks and the last revealed round
	S1, S2, Rd, LW                    int
	TS, Upd                           int64
	Paid                              bool
}
type WS struct {
	Day  string
	G    []string
	Done bool
	Won  bool
}
type Ext6 struct {
	Cos   map[string]*Cos
	Games map[string]*Game
	Word  map[string]*WS
	Code  map[string]string // user -> invite code
	CodeU map[string]string // invite code -> user
	RefBy map[string]string // invited user -> inviter
	RefN  map[string]int
	Int   map[string][]string
}

func x6() *Ext6 {
	e := &S.X6
	if e.Cos == nil {
		e.Cos = map[string]*Cos{}
	}
	if e.Games == nil {
		e.Games = map[string]*Game{}
	}
	if e.Word == nil {
		e.Word = map[string]*WS{}
	}
	if e.Code == nil {
		e.Code = map[string]string{}
	}
	if e.CodeU == nil {
		e.CodeU = map[string]string{}
	}
	if e.RefBy == nil {
		e.RefBy = map[string]string{}
	}
	if e.RefN == nil {
		e.RefN = map[string]int{}
	}
	if e.Int == nil {
		e.Int = map[string][]string{}
	}
	return e
}

// ---------- games ----------
func gameKind(k string) bool {
	return k == "ttt" || k == "c4" || k == "rps" || k == "chk" || k == "bs" || k == "dab" || k == "tri"
}

func newGame(kind, a, b string) *Game {
	g := &Game{ID: uid(), Kind: kind, A: a, B: b, Turn: a, Status: "active", TS: now(), Upd: now()}
	switch kind {
	case "ttt":
		g.Board = make([]int, 9)
	case "c4":
		g.Board = make([]int, 42)
	case "rps":
		g.Turn = ""
	case "chk":
		g.Board = make([]int, 64)
		for i := range g.Board {
			if r, c := i/8, i%8; (r+c)%2 == 1 {
				if r < 3 {
					g.Board[i] = 2
				} else if r > 4 {
					g.Board[i] = 1
				}
			}
		}
	case "bs":
		g.F1, g.F2 = placeFleet(), placeFleet()
		g.Sh1, g.Sh2 = make([]int, 100), make([]int, 100)
	case "dab":
		g.Board, g.Bx = make([]int, 40), make([]int, 16)
	case "tri":
		g.Turn = ""
		triNew(g)
	}
	x6().Games[g.ID] = g
	return g
}

func activeGames(id string) int {
	n := 0
	for _, g := range x6().Games {
		if g.Status == "active" && (g.A == id || g.B == id) {
			n++
		}
	}
	return n
}

func gameView(g *Game, me string) map[string]any {
	p := 1
	if me == g.B {
		p = 2
	}
	my, opp := g.P1, g.P2
	if p == 2 {
		my, opp = g.P2, g.P1
	}
	v := map[string]any{"ID": g.ID, "Kind": g.Kind, "Board": g.Board, "Turn": g.Turn, "Status": g.Status, "Win": g.Win, "Me": p,
		"A": uv(g.A), "B": uv(g.B), "S1": g.S1, "S2": g.S2, "Rd": g.Rd, "L1": g.L1, "L2": g.L2, "LW": g.LW, "My": my, "OppPicked": opp != ""}
	return gameViewX(v, g, me, p)
}

func lineWin(b []int, p int) bool {
	ls := [][3]int{{0, 1, 2}, {3, 4, 5}, {6, 7, 8}, {0, 3, 6}, {1, 4, 7}, {2, 5, 8}, {0, 4, 8}, {2, 4, 6}}
	for _, l := range ls {
		if b[l[0]] == p && b[l[1]] == p && b[l[2]] == p {
			return true
		}
	}
	return false
}

func c4Win(b []int, r, c, p int) bool {
	dirs := [][2]int{{0, 1}, {1, 0}, {1, 1}, {1, -1}}
	for _, d := range dirs {
		n := 1
		for s := -1; s <= 1; s += 2 {
			rr, cc := r+d[0]*s, c+d[1]*s
			for rr >= 0 && rr < 6 && cc >= 0 && cc < 7 && b[rr*7+cc] == p {
				n++
				rr += d[0] * s
				cc += d[1] * s
			}
		}
		if n >= 4 {
			return true
		}
	}
	return false
}

// 0 = tie, 1 = first picker wins, 2 = second wins
func rpsWinner(a, b string) int {
	if a == b {
		return 0
	}
	if (a == "r" && b == "s") || (a == "s" && b == "p") || (a == "p" && b == "r") {
		return 1
	}
	return 2
}

// payGame gives the rewards once, when a game ends (call with mu held)
func payGame(g *Game) {
	if g.Paid {
		return
	}
	g.Paid = true
	defer tourAdvance(g)
	enough := false
	if g.Kind == "rps" {
		enough = g.S1+g.S2 >= 2
	} else if g.Kind == "tri" {
		enough = true
	} else if g.Kind == "chk" || g.Kind == "bs" || g.Kind == "dab" {
		enough = g.Mv >= 6
	} else {
		n := 0
		for _, v := range g.Board {
			if v != 0 {
				n++
			}
		}
		enough = n >= 5
	}
	if !enough {
		return
	}
	switch g.Status {
	case "won":
		lo := g.A
		if g.Win == g.A {
			lo = g.B
		}
		gw := gpf(g.Win)
		if gw.Cnt["gwin"] < 5 { // daily cap, stops win-trading
			gw.Cnt["gwin"]++
			gw.XP += 25
			gw.Coins += 10
			ledger(g.Win, 10, "game", "Won a multiplayer game")
		}
		gw.Cnt["t_gwin"]++
		badge(gw, "gamer")
		gpf(lo).XP += 3
		checkBadges(gw)
	case "draw":
		gpf(g.A).XP += 5
		gpf(g.B).XP += 5
	}
}

type qEnt struct {
	user string
	ts   int64
}

var (
	gQueue = map[string][]qEnt{}
	gMatch = map[string]string{} // waiting user -> game id they were matched into
)

func qDrop(id string) {
	for k, l := range gQueue {
		n := l[:0:0]
		for _, e := range l {
			if e.user != id && now()-e.ts < 75 {
				n = append(n, e)
			}
		}
		gQueue[k] = n
	}
}

// ---------- daily word ----------
var wordList = strings.Fields(`about above actor adult after again agree ahead alarm album alert alive allow alone along angel anger angle angry apart apple apply arena argue arise armed aside asset audio avoid awake award aware badly baker basic beach began begin being below bench berry birth black blame blank blind block blood board boost brain brand brave bread break brick brief bring broad brown build burst buyer cabin candy carry catch cause chain chair chalk charm chart chase cheap check chest chief child chill civil claim class clean clear clerk click climb clock close cloud coach coast color count court cover craft crash crazy cream crime cross crowd crown cycle daily dance dealt death debut delay depth doubt dozen draft drama dream dress drink drive eager early earth eight elite empty enemy enjoy enter entry equal error event every exact exist extra faith false fancy fault field fifth fight final first flame flash fleet floor focus force frame fresh front fruit funny giant given glass globe glory grace grade grain grand grant grass great green greet group grown guard guess guest guide habit happy harsh heart heavy hello honey honor horse hotel house human humor ideal image index inner input issue joint judge juice knife known label large laser later laugh layer learn lemon level light limit lunch magic major maker march match maybe mayor media metal model money month moral motor mouse mouth movie music naive nerve never newly night noble noise north novel nurse ocean offer often olive onion order other ought outer owner paint panel paper party peace phase phone photo piano pilot pitch place plain plane plant plate point power press price pride prime print prize proof proud queen quick quiet radio raise range rapid ratio reach ready realm relax reply right river robot rough round route royal rural salad scale scene scope score sense serve seven shade shake shape share sharp sheep sheet shelf shell shift shine shirt shock shoot short shown sight skill sleep slice slide small smart smile smoke solid solve sorry sound south space spare speak speed spend spice split sport staff stage stair stamp stand start state steam steel stick still stock stone store storm story strip study style sugar suite sunny super sweet swing table taste teach thank their theme there thick thing think third those three throw tiger title today token total touch tough tower trace track trade trail train treat trend trial tribe trick truck truly trust truth twice uncle under union unity until upper urban usage usual valid value video visit vital voice waste watch water wheel where which while white whole whose woman world worry worth would write wrong yield young youth`)

var wordRe = regexp.MustCompile(`^[a-z]{5}$`)

func wordOf(day string) string {
	h := sha256.Sum256([]byte(day + env("WORD_SALT")))
	n := int(h[0])<<24 | int(h[1])<<16 | int(h[2])<<8 | int(h[3])
	if n < 0 {
		n = -n
	}
	return wordList[n%len(wordList)]
}

// feedback: 2 = right spot, 1 = in the word, 0 = not in the word
func wordFB(guess, ans string) string {
	res := []byte("00000")
	left := map[byte]int{}
	for i := 0; i < 5; i++ {
		if guess[i] == ans[i] {
			res[i] = '2'
		} else {
			left[ans[i]]++
		}
	}
	for i := 0; i < 5; i++ {
		if res[i] == '2' {
			continue
		}
		if left[guess[i]] > 0 {
			res[i] = '1'
			left[guess[i]]--
		}
	}
	return string(res)
}

func wordState(id string) *WS {
	e := x6()
	ws := e.Word[id]
	if ws == nil || ws.Day != today() {
		ws = &WS{Day: today()}
		e.Word[id] = ws
	}
	return ws
}

func wordView(ws *WS) map[string]any {
	ans := wordOf(ws.Day)
	gs := []map[string]string{}
	for _, g := range ws.G {
		gs = append(gs, map[string]string{"W": g, "F": wordFB(g, ans)})
	}
	out := map[string]any{"Day": ws.Day, "G": gs, "Done": ws.Done, "Won": ws.Won, "Max": 6}
	if ws.Done && !ws.Won {
		out["Answer"] = ans
	}
	return out
}

// ---------- shop ----------
type ShopItem struct {
	ID, Type, Name string
	Price, Lvl     int
}

var shopItems = []ShopItem{
	{"fr_gold", "frame", "Gold ring", 100, 1},
	{"fr_neon", "frame", "Neon ring", 150, 2},
	{"fr_rainbow", "frame", "Rainbow ring", 300, 4},
	{"fr_fire", "frame", "Fire ring", 500, 6},
	{"nc_blue", "name", "Blue name", 80, 1},
	{"nc_pink", "name", "Pink name", 80, 1},
	{"nc_green", "name", "Green name", 80, 1},
	{"nc_shine", "name", "Shiny name", 400, 5},
	{"bb_ocean", "bubble", "Ocean bubbles", 120, 2},
	{"bb_sunset", "bubble", "Sunset bubbles", 120, 2},
	{"bb_forest", "bubble", "Forest bubbles", 120, 3},
	{"bb_night", "bubble", "Midnight bubbles", 200, 4},
	{"bd_vip", "badge", "VIP badge", 300, 1},
	{"bd_star", "badge", "Star badge", 400, 2},
	{"bd_flame", "badge", "Flame badge", 600, 3},
	{"bd_crown", "badge", "Crown badge", 900, 4},
	{"bd_diamond", "badge", "Diamond badge", 1500, 5},
	{"bd_royal", "badge", "Royal badge", 2000, 6},
	{"bd_legend", "badge", "Legend badge", 3500, 8},
	{"bd_elite", "badge", "Elite badge", 5000, 10},
	{"car_silver", "game", "Street Racer · Silver paint", 180, 2},
	{"car_neon", "game", "Street Racer · Neon paint", 320, 4},
	{"parkour_trail", "game", "Sky Parkour · Light trail", 220, 3},
	{"island_explorer", "game", "Lost Island · Explorer outfit", 260, 3},
	{"runner_pulse", "game", "Neon Runner · Pulse trail", 240, 3},
	{"car_gold", "game", "Street Racer · Gold paint", 420, 5},
	{"car_crimson", "game", "Street Racer · Crimson paint", 360, 4},
}

func shopItem(id string) *ShopItem {
	for i := range shopItems {
		if shopItems[i].ID == id {
			return &shopItems[i]
		}
	}
	return nil
}
func setEq(c *Cos, it *ShopItem) {
	switch it.Type {
	case "frame":
		c.Frame = it.ID
	case "name":
		c.Name = it.ID
	case "bubble":
		c.Bub = it.ID
	case "badge":
		c.Badge = it.ID
	case "game":
		c.GameSkin = it.ID
	}
}

// ---------- invites and interests ----------
const codeAlpha = "abcdefghjkmnpqrstuvwxyz23456789"

func refCode(id string) string {
	e := x6()
	if c := e.Code[id]; c != "" {
		return c
	}
	for t := 0; t < 20; t++ {
		b := make([]byte, 6)
		rand.Read(b)
		for i := range b {
			b[i] = codeAlpha[int(b[i])%len(codeAlpha)]
		}
		c := string(b)
		if _, k := e.CodeU[c]; !k {
			e.Code[id] = c
			e.CodeU[c] = id
			return c
		}
	}
	return ""
}

var interestList = []string{"music", "sports", "comedy", "gaming", "food", "travel", "fashion", "tech", "art", "study"}
var interestWords = map[string][]string{
	"music": {"music", "song", "album", "concert", "afrobeats", "lyrics"}, "sports": {"football", "soccer", "basketball", "sport", "sports", "goal", "match"},
	"comedy": {"funny", "comedy", "meme", "memes", "lol", "joke"}, "gaming": {"game", "games", "gaming", "gamer", "ps5", "xbox", "fifa"},
	"food": {"food", "cook", "cooking", "recipe", "jollof", "restaurant"}, "travel": {"travel", "trip", "vacation", "flight", "beach"},
	"fashion": {"fashion", "style", "outfit", "ootd"}, "tech": {"tech", "code", "coding", "ai", "app", "phone"},
	"art": {"art", "draw", "drawing", "design", "photo", "paint"}, "study": {"study", "school", "exam", "learn", "class"},
}

func interestBoost(uid string, p *Post) float64 {
	mine := S.X6.Int[uid]
	if len(mine) == 0 || p.Text == "" {
		return 1
	}
	toks := map[string]bool{}
	for _, t := range wordTok.FindAllString(strings.ToLower(p.Text), -1) {
		toks[t] = true
	}
	n := 0
	for _, it := range mine {
		for _, w := range interestWords[it] {
			if toks[w] {
				n++
				break
			}
		}
	}
	if n > 3 {
		n = 3
	}
	return 1 + 0.4*float64(n)
}

// ---------- word index for post search ----------
var (
	wordIx  = map[string][]*Post{}
	wxN     int
	wxLast  string
	wxAt    int64
	wordTok = regexp.MustCompile(`[\p{L}\p{N}_]{3,}`)
)

func wordIdx() {
	n := len(S.Posts)
	last := ""
	if n > 0 {
		last = S.Posts[n-1].ID
	}
	if n == wxN && last == wxLast && now()-wxAt < 60 {
		return
	}
	wordIx = map[string][]*Post{}
	for _, p := range S.Posts {
		if p.Comm != "" || p.Text == "" {
			continue
		}
		seen := map[string]bool{}
		for _, t := range wordTok.FindAllString(strings.ToLower(p.Text), -1) {
			if !seen[t] {
				seen[t] = true
				wordIx[t] = append(wordIx[t], p)
			}
		}
	}
	wxN, wxLast, wxAt = n, last, now()
}

func init() {
	extra10()
}

func extra10() {
	ok := map[string]bool{"ok": true}

	// housekeeping: old games, old word states, stale abuse counters, expired queue entries
	go func() {
		for range time.Tick(10 * time.Minute) {
			mu.Lock()
			e := x6()
			forfeitStale(e)
			for id, g := range e.Games {
				if (g.Status != "active" && now()-g.Upd > 3*86400) || now()-g.Upd > 7*86400 {
					delete(e.Games, id)
				}
			}
			for id, ws := range e.Word {
				y := time.Now().UTC().AddDate(0, 0, -1).Format("2006-01-02")
				if ws.Day != today() && ws.Day != y {
					delete(e.Word, id)
				}
			}
			for k, l := range modHits {
				if len(l) == 0 || now()-l[len(l)-1] > 3600 {
					delete(modHits, k)
				}
			}
			qDrop("")
			for u, id := range gMatch {
				if g := e.Games[id]; g == nil || now()-g.TS > 300 {
					delete(gMatch, u)
				}
			}
			mu.Unlock()
		}
	}()

	// ----- challenge a friend (also posts a game card in the chat) -----
	api("gnew", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		o := S.Users[b["to"]]
		k := b["kind"]
		if o == nil || o.ID == u.ID || o.Banned || blk(u, o) || !gameKind(k) {
			fail(w, 400, "You cannot challenge this person")
			return
		}
		for _, g := range x6().Games { // one open game of the same kind per pair
			if g.Status == "active" && g.Kind == k && ((g.A == u.ID && g.B == o.ID) || (g.A == o.ID && g.B == u.ID)) {
				js(w, map[string]string{"id": g.ID})
				return
			}
		}
		if activeGames(u.ID) >= 8 {
			fail(w, 400, "You have too many open games. Finish or resign one first")
			return
		}
		g := newGame(k, u.ID, o.ID)
		S.Msgs = append(S.Msgs, Msg{From: u.ID, To: o.ID, Text: "🎮 " + k + " " + g.ID, TS: now(), Exp: dexp(u.ID, o.ID)})
		note(o.ID, u.ID, "msg", "")
		push(o.ID, "game:"+g.ID)
		save()
		js(w, map[string]string{"id": g.ID})
	})
	api("gget", func(u *User, w http.ResponseWriter, r *http.Request) {
		g := x6().Games[r.URL.Query().Get("id")]
		if g == nil || (g.A != u.ID && g.B != u.ID) {
			fail(w, 404, "Game not found")
			return
		}
		if triTick(g) {
			save()
		}
		js(w, gameView(g, u.ID))
	})
	api("gmine", func(u *User, w http.ResponseWriter, r *http.Request) {
		l := []*Game{}
		for _, g := range x6().Games {
			if g.A == u.ID || g.B == u.ID {
				l = append(l, g)
			}
		}
		sort.Slice(l, func(a, b int) bool { return l[a].Upd > l[b].Upd })
		out := []map[string]any{}
		for i := 0; i < len(l) && i < 30; i++ {
			g := l[i]
			opp := g.A
			if opp == u.ID {
				opp = g.B
			}
			mine := false
			if g.Status == "active" {
				if g.Kind == "rps" || g.Kind == "tri" {
					if g.Kind == "tri" {
						if (g.A == u.ID && g.A1 == 0) || (g.B == u.ID && g.A2 == 0) {
							mine = true
						}
					} else if (g.A == u.ID && g.P1 == "") || (g.B == u.ID && g.P2 == "") {
						mine = true
					}
				} else {
					mine = g.Turn == u.ID
				}
			}
			out = append(out, map[string]any{"ID": g.ID, "Kind": g.Kind, "Status": g.Status, "Win": g.Win, "Opp": uv(opp), "Upd": g.Upd, "Mine": mine})
		}
		js(w, out)
	})
	api("gmove", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		g := x6().Games[b["id"]]
		if g == nil || (g.A != u.ID && g.B != u.ID) {
			fail(w, 404, "Game not found")
			return
		}
		if g.Status != "active" {
			fail(w, 400, "This game is over")
			return
		}
		isA := u.ID == g.A
		opp := g.B
		p := 1
		if !isA {
			opp, p = g.A, 2
		}
		if g.Kind == "chk" || g.Kind == "bs" || g.Kind == "dab" || g.Kind == "tri" {
			if msg := newKindMove(g, u.ID, b["m"]); msg != "" {
				fail(w, 400, msg)
				return
			}
			g.Upd = now()
			save()
			push(opp, "game:"+g.ID)
			js(w, gameView(g, u.ID))
			return
		}
		if g.Kind == "rps" {
			m := b["m"]
			if m != "r" && m != "p" && m != "s" {
				fail(w, 400, "Pick rock, paper or scissors")
				return
			}
			if (isA && g.P1 != "") || (!isA && g.P2 != "") {
				fail(w, 400, "You already picked. Wait for your opponent")
				return
			}
			if isA {
				g.P1 = m
			} else {
				g.P2 = m
			}
			if g.P1 != "" && g.P2 != "" {
				g.L1, g.L2 = g.P1, g.P2
				g.LW = rpsWinner(g.P1, g.P2)
				if g.LW == 1 {
					g.S1++
				} else if g.LW == 2 {
					g.S2++
				}
				g.P1, g.P2 = "", ""
				g.Rd++
				if g.S1 >= 2 {
					g.Status, g.Win = "won", g.A
				} else if g.S2 >= 2 {
					g.Status, g.Win = "won", g.B
				}
				if g.Status != "active" {
					payGame(g)
				}
			}
		} else {
			if g.Turn != u.ID {
				fail(w, 400, "It is not your turn")
				return
			}
			m, e := strconv.Atoi(b["m"])
			won := false
			if g.Kind == "ttt" {
				if e != nil || m < 0 || m > 8 || g.Board[m] != 0 {
					fail(w, 400, "That square is taken")
					return
				}
				g.Board[m] = p
				won = lineWin(g.Board, p)
			} else {
				if e != nil || m < 0 || m > 6 {
					fail(w, 400, "Pick a column")
					return
				}
				row := -1
				for rr := 5; rr >= 0; rr-- {
					if g.Board[rr*7+m] == 0 {
						row = rr
						break
					}
				}
				if row < 0 {
					fail(w, 400, "That column is full")
					return
				}
				g.Board[row*7+m] = p
				won = c4Win(g.Board, row, m, p)
			}
			full := true
			for _, v := range g.Board {
				if v == 0 {
					full = false
					break
				}
			}
			if won {
				g.Status, g.Win = "won", u.ID
			} else if full {
				g.Status = "draw"
			} else {
				g.Turn = opp
			}
			if g.Status != "active" {
				payGame(g)
			}
		}
		g.Upd = now()
		save()
		push(opp, "game:"+g.ID)
		js(w, gameView(g, u.ID))
	})
	api("gresign", func(u *User, w http.ResponseWriter, r *http.Request) {
		g := x6().Games[in(r)["id"]]
		if g == nil || (g.A != u.ID && g.B != u.ID) || g.Status != "active" {
			fail(w, 400, "You cannot resign this game")
			return
		}
		opp := g.A
		if opp == u.ID {
			opp = g.B
		}
		g.Status, g.Win, g.Paid, g.Upd = "resigned", opp, true, now()
		tourAdvance(g)
		save()
		push(opp, "game:"+g.ID)
		js(w, gameView(g, u.ID))
	})

	// ----- quick match: play a random person who is waiting -----
	api("gqjoin", func(u *User, w http.ResponseWriter, r *http.Request) {
		k := in(r)["kind"]
		if !gameKind(k) {
			fail(w, 400, "Unknown game")
			return
		}
		if activeGames(u.ID) >= 8 {
			fail(w, 400, "You have too many open games. Finish or resign one first")
			return
		}
		qDrop(u.ID)
		for i, e := range gQueue[k] {
			o := S.Users[e.user]
			if o == nil || o.Banned || blk(u, o) {
				continue
			}
			gQueue[k] = append(gQueue[k][:i:i], gQueue[k][i+1:]...)
			a, bb := e.user, u.ID
			rb := make([]byte, 1)
			rand.Read(rb)
			if rb[0]%2 == 0 {
				a, bb = bb, a
			}
			g := newGame(k, a, bb)
			gMatch[e.user] = g.ID
			push(e.user, "match:"+g.ID)
			save()
			js(w, map[string]any{"id": g.ID})
			return
		}
		gQueue[k] = append(gQueue[k], qEnt{u.ID, now()})
		delete(gMatch, u.ID)
		js(w, map[string]any{"wait": true})
	})
	api("gqpoll", func(u *User, w http.ResponseWriter, r *http.Request) {
		id := gMatch[u.ID]
		if id != "" {
			delete(gMatch, u.ID)
			if g := x6().Games[id]; g != nil {
				js(w, map[string]any{"id": id})
				return
			}
		}
		js(w, map[string]any{"wait": true})
	})
	api("gqcancel", func(u *User, w http.ResponseWriter, r *http.Request) {
		qDrop(u.ID)
		js(w, ok)
	})

	// ----- daily word -----
	api("wget", func(u *User, w http.ResponseWriter, r *http.Request) {
		js(w, wordView(wordState(u.ID)))
	})
	api("wguess", func(u *User, w http.ResponseWriter, r *http.Request) {
		g := strings.ToLower(strings.TrimSpace(in(r)["w"]))
		ws := wordState(u.ID)
		if ws.Done {
			fail(w, 400, "You already finished today's word")
			return
		}
		if !wordRe.MatchString(g) {
			fail(w, 400, "Type a 5-letter word")
			return
		}
		ws.G = append(ws.G, g)
		gg := gpf(u.ID)
		if g == wordOf(ws.Day) {
			ws.Done, ws.Won = true, true
			left := 6 - len(ws.G)
			gg.XP += 20 + left*5
			gg.Coins += 10 + left*5
			ledger(u.ID, 10+left*5, "word", "Solved the Daily Word")
			gg.Cnt["t_word"]++
			badge(gg, "wordsmith")
			award(u.ID, "play")
		} else if len(ws.G) >= 6 {
			ws.Done = true
			gg.XP += 3
		}
		save()
		js(w, wordView(ws))
	})

	// ----- shop -----
	api("shop", func(u *User, w http.ResponseWriter, r *http.Request) {
		g := gpf(u.ID)
		own, eq := map[string]bool{}, map[string]bool{}
		if c := x6().Cos[u.ID]; c != nil {
			for _, id := range c.Own {
				own[id] = true
			}
			eq[c.Frame], eq[c.Name], eq[c.Bub], eq[c.Badge], eq[c.GameSkin] = true, true, true, true, true
		}
		items := []map[string]any{}
		for _, it := range shopItems {
			items = append(items, map[string]any{"ID": it.ID, "Type": it.Type, "Name": it.Name, "Price": it.Price, "Lvl": it.Lvl, "Own": own[it.ID], "Eq": eq[it.ID]})
		}
		js(w, map[string]any{"Items": items, "Coins": g.Coins, "Level": lvl(g.XP), "Inf": u.Admin})
	})
	api("shopbuy", func(u *User, w http.ResponseWriter, r *http.Request) {
		it := shopItem(in(r)["id"])
		if it == nil {
			fail(w, 400, "Unknown item")
			return
		}
		g := gpf(u.ID)
		e := x6()
		c := e.Cos[u.ID]
		if c == nil {
			c = &Cos{}
			e.Cos[u.ID] = c
		}
		if has(c.Own, it.ID) {
			fail(w, 400, "You already own this")
			return
		}
		if !u.Admin && lvl(g.XP) < it.Lvl {
			fail(w, 400, fmt.Sprintf("Reach level %d to unlock this", it.Lvl))
			return
		}
		if !spendCoins(u, g, it.Price) {
			fail(w, 400, "Not enough Ran Coin")
			return
		}
		ledger(u.ID, spentAmt(u, it.Price), "shop", "Bought "+it.Name+adminNote(u))
		c.Own = append(c.Own, it.ID)
		setEq(c, it)
		save()
		js(w, map[string]int{"Coins": g.Coins})
	})
	api("shopeq", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		e := x6()
		c := e.Cos[u.ID]
		if c == nil {
			c = &Cos{}
			e.Cos[u.ID] = c
		}
		if b["id"] == "" {
			switch b["type"] {
			case "frame":
				c.Frame = ""
			case "name":
				c.Name = ""
			case "bubble":
				c.Bub = ""
			case "badge":
				c.Badge = ""
			case "game":
				c.GameSkin = ""
			default:
				fail(w, 400, "Unknown type")
				return
			}
		} else {
			it := shopItem(b["id"])
			if it == nil || !has(c.Own, it.ID) {
				fail(w, 403, "You do not own this")
				return
			}
			setEq(c, it)
		}
		save()
		js(w, ok)
	})
	api("cosme", func(u *User, w http.ResponseWriter, r *http.Request) {
		c := x6().Cos[u.ID]
		if c == nil {
			c = &Cos{}
		}
		js(w, map[string]string{"Frame": c.Frame, "Name": c.Name, "Bub": c.Bub, "Badge": c.Badge, "GameSkin": c.GameSkin})
	})

	// ----- invites -----
	api("refcode", func(u *User, w http.ResponseWriter, r *http.Request) {
		c := refCode(u.ID)
		save()
		js(w, map[string]any{"Code": c, "N": x6().RefN[u.ID]})
	})
	api("refclaim", func(u *User, w http.ResponseWriter, r *http.Request) {
		code := strings.ToLower(strings.TrimSpace(in(r)["code"]))
		e := x6()
		inv := e.CodeU[code]
		iu := S.Users[inv]
		if _, done := e.RefBy[u.ID]; done || inv == "" || inv == u.ID || iu == nil || iu.Banned || blk(u, iu) {
			fail(w, 400, "This invite link cannot be used")
			return
		}
		born, k := x().Born[u.ID]
		if !k || now()-born > 7*86400 {
			fail(w, 400, "Invite links only work for new accounts")
			return
		}
		if e.RefN[inv] >= 50 {
			fail(w, 400, "This invite link has reached its limit")
			return
		}
		e.RefBy[u.ID] = inv
		e.RefN[inv]++
		gi, gu := gpf(inv), gpf(u.ID)
		gi.Coins += 50
		gu.Coins += 50
		ledger(inv, 50, "invite", "A friend joined with your invite link")
		ledger(u.ID, 50, "invite", "Joined with an invite link")
		gi.XP += 20
		gu.XP += 20
		if e.RefN[inv] >= 3 {
			badge(gi, "recruiter")
		}
		save()
		js(w, map[string]int{"Coins": 50})
	})

	// ----- interests -----
	api("intget", func(u *User, w http.ResponseWriter, r *http.Request) {
		m := x6().Int[u.ID]
		if m == nil {
			m = []string{}
		}
		js(w, map[string]any{"List": interestList, "Mine": m})
	})
	api("intset", func(u *User, w http.ResponseWriter, r *http.Request) {
		var out []string
		for _, s := range strings.Split(strings.ToLower(in(r)["list"]), ",") {
			s = strings.TrimSpace(s)
			if has(interestList, s) && !has(out, s) && len(out) < 10 {
				out = append(out, s)
			}
		}
		x6().Int[u.ID] = out
		save()
		js(w, ok)
	})
}

// =====================================================================
// ---------- Batch 4: tournaments, party games, safety pack, duets ----------
// =====================================================================

type TM struct {
	ID, A, B, Game, Win string
	Round               int
}
type Tour struct {
	ID, Kind, Status, Champ string
	Players                 []string
	M                       []*TM
	TS                      int64
}
type Appeal struct {
	ID, User, Kind, Ref, Text, Status string
	TS                                int64
}
type Puz struct {
	Ans string
	Exp int64
}
type Ext7 struct {
	Tours map[string]*Tour
	Flag  map[string]bool // posts hidden from feeds after several reports
	Mods  map[string]bool // moderators who are not admins
	Apps  []*Appeal
	Puz   map[string]*Puz
}

func x7() *Ext7 {
	e := &S.X7
	if e.Tours == nil {
		e.Tours = map[string]*Tour{}
	}
	if e.Flag == nil {
		e.Flag = map[string]bool{}
	}
	if e.Mods == nil {
		e.Mods = map[string]bool{}
	}
	if e.Puz == nil {
		e.Puz = map[string]*Puz{}
	}
	return e
}
func isMod(id string) bool { return S.X7.Mods[id] }

// ---------- tournaments: 8 players, 3 rounds, knockout ----------
func tourMatch(t *Tour, round int, a, b string) {
	m := &TM{ID: uid(), Round: round, A: a, B: b}
	g := newGame(t.Kind, a, b)
	g.Tour = t.ID
	m.Game = g.ID
	t.M = append(t.M, m)
	push(a, "game:"+g.ID)
	push(b, "game:"+g.ID)
}

func tourStart(t *Tour) {
	ps := append([]string(nil), t.Players...)
	for i := len(ps) - 1; i > 0; i-- {
		rb := make([]byte, 1)
		rand.Read(rb)
		j := int(rb[0]) % (i + 1)
		ps[i], ps[j] = ps[j], ps[i]
	}
	t.Status = "running"
	for i := 0; i+1 < len(ps); i += 2 {
		tourMatch(t, 1, ps[i], ps[i+1])
	}
}

// tourAdvance is called when a game ends (mu held)
func tourAdvance(g *Game) {
	if g.Tour == "" {
		return
	}
	t := x7().Tours[g.Tour]
	if t == nil || t.Status != "running" {
		return
	}
	var m *TM
	for _, x := range t.M {
		if x.Game == g.ID {
			m = x
		}
	}
	if m == nil || m.Win != "" {
		return
	}
	if g.Status == "draw" { // sudden death: play again right away
		ng := newGame(t.Kind, g.A, g.B)
		ng.Tour = t.ID
		m.Game = ng.ID
		push(g.A, "game:"+ng.ID)
		push(g.B, "game:"+ng.ID)
		return
	}
	if g.Win == "" {
		return
	}
	m.Win = g.Win
	var ws []string
	total := 0
	for _, x := range t.M {
		if x.Round == m.Round {
			total++
			if x.Win != "" {
				ws = append(ws, x.Win)
			}
		}
	}
	if len(ws) < total {
		return
	}
	if total == 1 { // the final is over
		t.Status, t.Champ = "done", m.Win
		ru := m.A
		if m.Win == m.A {
			ru = m.B
		}
		gw := gpf(m.Win)
		gw.Coins += 100
		ledger(m.Win, 100, "cup", "Won a cup")
		gw.XP += 60
		badge(gw, "champion")
		gr := gpf(ru)
		gr.Coins += 40
		ledger(ru, 40, "cup", "Cup runner-up")
		gr.XP += 20
		for _, p := range t.Players {
			push(p, "game:"+g.ID)
		}
		return
	}
	for i := 0; i+1 < len(ws); i += 2 {
		tourMatch(t, m.Round+1, ws[i], ws[i+1])
	}
}

// ---------- party games for circles ----------
var truths = []string{"What is the most embarrassing thing you have done in public?", "Who was your first crush?", "What is a secret talent nobody knows about?", "What is the last lie you told?", "What song do you secretly love?", "What is your biggest fear?", "Which friend here would you trust with your phone?", "What is the worst gift you ever received?"}
var dares = []string{"Send a voice note singing the chorus of any song.", "Change your profile photo to a funny face for one hour.", "Send the 5th photo in your gallery.", "Talk in rhymes for the next 5 messages.", "Post a compliment to everyone in this circle.", "Send a selfie making your silliest face.", "Say something nice about each person here.", "Write a short poem about this circle."}
var wyrs = []string{"Have free food for life OR free flights for life?", "Be able to fly OR be invisible?", "Always be 10 minutes late OR always be 20 minutes early?", "Live without music OR live without movies?", "Know when you will die OR how you will die?", "Have no phone for a month OR no friends for a month?"}
var puzzles = []struct{ E, A string }{{"🦁👑", "lion king"}, {"🕷️👨", "spider man"}, {"❄️👸", "frozen"}, {"🌊🐠🔍", "finding nemo"}, {"🏠⬆️🎈", "up"}, {"🐍🍎", "adam and eve"}, {"⭐🌍⚔️", "star wars"}, {"🍕🐢", "ninja turtles"}, {"🚢🧊💔", "titanic"}, {"🧙‍♂️💍", "lord of the rings"}, {"🎤🇳🇬", "afrobeats"}, {"⚽🏆🌍", "world cup"}, {"🌧️☂️", "umbrella"}, {"🔥🐉", "dragon"}, {"🍯🐝", "honey bee"}}

func puzzleCheck(g *Group, u *User, t string) {
	pz := x7().Puz[g.ID]
	if pz == nil || pz.Exp < now() {
		return
	}
	if strings.EqualFold(strings.TrimSpace(t), pz.Ans) {
		delete(x7().Puz, g.ID)
		gg := gpf(u.ID)
		if gg.Cnt["puz"] < 10 {
			gg.Cnt["puz"]++
			gg.XP += 5
			gg.Coins += 2
			ledger(u.ID, 2, "party", "Won an emoji guess")
		}
		g.Msgs = append(g.Msgs, Msg{From: u.ID, To: g.ID, Text: "✅ Got it! The answer was " + pz.Ans, TS: now()})
	}
}

// ---------- safety ----------
func autoFlag(pid string) {
	p := fp(pid)
	if p == nil || x7().Flag[pid] {
		return
	}
	seen := map[string]bool{}
	for _, rp := range S.Reports {
		if rp.Post == pid {
			seen[rp.By] = true
		}
	}
	if len(seen) < 3 {
		return
	}
	x7().Flag[pid] = true
	x().Sens[pid] = true
	S.Notifs = append(S.Notifs, Notif{p.User, p.User, "flag", pid, now(), false})
	push(p.User, "flag")
}

func purgeX7(id string) {
	e7, e6, e4 := x7(), x6(), x4()
	delete(e7.Mods, id)
	for k, t := range e7.Tours {
		if t.Status == "open" {
			t.Players = del(t.Players, id)
			if len(t.Players) == 0 {
				delete(e7.Tours, k)
			}
		}
	}
	keep := e7.Apps[:0]
	for _, a := range e7.Apps {
		if a.User != id {
			keep = append(keep, a)
		}
	}
	e7.Apps = keep
	for k, g := range e6.Games {
		if g.A == id || g.B == id {
			delete(e6.Games, k)
		}
	}
	delete(e6.Cos, id)
	delete(e6.Word, id)
	delete(e6.Int, id)
	delete(e6.RefBy, id)
	if c := e6.Code[id]; c != "" {
		delete(e6.CodeU, c)
	}
	delete(e6.Code, id)
	delete(e4.G, id)
	for _, m := range e4.Sc {
		delete(m, id)
	}
}

func init() {
	pushEv["yourmove"] = true
	pushEv["flag"] = true
	pushEv["aok"] = true
	pushEv["ano"] = true
	pushEv["duet"] = true
	nGroups["duet"] = "reposts"
	extra11()
}

func extra11() {
	ok := map[string]bool{"ok": true}

	// housekeeping for old tournaments
	go func() {
		for range time.Tick(15 * time.Minute) {
			mu.Lock()
			for id, t := range x7().Tours {
				age := now() - t.TS
				if (t.Status == "done" && age > 7*86400) || (t.Status == "open" && age > 3*86400) || age > 14*86400 {
					delete(x7().Tours, id)
				}
			}
			// a player who does not move for 24 hours forfeits their cup match, so cups always finish
			for _, t := range x7().Tours {
				if t.Status != "running" {
					continue
				}
				for _, m := range append([]*TM(nil), t.M...) {
					g := x6().Games[m.Game]
					if m.Win != "" || g == nil || g.Status != "active" || now()-g.Upd < 86400 {
						continue
					}
					loser := g.A
					if g.Kind == "rps" {
						if g.P1 != "" && g.P2 == "" {
							loser = g.B
						}
					} else if g.Turn == g.B {
						loser = g.B
					}
					winner := g.A
					if loser == g.A {
						winner = g.B
					}
					g.Status, g.Win, g.Paid, g.Upd = "resigned", winner, true, now()
					tourAdvance(g)
				}
			}
			mu.Unlock()
		}
	}()

	// ----- tournaments -----
	api("tlist", func(u *User, w http.ResponseWriter, r *http.Request) {
		open, mine := []map[string]any{}, []map[string]any{}
		ts := []*Tour{}
		for _, t := range x7().Tours {
			ts = append(ts, t)
		}
		sort.Slice(ts, func(a, b int) bool { return ts[a].TS > ts[b].TS })
		for _, t := range ts {
			joined := has(t.Players, u.ID)
			row := map[string]any{"ID": t.ID, "Kind": t.Kind, "Status": t.Status, "N": len(t.Players), "In": joined}
			if joined {
				mine = append(mine, row)
			} else if t.Status == "open" {
				open = append(open, row)
			}
		}
		js(w, map[string]any{"Open": open, "Mine": mine})
	})
	api("tjoin", func(u *User, w http.ResponseWriter, r *http.Request) {
		k := in(r)["kind"]
		if !gameKind(k) {
			fail(w, 400, "Unknown game")
			return
		}
		live := 0
		for _, t := range x7().Tours {
			if has(t.Players, u.ID) && t.Status != "done" {
				live++
				if t.Kind == k {
					fail(w, 400, "You are already in a cup for this game")
					return
				}
			}
		}
		if live >= 3 {
			fail(w, 400, "You can be in up to 3 cups at a time")
			return
		}
		var t *Tour
		for _, c := range x7().Tours {
			if c.Kind == k && c.Status == "open" && len(c.Players) < 8 && (t == nil || c.TS < t.TS) {
				t = c
			}
		}
		if t == nil {
			t = &Tour{ID: uid(), Kind: k, Status: "open", TS: now()}
			x7().Tours[t.ID] = t
		}
		t.Players = append(t.Players, u.ID)
		if len(t.Players) == 8 {
			tourStart(t)
		}
		save()
		js(w, map[string]string{"id": t.ID})
	})
	api("tleave", func(u *User, w http.ResponseWriter, r *http.Request) {
		t := x7().Tours[in(r)["id"]]
		if t == nil || t.Status != "open" || !has(t.Players, u.ID) {
			fail(w, 400, "You cannot leave this cup")
			return
		}
		t.Players = del(t.Players, u.ID)
		if len(t.Players) == 0 {
			delete(x7().Tours, t.ID)
		}
		save()
		js(w, ok)
	})
	api("tget", func(u *User, w http.ResponseWriter, r *http.Request) {
		t := x7().Tours[r.URL.Query().Get("id")]
		if t == nil {
			fail(w, 404, "Cup not found")
			return
		}
		ps := []map[string]any{}
		for _, id := range t.Players {
			ps = append(ps, uv(id))
		}
		rounds := map[int][]map[string]any{}
		for _, m := range t.M {
			st := ""
			if g := x6().Games[m.Game]; g != nil {
				st = g.Status
			}
			rounds[m.Round] = append(rounds[m.Round], map[string]any{"A": uv(m.A), "B": uv(m.B), "Game": m.Game, "Win": m.Win, "St": st, "Mine": m.Win == "" && (m.A == u.ID || m.B == u.ID)})
		}
		out := [][]map[string]any{}
		for i := 1; i <= 3; i++ {
			if l, k := rounds[i]; k {
				out = append(out, l)
			}
		}
		res := map[string]any{"ID": t.ID, "Kind": t.Kind, "Status": t.Status, "Players": ps, "Rounds": out, "In": has(t.Players, u.ID)}
		if t.Champ != "" {
			res["Champ"] = uv(t.Champ)
		}
		js(w, res)
	})

	// ----- party games in circles -----
	api("pgame", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		g := fg(b["g"])
		if g == nil || !has(g.Members, u.ID) || len(g.Members) == 0 {
			fail(w, 403, "Not in this circle")
			return
		}
		pick := func(n int) int {
			rb := make([]byte, 2)
			rand.Read(rb)
			return (int(rb[0])<<8 | int(rb[1])) % n
		}
		who := "someone"
		if o := S.Users[g.Members[pick(len(g.Members))]]; o != nil && o.Username != "" {
			who = "@" + o.Username
		}
		var txt string
		switch b["kind"] {
		case "truth":
			txt = "🎲 Truth for " + who + ": " + truths[pick(len(truths))]
		case "dare":
			txt = "🎲 Dare for " + who + ": " + dares[pick(len(dares))]
		case "wyr":
			txt = "🎲 Would you rather… " + wyrs[pick(len(wyrs))] + " Reply A or B!"
		case "emoji":
			pz := puzzles[pick(len(puzzles))]
			x7().Puz[g.ID] = &Puz{Ans: pz.A, Exp: now() + 600}
			txt = "🧩 Emoji guess: " + pz.E + "  (type your answer, the first correct one wins)"
		default:
			fail(w, 400, "Unknown game")
			return
		}
		g.Msgs = append(g.Msgs, Msg{From: u.ID, To: g.ID, Text: txt, TS: now()})
		for _, m := range g.Members {
			note(m, u.ID, "msg", "")
		}
		save()
		js(w, ok)
	})

	// ----- moderators -----
	api("modme", func(u *User, w http.ResponseWriter, r *http.Request) {
		js(w, map[string]bool{"Mod": u.Admin || isMod(u.ID)})
	})
	api("modrole", func(u *User, w http.ResponseWriter, r *http.Request) {
		o := S.Users[in(r)["id"]]
		if !u.Admin || o == nil || o.Admin {
			fail(w, 403, "Not allowed")
			return
		}
		x7().Mods[o.ID] = !x7().Mods[o.ID]
		alog(u, "toggled moderator on @"+o.Username)
		save()
		js(w, map[string]bool{"Mod": x7().Mods[o.ID]})
	})

	// ----- appeals: works for banned users too, so it is not behind the normal login check -----
	http.HandleFunc("/api/appeal", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		u := S.Users[S.Sess[r.Header.Get("X-Token")]]
		if u == nil {
			fail(w, 401, "Please log in")
			return
		}
		if !ipAllow("ap|"+u.ID, 5) {
			fail(w, 429, "Slow down a little")
			return
		}
		b := in(r)
		txt := strings.TrimSpace(b["text"])
		if rn(txt) < 10 || rn(txt) > 500 {
			fail(w, 400, "Explain in 10 to 500 characters")
			return
		}
		for _, a := range x7().Apps {
			if a.User == u.ID && a.Status == "pending" {
				fail(w, 400, "Your appeal is already under review")
				return
			}
		}
		kind := "other"
		if u.Banned {
			kind = "ban"
		} else if x().Susp[u.ID] > now() {
			kind = "suspension"
		}
		x7().Apps = append(x7().Apps, &Appeal{ID: uid(), User: u.ID, Kind: kind, Ref: trunc(b["ref"], 40), Text: txt, Status: "pending", TS: now()})
		if len(x7().Apps) > 500 {
			x7().Apps = x7().Apps[100:]
		}
		save()
		js(w, map[string]bool{"ok": true})
	})
	api("appeals", func(u *User, w http.ResponseWriter, r *http.Request) {
		if !u.Admin && !isMod(u.ID) {
			fail(w, 403, "Moderators only")
			return
		}
		out := []map[string]any{}
		for _, a := range x7().Apps {
			if a.Status == "pending" && S.Users[a.User] != nil && len(out) < 50 {
				out = append(out, map[string]any{"ID": a.ID, "U": uv(a.User), "Kind": a.Kind, "Text": a.Text, "TS": a.TS})
			}
		}
		js(w, out)
	})
	api("appealact", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		if !u.Admin && !isMod(u.ID) {
			fail(w, 403, "Moderators only")
			return
		}
		for _, a := range x7().Apps {
			if a.ID != b["id"] || a.Status != "pending" {
				continue
			}
			o := S.Users[a.User]
			if o == nil {
				a.Status = "closed"
				break
			}
			if b["v"] == "1" {
				if a.Kind == "ban" && !u.Admin {
					fail(w, 403, "Only an admin can lift a ban")
					return
				}
				o.Banned = false
				delete(x().Susp, o.ID)
				if a.Ref != "" {
					delete(x7().Flag, a.Ref)
				}
				a.Status = "approved"
				S.Notifs = append(S.Notifs, Notif{o.ID, o.ID, "aok", "", now(), false})
				push(o.ID, "aok")
			} else {
				a.Status = "rejected"
				S.Notifs = append(S.Notifs, Notif{o.ID, o.ID, "ano", "", now(), false})
				push(o.ID, "ano")
			}
			alog(u, a.Status+" an appeal from @"+o.Username)
			save()
			js(w, ok)
			return
		}
		fail(w, 404, "Appeal not found")
	})
}

// =====================================================================
// ---------- Batch 5: Checkers, Battleship, Dots and Boxes, Trivia, Ran Coin wallet, gifts, boost ----------
// =====================================================================

// ---------- checkers (English draughts: captures are compulsory, men that reach the far side become kings) ----------
func chkOwner(v int) int {
	if v == 1 || v == 3 {
		return 1
	}
	if v == 2 || v == 4 {
		return 2
	}
	return 0
}

func chkMoves(b []int, p, only int) (caps, simple [][2]int) {
	for i, v := range b {
		if chkOwner(v) != p || (only >= 0 && i != only) {
			continue
		}
		r, c := i/8, i%8
		for _, dr := range []int{-1, 1} {
			if (v == 1 && dr != -1) || (v == 2 && dr != 1) {
				continue
			}
			for _, dc := range []int{-1, 1} {
				r1, c1 := r+dr, c+dc
				if r1 < 0 || r1 > 7 || c1 < 0 || c1 > 7 {
					continue
				}
				t := b[r1*8+c1]
				if t == 0 {
					simple = append(simple, [2]int{i, r1*8 + c1})
				} else if chkOwner(t) != p {
					r2, c2 := r+2*dr, c+2*dc
					if r2 >= 0 && r2 <= 7 && c2 >= 0 && c2 <= 7 && b[r2*8+c2] == 0 {
						caps = append(caps, [2]int{i, r2*8 + c2})
					}
				}
			}
		}
	}
	return
}

func chkLegal(g *Game, p int) [][2]int {
	only := -1
	if g.Aux > 0 {
		only = g.Aux - 1
	}
	c, s := chkMoves(g.Board, p, only)
	if len(c) > 0 || only >= 0 {
		return c
	}
	return s
}

// chkApply plays a move and reports whether the same player must keep jumping
func chkApply(g *Game, p, from, to int) bool {
	b := g.Board
	v := b[from]
	r1, c1, r2, c2 := from/8, from%8, to/8, to%8
	capture := r2-r1 == 2 || r1-r2 == 2
	man := v == 1 || v == 2
	b[from] = 0
	if capture {
		b[((r1+r2)/2)*8+(c1+c2)/2] = 0
	}
	promoted := false
	if v == 1 && r2 == 0 {
		v, promoted = 3, true
	}
	if v == 2 && r2 == 7 {
		v, promoted = 4, true
	}
	b[to] = v
	g.Aux = 0
	if capture || man {
		g.Rd = 0
	} else {
		g.Rd++
	}
	if capture && !promoted {
		if c, _ := chkMoves(b, p, to); len(c) > 0 {
			g.Aux = to + 1
			return true
		}
	}
	return false
}

// ---------- battleship ----------
var shipLens = []int{5, 4, 3, 3, 2}

func placeFleet() []int {
	for {
		f := make([]int, 100)
		all := true
		for id, L := range shipLens {
			placed := false
			for t := 0; t < 200 && !placed; t++ {
				rb := make([]byte, 3)
				rand.Read(rb)
				horiz := rb[0]%2 == 0
				r, c := int(rb[1])%10, int(rb[2])%10
				if (horiz && c+L > 10) || (!horiz && r+L > 10) {
					continue
				}
				free := true
				for k := 0; k < L; k++ {
					rr, cc := r, c
					if horiz {
						cc += k
					} else {
						rr += k
					}
					if f[rr*10+cc] != 0 {
						free = false
						break
					}
				}
				if !free {
					continue
				}
				for k := 0; k < L; k++ {
					rr, cc := r, c
					if horiz {
						cc += k
					} else {
						rr += k
					}
					f[rr*10+cc] = id + 1
				}
				placed = true
			}
			if !placed {
				all = false
				break
			}
		}
		if all {
			return f
		}
	}
}

// lengths of the ships that are completely hit
func sunkShips(fleet, shots []int) []int {
	out := []int{}
	for id := 1; id <= len(shipLens); id++ {
		whole, n := true, 0
		for i, v := range fleet {
			if v == id {
				n++
				if shots[i] != 2 {
					whole = false
				}
			}
		}
		if whole && n > 0 {
			out = append(out, shipLens[id-1])
		}
	}
	return out
}

// ---------- dots and boxes (4 x 4 boxes) ----------
func dabBox(e []int, r, c int) bool {
	return e[r*4+c] != 0 && e[(r+1)*4+c] != 0 && e[20+r*5+c] != 0 && e[20+r*5+c+1] != 0
}

// ---------- trivia battle ----------
type TQ struct {
	T string
	O [4]string
	C int
}

var triBank = []TQ{
	{"What is the capital of Australia?", [4]string{"Sydney", "Melbourne", "Canberra", "Perth"}, 2},
	{"Which planet is known as the Red Planet?", [4]string{"Venus", "Mars", "Jupiter", "Mercury"}, 1},
	{"How many players does one football team have on the pitch?", [4]string{"9", "10", "11", "12"}, 2},
	{"What is the largest ocean on Earth?", [4]string{"Atlantic", "Indian", "Pacific", "Arctic"}, 2},
	{"What is the chemical symbol for gold?", [4]string{"Ag", "Au", "Gd", "Go"}, 1},
	{"Who painted the Mona Lisa?", [4]string{"Michelangelo", "Leonardo da Vinci", "Raphael", "Van Gogh"}, 1},
	{"Which country won the 2018 FIFA World Cup?", [4]string{"Croatia", "Belgium", "France", "Brazil"}, 2},
	{"Which African country has the largest population?", [4]string{"Nigeria", "Ethiopia", "Egypt", "South Africa"}, 0},
	{"How many continents are there?", [4]string{"5", "6", "7", "8"}, 2},
	{"What is the hardest natural substance?", [4]string{"Gold", "Iron", "Diamond", "Quartz"}, 2},
	{"Which gas do plants take in from the air?", [4]string{"Oxygen", "Nitrogen", "Carbon dioxide", "Helium"}, 2},
	{"The Great Wall is in which country?", [4]string{"India", "China", "Japan", "Mongolia"}, 1},
	{"Lagos is a city in which country?", [4]string{"Ghana", "Nigeria", "Kenya", "Senegal"}, 1},
	{"Which instrument has 88 keys?", [4]string{"Guitar", "Violin", "Piano", "Flute"}, 2},
	{"What is the fastest land animal?", [4]string{"Lion", "Cheetah", "Horse", "Leopard"}, 1},
	{"How long is a normal football match without extra time?", [4]string{"80 minutes", "90 minutes", "100 minutes", "120 minutes"}, 1},
	{"Who wrote Romeo and Juliet?", [4]string{"Charles Dickens", "William Shakespeare", "Mark Twain", "Jane Austen"}, 1},
	{"What is the currency of Japan?", [4]string{"Yuan", "Won", "Yen", "Ringgit"}, 2},
	{"Which planet is famous for its rings?", [4]string{"Mars", "Saturn", "Venus", "Mercury"}, 1},
	{"At what temperature does water boil at sea level?", [4]string{"90 °C", "100 °C", "110 °C", "120 °C"}, 1},
	{"What is the largest animal on Earth?", [4]string{"Elephant", "Blue whale", "Giraffe", "Shark"}, 1},
	{"What is the capital of Canada?", [4]string{"Toronto", "Vancouver", "Ottawa", "Montreal"}, 2},
	{"Which language has the most native speakers?", [4]string{"English", "Spanish", "Mandarin Chinese", "Hindi"}, 2},
	{"In which year did humans first land on the Moon?", [4]string{"1959", "1969", "1979", "1989"}, 1},
	{"Which is the longest river in Africa?", [4]string{"Niger", "Congo", "Nile", "Zambezi"}, 2},
	{"How many strings does a standard guitar have?", [4]string{"4", "5", "6", "7"}, 2},
	{"What is H2O more commonly called?", [4]string{"Salt", "Water", "Bleach", "Oxygen"}, 1},
	{"Rio de Janeiro is a city in which country?", [4]string{"Argentina", "Brazil", "Portugal", "Chile"}, 1},
	{"What is the smallest prime number?", [4]string{"0", "1", "2", "3"}, 2},
	{"Which organ pumps blood around the body?", [4]string{"Lungs", "Liver", "Heart", "Kidney"}, 2},
	{"Which mountain is the highest above sea level?", [4]string{"K2", "Mount Everest", "Kilimanjaro", "Denali"}, 1},
	{"Which sport is played at Wimbledon?", [4]string{"Golf", "Tennis", "Cricket", "Rugby"}, 1},
	{"Which company makes the iPhone?", [4]string{"Samsung", "Apple", "Google", "Sony"}, 1},
	{"How many days are in a leap year?", [4]string{"364", "365", "366", "367"}, 2},
	{"In which country is the Eiffel Tower?", [4]string{"Italy", "France", "Spain", "Belgium"}, 1},
	{"Which set contains the primary colours of light?", [4]string{"Red, yellow, blue", "Red, green, blue", "Cyan, magenta, yellow", "Black, white, grey"}, 1},
	{"Which is the smallest continent by land area?", [4]string{"Europe", "Australia (Oceania)", "Antarctica", "South America"}, 1},
	{"Who is known as the King of Pop?", [4]string{"Elvis Presley", "Michael Jackson", "Prince", "Justin Bieber"}, 1},
	{"Which metal is liquid at room temperature?", [4]string{"Iron", "Mercury", "Lead", "Tin"}, 1},
	{"The pyramids of Giza are in which country?", [4]string{"Morocco", "Egypt", "Sudan", "Libya"}, 1},
}

func triNew(g *Game) {
	idx := make([]int, len(triBank))
	for i := range idx {
		idx[i] = i
	}
	for i := len(idx) - 1; i > 0; i-- {
		rb := make([]byte, 2)
		rand.Read(rb)
		j := (int(rb[0])<<8 | int(rb[1])) % (i + 1)
		idx[i], idx[j] = idx[j], idx[i]
	}
	g.Qs, g.Qi, g.QTS, g.A1, g.A2 = idx[:7], 0, now(), 0, 0
}

func triResolve(g *Game) {
	q := triBank[g.Qs[g.Qi]]
	g.Lq, g.La1, g.La2 = g.Qs[g.Qi]+1, g.A1, g.A2
	if g.A1 == q.C+1 {
		g.S1 += 10
	}
	if g.A2 == q.C+1 {
		g.S2 += 10
	}
	g.A1, g.A2 = 0, 0
	g.Qi++
	g.QTS, g.Upd = now(), now()
	if g.Qi >= len(g.Qs) {
		switch {
		case g.S1 > g.S2:
			g.Status, g.Win = "won", g.A
		case g.S2 > g.S1:
			g.Status, g.Win = "won", g.B
		default:
			g.Status = "draw"
		}
		payGame(g)
	}
}

// triTick moves the quiz on when both players answered or 15 seconds passed; returns true if anything changed
func triTick(g *Game) bool {
	if g.Kind != "tri" || g.Status != "active" || g.Qi >= len(g.Qs) {
		return false
	}
	if (g.A1 != 0 && g.A2 != 0) || now()-g.QTS >= 15 {
		triResolve(g)
		return true
	}
	return false
}

// newKindMove plays one move of the four new games. It returns an error message, or "" when the move was accepted.
func newKindMove(g *Game, uid, ms string) string {
	isA := uid == g.A
	p, opp := 1, g.B
	if !isA {
		p, opp = 2, g.A
	}
	if g.Kind != "tri" && g.Turn != uid {
		return "It is not your turn"
	}
	switch g.Kind {
	case "chk":
		pr := strings.Split(ms, "-")
		if len(pr) != 2 {
			return "Pick a piece and a square"
		}
		from, e1 := strconv.Atoi(pr[0])
		to, e2 := strconv.Atoi(pr[1])
		if e1 != nil || e2 != nil {
			return "Invalid move"
		}
		legal := false
		for _, mv := range chkLegal(g, p) {
			if mv[0] == from && mv[1] == to {
				legal = true
			}
		}
		if !legal {
			return "That move is not allowed"
		}
		if !chkApply(g, p, from, to) {
			g.Turn = opp
			if c, s := chkMoves(g.Board, 3-p, -1); len(c)+len(s) == 0 {
				g.Status, g.Win = "won", uid
			} else if g.Rd >= 80 {
				g.Status = "draw"
			}
		}
	case "bs":
		idx, e := strconv.Atoi(ms)
		if e != nil || idx < 0 || idx > 99 {
			return "Pick a square"
		}
		shots, target := g.Sh1, g.F2
		if !isA {
			shots, target = g.Sh2, g.F1
		}
		if shots[idx] != 0 {
			return "You already fired there"
		}
		if target[idx] != 0 {
			shots[idx] = 2
		} else {
			shots[idx] = 1
		}
		hits := 0
		for _, v := range shots {
			if v == 2 {
				hits++
			}
		}
		if hits >= 17 {
			g.Status, g.Win = "won", uid
		} else {
			g.Turn = opp
		}
	case "dab":
		m, e := strconv.Atoi(ms)
		if e != nil || m < 0 || m > 39 || g.Board[m] != 0 {
			return "That line is already drawn"
		}
		g.Board[m] = p
		var cand [][2]int
		if m < 20 {
			hr, hc := m/4, m%4
			if hr > 0 {
				cand = append(cand, [2]int{hr - 1, hc})
			}
			if hr < 4 {
				cand = append(cand, [2]int{hr, hc})
			}
		} else {
			k := m - 20
			vr, vc := k/5, k%5
			if vc > 0 {
				cand = append(cand, [2]int{vr, vc - 1})
			}
			if vc < 4 {
				cand = append(cand, [2]int{vr, vc})
			}
		}
		got := 0
		for _, bx := range cand {
			if g.Bx[bx[0]*4+bx[1]] == 0 && dabBox(g.Board, bx[0], bx[1]) {
				g.Bx[bx[0]*4+bx[1]] = p
				got++
			}
		}
		if p == 1 {
			g.S1 += got
		} else {
			g.S2 += got
		}
		if got == 0 {
			g.Turn = opp
		}
		if g.S1+g.S2 == 16 {
			switch {
			case g.S1 > g.S2:
				g.Status, g.Win = "won", g.A
			case g.S2 > g.S1:
				g.Status, g.Win = "won", g.B
			default:
				g.Status = "draw"
			}
		}
	case "tri":
		triTick(g)
		if g.Status != "active" {
			return "This game is over"
		}
		pr := strings.Split(ms, ":")
		if len(pr) != 2 {
			return "Pick an answer"
		}
		qn, e1 := strconv.Atoi(pr[0])
		o, e2 := strconv.Atoi(pr[1])
		if e1 != nil || e2 != nil || o < 0 || o > 3 {
			return "Pick an answer"
		}
		if qn != g.Qi {
			return "That question has already ended"
		}
		if (isA && g.A1 != 0) || (!isA && g.A2 != 0) {
			return "You already answered"
		}
		if isA {
			g.A1 = o + 1
		} else {
			g.A2 = o + 1
		}
		triTick(g)
	default:
		return "Unknown game"
	}
	g.Mv++
	if g.Status != "active" {
		payGame(g)
	}
	return ""
}

// gameViewX adds what each of the new games needs to show
func gameViewX(v map[string]any, g *Game, me string, p int) map[string]any {
	switch g.Kind {
	case "chk":
		if g.Status == "active" && g.Turn == me {
			v["Moves"] = chkLegal(g, p)
		}
	case "bs":
		mine, myShots, theirShots, theirFleet := g.F1, g.Sh1, g.Sh2, g.F2
		if p == 2 {
			mine, myShots, theirShots, theirFleet = g.F2, g.Sh2, g.Sh1, g.F1
		}
		v["F"], v["SH"], v["OS"] = mine, myShots, theirShots
		v["OppSunk"], v["MySunk"] = sunkShips(theirFleet, myShots), sunkShips(mine, theirShots)
		if g.Status != "active" {
			v["OppF"] = theirFleet
		}
	case "dab":
		v["Bx"] = g.Bx
	case "tri":
		ma, oa := g.A1, g.A2
		if p == 2 {
			ma, oa = g.A2, g.A1
		}
		v["My"], v["OppPicked"] = ma, oa != 0
		v["QT"] = len(g.Qs)
		if g.Status == "active" && g.Qi < len(g.Qs) {
			q := triBank[g.Qs[g.Qi]]
			left := 15 - int(now()-g.QTS)
			if left < 0 {
				left = 0
			}
			v["Q"] = map[string]any{"T": q.T, "O": q.O[:]}
			v["QN"], v["Left"] = g.Qi+1, left
		}
		if g.Lq > 0 {
			lq := triBank[g.Lq-1]
			la, lb := g.La1, g.La2
			if p == 2 {
				la, lb = lb, la
			}
			v["LQ"] = map[string]any{"T": lq.T, "O": lq.O[:], "C": lq.C}
			v["LA"] = []int{la, lb}
		}
	}
	return v
}

// ---------- Ran Coin: history, gifts, profile boost ----------
type Led struct {
	ID, User, Kind, Note string
	D                    int
	TS                   int64
}
type Ext8 struct {
	Led   []*Led
	Boost map[string]int64
	GSent map[string]int
	GGot  map[string]int
	GDay  map[string]int
}

func x8() *Ext8 {
	e := &S.X8
	if e.Boost == nil {
		e.Boost = map[string]int64{}
	}
	if e.GSent == nil {
		e.GSent = map[string]int{}
	}
	if e.GGot == nil {
		e.GGot = map[string]int{}
	}
	if e.GDay == nil {
		e.GDay = map[string]int{}
	}
	return e
}

func ledger(id string, d int, kind, note string) {
	e := x8()
	e.Led = append(e.Led, &Led{uid(), id, kind, note, d, now()})
	if len(e.Led) > 20000 {
		e.Led = e.Led[5000:]
	}
}

type GiftT struct {
	ID, Name, Icon string
	Price          int
}

var giftList = []GiftT{{"rose", "Rose", "🌹", 10}, {"heart", "Heart", "💖", 25}, {"star", "Star", "⭐", 50}, {"crown", "Crown", "👑", 100}, {"rocket", "Rocket", "🚀", 250}, {"diamond", "Diamond", "💎", 500}}
var boostTiers = map[string][2]int{"d1": {100, 86400}, "d3": {250, 3 * 86400}, "d7": {500, 7 * 86400}}

// boosted returns up to 3 promoted people for the "who to follow" lists, rotating every 10 minutes
func boosted(u *User) []map[string]any {
	type be struct{ id, key string }
	l := []be{}
	bucket := strconv.FormatInt(now()/600, 10)
	for id, t := range S.X8.Boost {
		o := S.Users[id]
		if t <= now() || id == u.ID || has(u.Following, id) || o == nil || o.Banned || o.Username == "" || blk(u, o) {
			continue
		}
		h := sha256.Sum256([]byte(id + bucket))
		l = append(l, be{id, hex.EncodeToString(h[:4])})
	}
	sort.Slice(l, func(a, b int) bool { return l[a].key < l[b].key })
	out := []map[string]any{}
	for i := 0; i < len(l) && i < 3; i++ {
		m := uv(l[i].id)
		m["Promoted"] = true
		out = append(out, m)
	}
	return out
}

func purgeX8(id string) {
	e := x8()
	delete(e.Boost, id)
	delete(e.GSent, id)
	delete(e.GGot, id)
	keep := e.Led[:0]
	for _, l := range e.Led {
		if l.User != id {
			keep = append(keep, l)
		}
	}
	e.Led = keep
}

func init() {
	pushEv["gift"] = true
	extra12()
}

func extra12() {
	go func() { // forget old daily gift counters
		for range time.Tick(30 * time.Minute) {
			mu.Lock()
			for k := range x8().GDay {
				if !strings.HasSuffix(k, today()) {
					delete(x8().GDay, k)
				}
			}
			mu.Unlock()
		}
	}()

	api("wallet", func(u *User, w http.ResponseWriter, r *http.Request) {
		g := gpf(u.ID)
		e := x8()
		rows := []map[string]any{}
		for i := len(e.Led) - 1; i >= 0 && len(rows) < 40; i-- {
			if l := e.Led[i]; l.User == u.ID {
				rows = append(rows, map[string]any{"D": l.D, "Kind": l.Kind, "Note": l.Note, "TS": l.TS})
			}
		}
		js(w, map[string]any{"Coins": g.Coins, "Level": lvl(g.XP), "Rows": rows, "Sent": e.GSent[u.ID], "Got": e.GGot[u.ID], "Boost": e.Boost[u.ID], "Inf": u.Admin})
	})
	api("giftlist", func(u *User, w http.ResponseWriter, r *http.Request) {
		js(w, map[string]any{"Gifts": giftList, "Coins": gpf(u.ID).Coins, "Inf": u.Admin})
	})
	api("giftsend", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		o := S.Users[b["to"]]
		var gt *GiftT
		for i := range giftList {
			if giftList[i].ID == b["id"] {
				gt = &giftList[i]
			}
		}
		if gt == nil || o == nil || o.ID == u.ID || o.Banned || blk(u, o) {
			fail(w, 400, "You cannot send this gift")
			return
		}
		e := x8()
		key := u.ID + "|" + today()
		if e.GDay[key] >= 50 {
			fail(w, 400, "You reached today's gift limit")
			return
		}
		g := gpf(u.ID)
		if !spendCoins(u, g, gt.Price) {
			fail(w, 400, "Not enough Ran Coin")
			return
		}
		back := gt.Price / 2 // the receiver gets half in Ran Coin
		gpf(o.ID).Coins += back
		e.GDay[key]++
		e.GSent[u.ID]++
		e.GGot[o.ID]++
		ledger(u.ID, spentAmt(u, gt.Price), "gift", "Sent a "+gt.Name+" to @"+o.Username+adminNote(u))
		ledger(o.ID, back, "gift", "Received a "+gt.Name+" from @"+u.Username)
		S.Msgs = append(S.Msgs, Msg{From: u.ID, To: o.ID, Text: "🎁 Sent you a " + gt.Name + " " + gt.Icon, TS: now(), Exp: dexp(u.ID, o.ID)})
		note(o.ID, u.ID, "gift", "")
		push(o.ID, "msg")
		save()
		js(w, map[string]int{"Coins": g.Coins})
	})
	api("boostbuy", func(u *User, w http.ResponseWriter, r *http.Request) {
		t, k := boostTiers[in(r)["tier"]]
		if !k {
			fail(w, 400, "Unknown boost")
			return
		}
		e := x8()
		if e.Boost[u.ID]-now() > 14*86400 {
			fail(w, 400, "Your profile is already boosted for a long time")
			return
		}
		g := gpf(u.ID)
		if !spendCoins(u, g, t[0]) {
			fail(w, 400, "Not enough Ran Coin")
			return
		}
		start := e.Boost[u.ID]
		if start < now() {
			start = now()
		}
		e.Boost[u.ID] = start + int64(t[1])
		ledger(u.ID, spentAmt(u, t[0]), "boost", "Profile boost"+adminNote(u))
		save()
		js(w, map[string]any{"Coins": g.Coins, "Until": e.Boost[u.ID]})
	})
}

// =====================================================================
// ---------- Batch 6: unlimited Ran Coin for admins, score verification, abandoned games ----------
// =====================================================================

// spendCoins takes a price from a balance. Admins have unlimited Ran Coin and are never charged.
func spendCoins(u *User, g *GP, price int) bool {
	if u.Admin {
		return true
	}
	if g.Coins < price {
		return false
	}
	g.Coins -= price
	return true
}

// spentAmt is the amount to write in the history: negative for a normal user, 0 for an admin
func spentAmt(u *User, price int) int {
	if u.Admin {
		return 0
	}
	return -price
}

func adminNote(u *User) string {
	if u.Admin {
		return " (free for admins)"
	}
	return ""
}

// ---------- solo-game scores must come from a game the server saw start ----------
type gTokE struct {
	user, game string
	ts         int64
}

var gTok = map[string]gTokE{}

// ---------- games nobody finishes ----------
// forfeitStale ends games where nobody has moved for 2 days, so people are never stuck at the open-games limit
func forfeitStale(e *Ext6) {
	for _, g := range e.Games {
		if g.Status != "active" || g.Tour != "" || now()-g.Upd < 2*86400 {
			continue
		}
		loser := g.A
		switch g.Kind {
		case "rps":
			if g.P1 != "" && g.P2 == "" {
				loser = g.B
			}
		case "tri":
			if g.A1 != 0 && g.A2 == 0 {
				loser = g.B
			}
		default:
			if g.Turn == g.B {
				loser = g.B
			}
		}
		winner := g.A
		if loser == g.A {
			winner = g.B
		}
		g.Status, g.Win, g.Paid, g.Upd = "resigned", winner, true, now()
		push(winner, "game:"+g.ID)
	}
}

func init() { extra13() }

func extra13() {
	api("gstart", func(u *User, w http.ResponseWriter, r *http.Request) {
		game := in(r)["game"]
		if game != "snake" && game != "memory" && game != "streetracer" && game != "skyparkour" && game != "lostisland" && game != "neonrunner" {
			fail(w, 400, "Unknown game")
			return
		}
		if len(gTok) > 5000 {
			for k, t := range gTok {
				if now()-t.ts > 3600 {
					delete(gTok, k)
				}
			}
		}
		tok := uid() + uid()
		gTok[tok] = gTokE{u.ID, game, now()}
		js(w, map[string]string{"tok": tok})
	})
}

// =====================================================================
// Amlink live 3D race rooms (HTTP state sync; no external websocket service)
// =====================================================================
type GRoomPlayer struct {
	User     string
	Ready    bool
	Score    int
	Progress int
	Lane     int
	Finished bool
	Updated  int64
}
type GRoom struct {
	ID, Code, Game, Host, Status string
	Created, StartAt, Updated    int64
	Players                      map[string]*GRoomPlayer
}
type Ext9 struct{ Rooms map[string]*GRoom }

func x9() *Ext9 {
	e := &S.X9
	if e.Rooms == nil {
		e.Rooms = map[string]*GRoom{}
	}
	return e
}

// Reels engagement and recommendation signals. Persisted with the main state.
type Ext10 struct {
	Views           map[string]int            `json:"views"`
	Seen            map[string]int64          `json:"seen"`            // user|post -> last counted view timestamp
	Signals         map[string]map[string]int `json:"signals"`         // user -> post -> weighted engagement
	CreatorAffinity map[string]map[string]int `json:"creatorAffinity"` // viewer -> creator -> score
	Shares          map[string]int            `json:"shares"`
}

func x10() *Ext10 {
	e := &S.X10
	if e.Views == nil {
		e.Views = map[string]int{}
	}
	if e.Seen == nil {
		e.Seen = map[string]int64{}
	}
	if e.Signals == nil {
		e.Signals = map[string]map[string]int{}
	}
	if e.CreatorAffinity == nil {
		e.CreatorAffinity = map[string]map[string]int{}
	}
	if e.Shares == nil {
		e.Shares = map[string]int{}
	}
	return e
}

func liveGameOK(g string) bool {
	return g == "streetracer" || g == "skyparkour" || g == "lostisland" || g == "neonrunner"
}
func roomPlayerView(p *GRoomPlayer) map[string]any {
	return map[string]any{"U": uv(p.User), "Ready": p.Ready, "Score": p.Score, "Progress": p.Progress, "Lane": p.Lane, "Finished": p.Finished}
}
func roomView(g *GRoom) map[string]any {
	ps := []map[string]any{}
	for _, p := range g.Players {
		if S.Users[p.User] != nil {
			ps = append(ps, roomPlayerView(p))
		}
	}
	sort.Slice(ps, func(i, j int) bool { return ps[i]["Score"].(int) > ps[j]["Score"].(int) })
	return map[string]any{"ID": g.ID, "Code": g.Code, "Game": g.Game, "Host": g.Host, "Status": g.Status, "StartAt": g.StartAt, "Players": ps, "MaxPlayers": 4}
}
func roomMember(g *GRoom, uid string) bool { return g != nil && g.Players[uid] != nil }
func init()                                { extra14() }
func extra14() {
	api("roomcreate", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		game := b["game"]
		if !liveGameOK(game) {
			fail(w, 400, "Unknown 3D game")
			return
		}
		for _, old := range x9().Rooms {
			if old.Status != "finished" && roomMember(old, u.ID) && now()-old.Updated < 3600 {
				fail(w, 409, "Leave your current race room first")
				return
			}
		}
		code := ""
		for i := 0; i < 6; i++ {
			code += strings.ToUpper(uid()[:1])
		}
		id := uid()
		g := &GRoom{ID: id, Code: code, Game: game, Host: u.ID, Status: "waiting", Created: now(), Updated: now(), Players: map[string]*GRoomPlayer{u.ID: {User: u.ID, Updated: now()}}}
		x9().Rooms[id] = g
		save()
		js(w, roomView(g))
	})
	api("roomlist", func(u *User, w http.ResponseWriter, r *http.Request) {
		game := r.URL.Query().Get("g")
		out := []map[string]any{}
		for _, g := range x9().Rooms {
			if g.Status == "waiting" && (game == "" || g.Game == game) && len(g.Players) < 4 && now()-g.Updated < 1800 {
				out = append(out, roomView(g))
			}
		}
		sort.Slice(out, func(i, j int) bool { return out[i]["ID"].(string) > out[j]["ID"].(string) })
		if len(out) > 20 {
			out = out[:20]
		}
		js(w, out)
	})
	api("roomjoin", func(u *User, w http.ResponseWriter, r *http.Request) {
		code := strings.ToUpper(strings.TrimSpace(in(r)["code"]))
		var g *GRoom
		for _, v := range x9().Rooms {
			if v.Code == code {
				g = v
				break
			}
		}
		if g == nil || g.Status != "waiting" || now()-g.Updated > 1800 {
			fail(w, 404, "Room not found or already started")
			return
		}
		if !roomMember(g, u.ID) && len(g.Players) >= 4 {
			fail(w, 409, "This room is full")
			return
		}
		if !roomMember(g, u.ID) {
			g.Players[u.ID] = &GRoomPlayer{User: u.ID, Updated: now()}
		}
		g.Updated = now()
		save()
		js(w, roomView(g))
	})
	api("roomready", func(u *User, w http.ResponseWriter, r *http.Request) {
		id := in(r)["id"]
		g := x9().Rooms[id]
		if !roomMember(g, u.ID) || g.Status != "waiting" {
			fail(w, 404, "Waiting room not found")
			return
		}
		g.Players[u.ID].Ready = true
		g.Players[u.ID].Updated = now()
		g.Updated = now()
		save()
		js(w, roomView(g))
	})
	api("roomstart", func(u *User, w http.ResponseWriter, r *http.Request) {
		id := in(r)["id"]
		g := x9().Rooms[id]
		if !roomMember(g, u.ID) || g.Host != u.ID {
			fail(w, 403, "Only the room host can start the race")
			return
		}
		if g.Status != "waiting" || len(g.Players) < 2 {
			fail(w, 400, "At least two players are needed")
			return
		}
		for _, p := range g.Players {
			if !p.Ready {
				fail(w, 400, "Everyone must be ready first")
				return
			}
		}
		g.Status = "countdown"
		g.StartAt = now() + 5
		g.Updated = now()
		save()
		js(w, roomView(g))
	})
	api("roomstate", func(u *User, w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		g := x9().Rooms[id]
		if !roomMember(g, u.ID) {
			fail(w, 404, "You are not in this room")
			return
		}
		if g.Status == "countdown" && now() >= g.StartAt {
			g.Status = "racing"
			g.Updated = now()
			save()
		}
		js(w, roomView(g))
	})
	api("roomtick", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		g := x9().Rooms[b["id"]]
		if !roomMember(g, u.ID) || g.Status != "racing" {
			fail(w, 409, "Race is not active")
			return
		}
		p := g.Players[u.ID]
		score, _ := strconv.Atoi(b["score"])
		progress, _ := strconv.Atoi(b["progress"])
		lane, _ := strconv.Atoi(b["lane"])
		if score < p.Score {
			score = p.Score
		}
		if score > 100000 {
			score = 100000
		}
		if progress < p.Progress {
			progress = p.Progress
		}
		if progress > 100 {
			progress = 100
		}
		if lane < -1 {
			lane = -1
		}
		if lane > 1 {
			lane = 1
		}
		p.Score, p.Progress, p.Lane, p.Updated = score, progress, lane, now()
		if b["finished"] == "true" {
			p.Finished = true
			p.Progress = 100
		}
		all := true
		for _, q := range g.Players {
			if !q.Finished {
				all = false
				break
			}
		}
		if all {
			g.Status = "finished"
		}
		g.Updated = now()
		save()
		js(w, roomView(g))
	})
	api("roomleave", func(u *User, w http.ResponseWriter, r *http.Request) {
		id := in(r)["id"]
		g := x9().Rooms[id]
		if !roomMember(g, u.ID) {
			js(w, map[string]bool{"ok": true})
			return
		}
		delete(g.Players, u.ID)
		if len(g.Players) == 0 || g.Host == u.ID {
			g.Status = "finished"
		}
		g.Updated = now()
		save()
		js(w, map[string]bool{"ok": true})
	})
}

// ---------- Amlink Empire: server-authoritative two-player kingdom tactics ----------
type EmpirePoint struct {
	X float64 `json:"x"`
	Z float64 `json:"z"`
}
type EmpireLiveUnit struct {
	ID        string        `json:"id"`
	Owner     string        `json:"owner"`
	Team      int           `json:"team"`
	Type      string        `json:"type"`
	X         float64       `json:"x"`
	Z         float64       `json:"z"`
	HP        int           `json:"hp"`
	Path      []EmpirePoint `json:"path"`
	PathIndex int           `json:"pathIndex"`
	AttackAt  int64         `json:"attackAt"`
}
type EmpireBattle struct {
	ID, Code, Host, Opponent, Status string
	Players                          []string
	Turn                             int
	HP                               map[string]int
	Gold                             map[string]int
	Army                             map[string]map[string]int
	Buildings                        map[string]map[string]int `json:"buildings,omitempty"`
	LiveUnits                        []EmpireLiveUnit          `json:"liveUnits,omitempty"`
	LastTick                         int64                     `json:"lastTick,omitempty"`
	Pending                          map[string]string
	Log                              []string
	Created, Updated                 int64
}
type Ext11 struct {
	Battles    map[string]*EmpireBattle   `json:"empireBattles"`
	Inventory  map[string]map[string]int  `json:"raceInventory"`
	Cars       map[string][]string        `json:"raceCars"`
	StoreOwned map[string]map[string]bool `json:"gameStoreOwned"`
}

func x11() *Ext11 {
	if S.X11.Battles == nil {
		S.X11.Battles = map[string]*EmpireBattle{}
	}
	if S.X11.Inventory == nil {
		S.X11.Inventory = map[string]map[string]int{}
	}
	if S.X11.Cars == nil {
		S.X11.Cars = map[string][]string{}
	}
	if S.X11.StoreOwned == nil {
		S.X11.StoreOwned = map[string]map[string]bool{}
	}
	return &S.X11
}
func empireView(g *EmpireBattle, uid string) map[string]any {
	return map[string]any{"ID": g.ID, "Code": g.Code, "Host": g.Host, "Opponent": g.Opponent, "Status": g.Status, "Players": len(g.Players), "Turn": g.Turn, "HP": g.HP, "Gold": g.Gold, "Army": g.Army, "Buildings": g.Buildings, "LiveUnits": g.LiveUnits, "Pending": g.Pending, "Log": g.Log, "Me": uid, "Updated": g.Updated}
}
func empireMember(g *EmpireBattle, uid string) bool {
	return g != nil && (g.Host == uid || g.Opponent == uid)
}
func empireResolve(g *EmpireBattle) {
	a, b := g.Host, g.Opponent
	if g.Pending[a] == "" || g.Pending[b] == "" {
		return
	}
	power := func(uid string, tactic string) int {
		ar := g.Army[uid]
		p := ar["spearmen"]*2 + ar["swordsmen"]*3 + ar["archers"]*2 + ar["knights"]*5 + ar["guards"]*3
		switch tactic {
		case "defend":
			p = p*3/4 + ar["guards"]*2
		case "flank":
			p = p + ar["knights"]*2 - ar["guards"]
		default:
			p = p + ar["swordsmen"]
		}
		if p < 1 {
			p = 1
		}
		return p
	}
	pa, pb := power(a, g.Pending[a]), power(b, g.Pending[b])
	damage := func(att, def int) int {
		d := att/4 + 1
		if d > 22 {
			d = 22
		}
		if def > att {
			d = 1
		}
		return d
	}
	da, db := damage(pb, pa), damage(pa, pb)
	if g.Pending[a] == "defend" {
		da = (da + 1) / 2
	}
	if g.Pending[b] == "defend" {
		db = (db + 1) / 2
	}
	// Defensive structures reduce incoming damage; gold mines increase round income.
	if g.Buildings != nil {
		defence := func(uid string) int { b := g.Buildings[uid]; return b["cannon"]*2 + b["archer_tower"]*2 + b["wall"] }
		da = max(1, da-defence(a))
		db = max(1, db-defence(b))
		g.Gold[a] += 4 + g.Buildings[a]["gold_mine"]*2
		g.Gold[b] += 4 + g.Buildings[b]["gold_mine"]*2
	} else {
		g.Gold[a] += 5
		g.Gold[b] += 5
	}
	g.HP[a] -= da
	g.HP[b] -= db
	g.Log = append(g.Log, fmt.Sprintf("Turn %d: both kingdoms clashed. %s lost %d health; %s lost %d health.", g.Turn, a, da, b, db))
	g.Pending[a], g.Pending[b] = "", ""
	g.Turn++
	if g.HP[a] <= 0 || g.HP[b] <= 0 {
		g.Status = "finished"
		if g.HP[a] > g.HP[b] {
			g.Log = append(g.Log, "The host kingdom wins the battle.")
		} else if g.HP[b] > g.HP[a] {
			g.Log = append(g.Log, "The challenger kingdom wins the battle.")
		} else {
			g.Log = append(g.Log, "The battle ends in a draw.")
		}
	}
	if len(g.Log) > 40 {
		g.Log = g.Log[len(g.Log)-40:]
	}
}

// Empire uses server-authoritative movement. A small grid A* routes troops around
// the rival's perimeter wall and through its gate; clients only render server state.
func empirePath(team int, lane int) []EmpirePoint {
	const cell = 1.0
	startX, goalX := -10, 14
	if team == 1 {
		startX, goalX = 10, -14
	}
	startZ := float64(lane) * 3
	sx, sz := int(math.Round(float64(startX)/cell)), int(math.Round(startZ/cell))
	gx, gz := int(math.Round(float64(goalX)/cell)), 0
	key := func(x, z int) string { return fmt.Sprintf("%d,%d", x, z) }
	blocked := func(x, z int) bool {
		// Opponent's two side walls; the central gate remains open.
		wallX := 12
		if team == 1 {
			wallX = -12
		}
		if x == wallX && z >= -7 && z <= 7 && (z < -1 || z > 1) {
			return true
		}
		// Decorative building footprints around both keeps.
		for _, cx := range []int{-17, 17} {
			if x >= cx-2 && x <= cx+2 && z >= -2 && z <= 2 {
				return true
			}
		}
		return false
	}
	type node struct {
		x, z int
		f, g int
	}
	open := []node{{sx, sz, absInt(gx-sx) + absInt(gz-sz), 0}}
	came := map[string]string{}
	cost := map[string]int{key(sx, sz): 0}
	closed := map[string]bool{}
	dirs := [][2]int{{1, 0}, {-1, 0}, {0, 1}, {0, -1}}
	for len(open) > 0 {
		best := 0
		for i := 1; i < len(open); i++ {
			if open[i].f < open[best].f {
				best = i
			}
		}
		cur := open[best]
		open = append(open[:best], open[best+1:]...)
		ck := key(cur.x, cur.z)
		if closed[ck] {
			continue
		}
		closed[ck] = true
		if cur.x == gx && cur.z == gz {
			rev := []EmpirePoint{{X: float64(gx), Z: float64(gz)}}
			k := ck
			for k != key(sx, sz) {
				prev, ok := came[k]
				if !ok {
					break
				}
				var x, z int
				fmt.Sscanf(prev, "%d,%d", &x, &z)
				rev = append(rev, EmpirePoint{X: float64(x), Z: float64(z)})
				k = prev
			}
			for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
				rev[i], rev[j] = rev[j], rev[i]
			}
			return rev
		}
		for _, d := range dirs {
			nx, nz := cur.x+d[0], cur.z+d[1]
			if nx < -22 || nx > 22 || nz < -12 || nz > 12 || blocked(nx, nz) {
				continue
			}
			nk := key(nx, nz)
			ng := cur.g + 1
			if old, ok := cost[nk]; ok && ng >= old {
				continue
			}
			cost[nk] = ng
			came[nk] = ck
			h := absInt(gx-nx) + absInt(gz-nz)
			open = append(open, node{nx, nz, ng + h, ng})
		}
	}
	// Safe fallback through the gate if the grid is unexpectedly blocked.
	return []EmpirePoint{{X: float64(startX), Z: startZ}, {X: float64(func() int {
		if team == 0 {
			return 12
		}
		return -12
	}()), Z: 0}, {X: float64(goalX), Z: 0}}
}
func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}
func empireTick(g *EmpireBattle) bool {
	nowMS := time.Now().UnixMilli()
	if g.LastTick == 0 {
		g.LastTick = nowMS
		return false
	}
	dt := float64(nowMS-g.LastTick) / 1000
	if dt < .25 {
		return false
	}
	if dt > 1.5 {
		dt = 1.5
	}
	g.LastTick = nowMS
	changed := false
	speed := 2.7 * dt
	for i := range g.LiveUnits {
		u := &g.LiveUnits[i]
		if u.HP <= 0 {
			continue
		}
		// Units engage nearby enemies before continuing toward the opposing keep.
		enemyIdx := -1
		best := 1.15
		for j := range g.LiveUnits {
			v := &g.LiveUnits[j]
			if v.HP <= 0 || v.Team == u.Team {
				continue
			}
			d := math.Hypot(v.X-u.X, v.Z-u.Z)
			if d < best {
				best = d
				enemyIdx = j
			}
		}
		if enemyIdx >= 0 {
			v := &g.LiveUnits[enemyIdx]
			if u.AttackAt <= nowMS {
				v.HP -= 12
				u.AttackAt = nowMS + 900
				changed = true
			}
			continue
		}
		if u.PathIndex < len(u.Path) {
			pt := u.Path[u.PathIndex]
			dx, dz := pt.X-u.X, pt.Z-u.Z
			dist := math.Hypot(dx, dz)
			if dist <= speed || dist < .08 {
				u.X, u.Z = pt.X, pt.Z
				u.PathIndex++
			} else {
				u.X += dx / dist * speed
				u.Z += dz / dist * speed
			}
			changed = true
		} else {
			def := g.Opponent
			if u.Team == 1 {
				def = g.Host
			}
			if g.HP[def] > 0 {
				g.HP[def] -= 5
				u.HP = 0
				g.Log = append(g.Log, fmt.Sprintf("%s troop reached the keep and dealt 5 damage.", u.Type))
				changed = true
			}
		}
	}
	kept := g.LiveUnits[:0]
	for _, u := range g.LiveUnits {
		if u.HP > 0 {
			kept = append(kept, u)
		}
	}
	if len(kept) != len(g.LiveUnits) {
		changed = true
	}
	g.LiveUnits = kept
	if g.HP[g.Host] <= 0 || g.HP[g.Opponent] <= 0 {
		g.Status = "finished"
		if g.HP[g.Host] > g.HP[g.Opponent] {
			g.Log = append(g.Log, "The host kingdom wins the raid.")
		} else if g.HP[g.Opponent] > g.HP[g.Host] {
			g.Log = append(g.Log, "The challenger kingdom wins the raid.")
		}
		changed = true
	}
	if changed {
		g.Updated = now()
		if len(g.Log) > 40 {
			g.Log = g.Log[len(g.Log)-40:]
		}
	}
	return changed
}

func extraEmpire() {
	api("empirecreate", func(u *User, w http.ResponseWriter, r *http.Request) {
		id := uid()
		code := ""
		for i := 0; i < 6; i++ {
			code += strings.ToUpper(uid()[:1])
		}
		g := &EmpireBattle{ID: id, Code: code, Host: u.ID, Status: "waiting", Players: []string{u.ID}, Turn: 1, HP: map[string]int{u.ID: 100}, Gold: map[string]int{u.ID: 25}, Army: map[string]map[string]int{u.ID: {"spearmen": 3, "swordsmen": 2, "archers": 2, "knights": 0, "guards": 1}}, Buildings: map[string]map[string]int{u.ID: {"town_hall": 1, "barracks": 1, "gold_mine": 1, "cannon": 1, "archer_tower": 1, "wall": 1}}, Pending: map[string]string{}, Created: now(), Updated: now()}
		x11().Battles[id] = g
		save()
		js(w, empireView(g, u.ID))
	})
	api("empirelist", func(u *User, w http.ResponseWriter, r *http.Request) {
		out := []map[string]any{}
		for _, g := range x11().Battles {
			if g.Status == "waiting" && g.Host != u.ID && now()-g.Updated < 3600 {
				out = append(out, map[string]any{"ID": g.ID, "Code": g.Code, "Players": len(g.Players), "Status": g.Status})
			}
		}
		js(w, out)
	})
	api("empirejoin", func(u *User, w http.ResponseWriter, r *http.Request) {
		code := strings.ToUpper(strings.TrimSpace(in(r)["code"]))
		var g *EmpireBattle
		for _, v := range x11().Battles {
			if v.Code == code {
				g = v
				break
			}
		}
		if g == nil {
			fail(w, 404, "Empire battle code was not found. Check the six characters or create a new battle.")
			return
		}
		// Let the creator reopen their own room code without adding themselves twice.
		if g.Host == u.ID {
			js(w, empireView(g, u.ID))
			return
		}
		if g.Status != "waiting" {
			fail(w, 409, "This Empire battle has already started or finished.")
			return
		}
		g.Opponent = u.ID
		g.Players = append(g.Players, u.ID)
		g.HP[u.ID] = 100
		g.Gold[u.ID] = 25
		g.Army[u.ID] = map[string]int{"spearmen": 3, "swordsmen": 2, "archers": 2, "knights": 0, "guards": 1}
		if g.Buildings == nil {
			g.Buildings = map[string]map[string]int{}
		}
		g.Buildings[u.ID] = map[string]int{"town_hall": 1, "barracks": 1, "gold_mine": 1, "cannon": 1, "archer_tower": 1, "wall": 1}
		g.Status = "active"
		g.Updated = now()
		g.Log = append(g.Log, "A rival kingdom has joined. Prepare your armies!")
		save()
		js(w, empireView(g, u.ID))
	})
	api("empirestate", func(u *User, w http.ResponseWriter, r *http.Request) {
		g := x11().Battles[r.URL.Query().Get("id")]
		if !empireMember(g, u.ID) {
			fail(w, 404, "Empire battle not found")
			return
		}
		if empireTick(g) {
			save()
		}
		js(w, empireView(g, u.ID))
	})
	api("empiredeploy", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		g := x11().Battles[b["id"]]
		if !empireMember(g, u.ID) || g.Status != "active" {
			fail(w, 409, "Empire raid is not active")
			return
		}
		if len(g.LiveUnits) >= 60 {
			fail(w, 400, "Battlefield troop limit reached")
			return
		}
		unit := b["unit"]
		costs := map[string]int{"spearmen": 3, "swordsmen": 4, "archers": 3, "knights": 6, "guards": 5}
		if _, ok := costs[unit]; !ok {
			fail(w, 400, "Unknown troop type")
			return
		}
		if g.Army[u.ID][unit] < 1 {
			fail(w, 400, "Recruit this troop before deploying it")
			return
		}
		lane, _ := strconv.Atoi(b["lane"])
		if lane < -1 || lane > 1 {
			lane = 0
		}
		team := 0
		if u.ID == g.Opponent {
			team = 1
		}
		g.Army[u.ID][unit]--
		startX := -10.0
		if team == 1 {
			startX = 10
		}
		hp := 30
		if unit == "knights" || unit == "guards" {
			hp = 48
		}
		if unit == "archers" {
			hp = 22
		}
		g.LiveUnits = append(g.LiveUnits, EmpireLiveUnit{ID: uid(), Owner: u.ID, Team: team, Type: unit, X: startX, Z: float64(lane) * 3, HP: hp, Path: empirePath(team, lane), AttackAt: time.Now().UnixMilli()})
		g.Log = append(g.Log, fmt.Sprintf("%s deployed a %s troop on lane %d.", map[bool]string{true: "Rival", false: "Your"}[team == 1], unit, lane+2))
		g.Updated = now()
		save()
		js(w, empireView(g, u.ID))
	})
	api("empireact", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		g := x11().Battles[b["id"]]
		if !empireMember(g, u.ID) || g.Status != "active" {
			fail(w, 409, "Empire battle is not active")
			return
		}
		if g.Pending[u.ID] != "" {
			fail(w, 409, "Your action is already submitted; waiting for your rival")
			return
		}
		switch b["action"] {
		case "recruit":
			unit := b["unit"]
			costs := map[string]int{"spearmen": 3, "swordsmen": 4, "archers": 3, "knights": 6, "guards": 5}
			cost, ok := costs[unit]
			if !ok {
				fail(w, 400, "Unknown troop type")
				return
			}
			if g.Gold[u.ID] < cost {
				fail(w, 400, "Not enough kingdom gold")
				return
			}
			g.Gold[u.ID] -= cost
			g.Army[u.ID][unit]++
			g.Log = append(g.Log, "A kingdom recruited "+unit+".")
		case "build":
			name := b["building"]
			costs := map[string]int{"barracks": 12, "gold_mine": 10, "cannon": 14, "archer_tower": 14, "wall": 8, "town_hall": 30}
			cost, ok := costs[name]
			if !ok {
				fail(w, 400, "Unknown building")
				return
			}
			if g.Buildings == nil {
				g.Buildings = map[string]map[string]int{}
			}
			if g.Buildings[u.ID] == nil {
				g.Buildings[u.ID] = map[string]int{"town_hall": 1}
			}
			level := g.Buildings[u.ID][name]
			if level >= 5 {
				fail(w, 400, "This building is already at maximum level")
				return
			}
			if name != "town_hall" && level >= g.Buildings[u.ID]["town_hall"]+1 {
				fail(w, 400, "Upgrade your Town Hall first")
				return
			}
			cost = cost * (level + 1)
			if g.Gold[u.ID] < cost {
				fail(w, 400, "Not enough kingdom gold")
				return
			}
			g.Gold[u.ID] -= cost
			g.Buildings[u.ID][name] = level + 1
			g.Log = append(g.Log, fmt.Sprintf("A kingdom upgraded %s to level %d.", strings.ReplaceAll(name, "_", " "), level+1))
		case "attack":
			t := b["tactic"]
			if t != "balanced" && t != "defend" && t != "flank" {
				fail(w, 400, "Choose a valid battle tactic")
				return
			}
			g.Pending[u.ID] = t
			g.Log = append(g.Log, "A kingdom submitted its battle tactic.")
			empireResolve(g)
		default:
			fail(w, 400, "Unknown Empire action")
			return
		}
		g.Updated = now()
		save()
		js(w, empireView(g, u.ID))
	})
	// Game marketplace endpoints. The browser catalog is display-only; prices and ownership are enforced here.
	api("gamestore", func(u *User, w http.ResponseWriter, r *http.Request) {
		game := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("game")))
		prices := map[string]map[string]int{
			"streetracer": {"starter_gt": 0, "silver_gt": 180, "neon_hyper": 320, "crimson_v12": 520, "phantom_xr": 680, "volt_apex": 760, "aurora_gt": 820, "titan_rs": 880, "eclipse_s": 940, "inferno_x": 1020, "nebula_r": 1100, "monarch_gt": 1180, "raptor_9": 1260, "tempest_v": 1340, "onyx_black": 1420, "solar_flare": 1500, "arctic_wolf": 1580, "scarlet_gt": 1660, "blue_streak": 1740, "copperhead": 1820, "phantom_e": 1900, "zenith_r": 1980, "drift_king": 2060, "obsidian_x": 2140, "sapphire_v": 2220, "rose_gold": 2300, "venom_gt": 2380, "storm_chaser": 2460, "dragon_r": 2540, "apex_one": 2800},
			"lostisland":  {"palm_camp": 120, "explorer_pack": 180, "azure_pickaxe": 220, "reef_boat": 300},
			"neonrunner":  {"cyber_suit": 140, "plasma_trail": 180, "pulse_board": 240, "holo_runner": 320},
			"skyparkour":  {"cloud_glider": 120, "sky_sneakers": 160, "aurora_trail": 200, "wing_cape": 280},
			"empire":      {"royal_banner": 120, "stone_keep_skin": 240, "golden_armour": 300, "dragon_standard": 420},
		}
		list, ok := prices[game]
		if !ok {
			fail(w, 400, "Unknown game store")
			return
		}
		owned := x11().StoreOwned[u.ID]
		if owned == nil {
			owned = map[string]bool{}
			x11().StoreOwned[u.ID] = owned
		}
		if game == "streetracer" {
			for _, id := range x11().Cars[u.ID] {
				owned[id] = true
			}
			if !owned["starter_gt"] {
				owned["starter_gt"] = true
				x11().Cars[u.ID] = append(x11().Cars[u.ID], "starter_gt")
			}
		}
		items := []map[string]any{}
		for id, price := range list {
			items = append(items, map[string]any{"id": id, "price": price})
		}
		sort.Slice(items, func(i, j int) bool { return items[i]["id"].(string) < items[j]["id"].(string) })
		js(w, map[string]any{"Game": game, "Coins": gpf(u.ID).Coins, "Owned": owned, "Items": items})
	})
	api("gamebuy", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		game := strings.ToLower(strings.TrimSpace(b["game"]))
		id := strings.TrimSpace(b["id"])
		prices := map[string]map[string]int{
			"streetracer": {"starter_gt": 0, "silver_gt": 180, "neon_hyper": 320, "crimson_v12": 520, "phantom_xr": 680, "volt_apex": 760, "aurora_gt": 820, "titan_rs": 880, "eclipse_s": 940, "inferno_x": 1020, "nebula_r": 1100, "monarch_gt": 1180, "raptor_9": 1260, "tempest_v": 1340, "onyx_black": 1420, "solar_flare": 1500, "arctic_wolf": 1580, "scarlet_gt": 1660, "blue_streak": 1740, "copperhead": 1820, "phantom_e": 1900, "zenith_r": 1980, "drift_king": 2060, "obsidian_x": 2140, "sapphire_v": 2220, "rose_gold": 2300, "venom_gt": 2380, "storm_chaser": 2460, "dragon_r": 2540, "apex_one": 2800},
			"lostisland":  {"palm_camp": 120, "explorer_pack": 180, "azure_pickaxe": 220, "reef_boat": 300},
			"neonrunner":  {"cyber_suit": 140, "plasma_trail": 180, "pulse_board": 240, "holo_runner": 320},
			"skyparkour":  {"cloud_glider": 120, "sky_sneakers": 160, "aurora_trail": 200, "wing_cape": 280},
			"empire":      {"royal_banner": 120, "stone_keep_skin": 240, "golden_armour": 300, "dragon_standard": 420},
		}
		price, ok := prices[game][id]
		if !ok {
			fail(w, 400, "Unknown store item")
			return
		}
		e := x11()
		if e.StoreOwned[u.ID] == nil {
			e.StoreOwned[u.ID] = map[string]bool{}
		}
		if e.StoreOwned[u.ID][id] {
			fail(w, 409, "You already own this item")
			return
		}
		if price > 0 && !spendCoins(u, gpf(u.ID), price) {
			fail(w, 400, "Not enough Ran Coin")
			return
		}
		e.StoreOwned[u.ID][id] = true
		if game == "streetracer" {
			e.Cars[u.ID] = append(e.Cars[u.ID], id)
		}
		if price > 0 {
			ledger(u.ID, spentAmt(u, price), "game-store", "Unlocked "+id+" in "+game)
		}
		save()
		js(w, map[string]any{"ok": true, "Owned": e.StoreOwned[u.ID], "Coins": gpf(u.ID).Coins})
	})
	// Cars and boosters are bought with earned Ran Coins; no external payment is processed here.
	api("raceuse", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		id := b["id"]
		if id != "nitro_small" && id != "nitro_large" {
			fail(w, 400, "Unknown booster")
			return
		}
		inv := x11().Inventory[u.ID]
		if inv == nil || inv[id] < 1 {
			fail(w, 400, "You do not own this booster")
			return
		}
		inv[id]--
		save()
		js(w, map[string]any{"Inventory": inv})
	})
	api("racegarage", func(u *User, w http.ResponseWriter, r *http.Request) {
		owned := x11().Cars[u.ID]
		if !has(owned, "starter_gt") {
			owned = append(owned, "starter_gt")
			x11().Cars[u.ID] = owned
		}
		js(w, map[string]any{"Cars": owned, "Inventory": x11().Inventory[u.ID], "Coins": gpf(u.ID).Coins})
	})
	api("racebuy", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		id := b["id"]
		prices := map[string]int{"silver_gt": 180, "neon_hyper": 320, "crimson_v12": 520, "nitro_small": 45, "nitro_large": 110}
		price, ok := prices[id]
		if !ok {
			fail(w, 400, "Unknown garage item")
			return
		}
		if id == "nitro_small" || id == "nitro_large" {
			inv := x11().Inventory[u.ID]
			if inv == nil {
				inv = map[string]int{}
				x11().Inventory[u.ID] = inv
			}
			if !spendCoins(u, gpf(u.ID), price) {
				fail(w, 400, "Not enough Ran Coin")
				return
			}
			inv[id]++
			ledger(u.ID, spentAmt(u, price), "race", "Bought race booster")
			save()
			js(w, map[string]any{"Inventory": inv, "Coins": gpf(u.ID).Coins})
			return
		}
		cars := x11().Cars[u.ID]
		if has(cars, id) {
			fail(w, 400, "You already own this car")
			return
		}
		if !spendCoins(u, gpf(u.ID), price) {
			fail(w, 400, "Not enough Ran Coin")
			return
		}
		x11().Cars[u.ID] = append(cars, id)
		ledger(u.ID, spentAmt(u, price), "race", "Bought race car")
		save()
		js(w, map[string]any{"Cars": x11().Cars[u.ID], "Coins": gpf(u.ID).Coins})
	})
}
func init() { extraEmpire() }

// ---------- Lost Island shared persistent world (voice chat intentionally deferred) ----------
type IslandPlayer struct {
	User    string  `json:"user"`
	X       float64 `json:"x"`
	Z       float64 `json:"z"`
	Updated int64   `json:"updated"`
	Online  bool    `json:"online"`
}
type IslandBuild struct {
	ID       string  `json:"id"`
	Owner    string  `json:"owner"`
	Kind     string  `json:"kind"`
	X        float64 `json:"x"`
	Z        float64 `json:"z"`
	Rotation int     `json:"rotation"`
	Name     string  `json:"name"`
	Created  int64   `json:"created"`
}
type IslandWorld struct {
	ID         string                    `json:"id"`
	Code       string                    `json:"code"`
	Name       string                    `json:"name"`
	Owner      string                    `json:"owner"`
	Private    bool                      `json:"private"`
	Players    map[string]*IslandPlayer  `json:"players"`
	Builds     []IslandBuild             `json:"builds"`
	Resources  map[string]map[string]int `json:"resources"`
	Reputation map[string]int            `json:"reputation"`
	Created    int64                     `json:"created"`
	Updated    int64                     `json:"updated"`
}
type Ext12 struct {
	Worlds map[string]*IslandWorld `json:"islandWorlds"`
}

func x12() *Ext12 {
	if S.X12.Worlds == nil {
		S.X12.Worlds = map[string]*IslandWorld{}
	}
	return &S.X12
}
func islandWorldView(w *IslandWorld, me string) map[string]any {
	ps := []map[string]any{}
	for id, p := range w.Players {
		if now()-p.Updated > 35 {
			p.Online = false
		}
		if p.Online && S.Users[id] != nil {
			ps = append(ps, map[string]any{"U": uv(id), "X": p.X, "Z": p.Z, "Owner": id == w.Owner})
		}
	}
	sort.Slice(ps, func(i, j int) bool {
		return ps[i]["U"].(map[string]any)["Username"].(string) < ps[j]["U"].(map[string]any)["Username"].(string)
	})
	res := w.Resources[me]
	if res == nil {
		res = map[string]int{"wood": 30, "stone": 20, "food": 15, "crystal": 0}
		w.Resources[me] = res
	}
	return map[string]any{"ID": w.ID, "Code": w.Code, "Name": w.Name, "Owner": w.Owner, "Private": w.Private, "Players": ps, "Builds": w.Builds, "Resources": res, "Reputation": w.Reputation[me], "MaxPlayers": 30}
}
func islandOnlineCount(w *IslandWorld) int {
	n := 0
	for _, p := range w.Players {
		if p.Online && now()-p.Updated < 35 {
			n++
		}
	}
	return n
}
func islandFind(code string) *IslandWorld {
	for _, w := range x12().Worlds {
		if strings.EqualFold(w.Code, code) || w.ID == code {
			return w
		}
	}
	return nil
}
func init() { extraLostIsland() }
func extraLostIsland() {
	api("islandlist", func(u *User, w http.ResponseWriter, r *http.Request) {
		out := []map[string]any{}
		for _, v := range x12().Worlds {
			if !v.Private && now()-v.Updated < 86400 {
				out = append(out, map[string]any{"ID": v.ID, "Code": v.Code, "Name": v.Name, "Players": islandOnlineCount(v), "Owner": uv(v.Owner)})
			}
		}
		sort.Slice(out, func(i, j int) bool { return out[i]["Players"].(int) > out[j]["Players"].(int) })
		if len(out) > 30 {
			out = out[:30]
		}
		js(w, out)
	})
	api("islandcreate", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		name := strings.TrimSpace(b["name"])
		if name == "" {
			name = "" + u.Username + "'s Island"
		}
		if len([]rune(name)) > 32 {
			name = string([]rune(name)[:32])
		}
		priv := b["private"] == "true"
		code := strings.ToUpper(uid()[:6])
		id := uid()
		world := &IslandWorld{ID: id, Code: code, Name: name, Owner: u.ID, Private: priv, Players: map[string]*IslandPlayer{}, Resources: map[string]map[string]int{}, Reputation: map[string]int{}, Created: now(), Updated: now()}
		world.Players[u.ID] = &IslandPlayer{User: u.ID, Updated: now(), Online: true}
		world.Resources[u.ID] = map[string]int{"wood": 50, "stone": 35, "food": 25, "crystal": 0}
		x12().Worlds[id] = world
		save()
		js(w, islandWorldView(world, u.ID))
	})
	api("islandjoin", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		world := islandFind(strings.TrimSpace(b["code"]))
		if world == nil || (world.Private && world.Owner != u.ID && !strings.EqualFold(world.Code, b["code"])) {
			fail(w, 404, "Island not found or private invite code is invalid")
			return
		}
		online := 0
		for _, p := range world.Players {
			if p.Online && now()-p.Updated < 35 {
				online++
			}
		}
		if world.Players[u.ID] == nil && online >= 30 {
			fail(w, 409, "This island is full. Try another island.")
			return
		}
		if world.Players[u.ID] == nil {
			world.Players[u.ID] = &IslandPlayer{User: u.ID, Updated: now(), Online: true}
		}
		if world.Resources[u.ID] == nil {
			world.Resources[u.ID] = map[string]int{"wood": 30, "stone": 20, "food": 15, "crystal": 0}
		}
		world.Players[u.ID].Online = true
		world.Players[u.ID].Updated = now()
		world.Updated = now()
		save()
		js(w, islandWorldView(world, u.ID))
	})
	api("islandstate", func(u *User, w http.ResponseWriter, r *http.Request) {
		world := islandFind(r.URL.Query().Get("id"))
		if world == nil || world.Players[u.ID] == nil {
			fail(w, 404, "Island session not found")
			return
		}
		p := world.Players[u.ID]
		p.Online = true
		p.Updated = now()
		world.Updated = now()
		js(w, islandWorldView(world, u.ID))
	})
	api("islandmove", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		world := islandFind(b["id"])
		if world == nil || world.Players[u.ID] == nil {
			fail(w, 404, "Island session not found")
			return
		}
		x, _ := strconv.ParseFloat(b["x"], 64)
		z, _ := strconv.ParseFloat(b["z"], 64)
		if math.IsNaN(x) || math.IsInf(x, 0) || math.IsNaN(z) || math.IsInf(z, 0) {
			fail(w, 400, "Invalid position")
			return
		}
		if x < -48 {
			x = -48
		}
		if x > 48 {
			x = 48
		}
		if z < -48 {
			z = -48
		}
		if z > 48 {
			z = 48
		}
		p := world.Players[u.ID]
		p.X = x
		p.Z = z
		p.Updated = now()
		p.Online = true
		world.Updated = now()
		js(w, map[string]bool{"ok": true})
	})
	api("islandgather", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		world := islandFind(b["id"])
		if world == nil || world.Players[u.ID] == nil {
			fail(w, 404, "Island session not found")
			return
		}
		kind := b["kind"]
		if kind != "wood" && kind != "stone" && kind != "food" && kind != "crystal" {
			fail(w, 400, "Unknown resource")
			return
		}
		res := world.Resources[u.ID]
		if res == nil {
			res = map[string]int{"wood": 30, "stone": 20, "food": 15, "crystal": 0}
			world.Resources[u.ID] = res
		}
		gain := 5
		if kind == "crystal" {
			gain = 1
		}
		if res[kind] >= 9999 {
			fail(w, 400, "Your storage is full")
			return
		}
		res[kind] += gain
		world.Reputation[u.ID]++
		world.Updated = now()
		save()
		js(w, islandWorldView(world, u.ID))
	})
	api("islandbuild", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		world := islandFind(b["id"])
		if world == nil || world.Players[u.ID] == nil {
			fail(w, 404, "Island session not found")
			return
		}
		kind := b["kind"]
		costs := map[string]map[string]int{"cabin": {"wood": 20, "stone": 5}, "house": {"wood": 35, "stone": 20}, "farm": {"wood": 15, "stone": 5}, "bridge": {"wood": 25, "stone": 10}, "dock": {"wood": 30, "stone": 15}, "village": {"wood": 60, "stone": 45}, "decoration": {"wood": 5, "stone": 2}}
		cost, ok := costs[kind]
		if !ok {
			fail(w, 400, "Unknown building")
			return
		}
		if len(world.Builds) >= 500 {
			fail(w, 409, "This island has reached its building limit")
			return
		}
		res := world.Resources[u.ID]
		for k, n := range cost {
			if res[k] < n {
				fail(w, 400, "Not enough resources: collect more "+k)
				return
			}
		}
		x, _ := strconv.ParseFloat(b["x"], 64)
		z, _ := strconv.ParseFloat(b["z"], 64)
		if x < -45 {
			x = -45
		}
		if x > 45 {
			x = 45
		}
		if z < -45 {
			z = -45
		}
		if z > 45 {
			z = 45
		}
		for _, v := range world.Builds {
			if math.Hypot(v.X-x, v.Z-z) < 2.8 {
				fail(w, 409, "That spot is too close to another building")
				return
			}
		}
		for k, n := range cost {
			res[k] -= n
		}
		world.Builds = append(world.Builds, IslandBuild{ID: uid(), Owner: u.ID, Kind: kind, X: x, Z: z, Name: cut(kind), Created: now()})
		world.Reputation[u.ID] += 5
		world.Updated = now()
		save()
		js(w, islandWorldView(world, u.ID))
	})
	api("islandleave", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		world := islandFind(b["id"])
		if world != nil {
			if p := world.Players[u.ID]; p != nil {
				p.Online = false
				p.Updated = now()
			}
			world.Updated = now()
			save()
		}
		js(w, map[string]bool{"ok": true})
	})
}
