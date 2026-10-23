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
	if has(x().Mute[to], from) {
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
	return map[string]any{"ID": u.ID, "Username": u.Username, "Verified": u.Verified, "On": now()-u.Seen < 120 && !u.Hide, "Seen": seen, "Av": u.Avatar, "Priv": x().Priv[u.ID], "Pin": x().Pin[u.ID]}
}
func pv(p *Post) map[string]any {
	cs := []map[string]any{}
	for _, c := range p.Comments {
		cs = append(cs, map[string]any{"ID": c.ID, "Parent": c.Parent, "U": uv(c.User), "Text": c.Text, "TS": c.TS, "Likes": c.Likes})
	}
	return map[string]any{"ID": p.ID, "U": uv(p.User), "Text": p.Text, "Media": p.Media, "Kind": p.Kind, "TS": p.TS, "Likes": p.Likes, "C": cs, "More": p.More, "Poll": p.Poll, "Sens": x().Sens[p.ID], "Rep": repOf(p.ID), "Views": x2().Views[p.ID], "PinC": x2().PinC[p.ID], "Comm": p.Comm}
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
	if len(S.Log) > 200 {
		S.Log = S.Log[100:]
	}
}

func main() {
	load()
	fillIDs(true)
	fillCIDs()
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
			if S.Posts[i].Comm != "" {
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
			m["Banned"], m["Email"], m["Admin"] = o.Banned, o.Email, o.Admin
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
		js(w, map[string]any{"Users": len(S.Users), "Posts": len(S.Posts), "Msgs": len(S.Msgs), "L": us, "Online": on, "Banned": bn, "Verified": vr, "Reels": rv, "Groups": len(S.Groups), "Channels": len(S.Channels), "Moments": len(S.Statuses), "PL": pl, "R": rp, "Notice": S.Notice, "Log": S.Log})
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
		http.ServeFile(w, r, "index.html")
	})

	port := env("PORT")
	if port == "" {
		port = "8080"
	}
	(&http.Server{Addr: ":" + port, Handler: modMW(http.DefaultServeMux), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute}).ListenAndServe()
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
	webPushTo(to, ev)
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
	"/sw.js": {"text/javascript", swJS},
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
		id := S.Sess[r.URL.Query().Get("t")]
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
		ppl := []map[string]any{}
		for i := 0; i < len(l) && i < 8; i++ {
			ppl = append(ppl, uv(l[i].id))
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
		mu.Lock()
		b, _ := json.Marshal(S)
		mu.Unlock()
		flush(b)
	}
}
func flush(b []byte) {
	flushParts()
}

// fp finds a post through an index that is rebuilt whenever the state changes
func fp(id string) *Post {
	if pver != saveVer || len(pidx) != len(S.Posts) {
		pidx = make(map[string]*Post, len(S.Posts))
		for _, p := range S.Posts {
			pidx[p.ID] = p
		}
		pver = saveVer
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
	e := x()
	var files []string
	if u := S.Users[id]; u != nil {
		files = append(files, u.Avatar, u.Cover)
	}
	for _, p := range S.Posts {
		if p.User == id {
			files = append(files, p.Media)
			files = append(files, p.More...)
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
			if p.Comm != "" || (vid && p.Kind != "video") || hid(u, p.User) {
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
		return strings.TrimSpace(strings.Split(f, ",")[0])
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
		if !u.Admin {
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
		if !u.Admin {
			fail(w, 403, "Admins only")
			return
		}
		b := in(r)
		id, op := b["id"], b["op"]
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
			}
		}
		if !found {
			fail(w, 404, "Report not found")
			return
		}
		t, e := S.Users[target], x()
		switch op {
		case "dismiss":
		case "delete":
			if cidx >= 0 {
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
	Views map[string]int
	Rep   map[string]string   // repost id -> original post id
	MW    map[string][]string // muted words per user
	Hid   map[string][]string // "not interested" post ids per user
	Tags  map[string][]string // followed hashtags per user
	Push  map[string][]string // web push endpoints per user
	PinC  map[string]string   // post id -> pinned comment id
	VReq  map[string]*VRq     // verification requests by user id
	Comms []*Comm             // communities
	Disap map[string]int64    // disappearing-message timer per chat (seconds)
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
				fs = append(fs, s.Media)
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

	// views (batched by the browser) shown to the post owner
	api("pview", func(u *User, w http.ResponseWriter, r *http.Request) {
		for i, id := range strings.Split(in(r)["ids"], ",") {
			if i >= 30 {
				break
			}
			if p := fp(id); p != nil && p.User != u.ID && !hid(u, p.User) {
				x2().Views[id]++
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
	ID, Name, Desc, Rules, Owner string
	TS                           int64
	Mods, Members, Banned        []string
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
		if rn(n) < 3 || rn(n) > 40 || rn(d) > 200 || rn(ru) > 500 {
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
		c := &Comm{ID: uid(), Name: n, Desc: d, Rules: ru, Owner: u.ID, TS: now(), Members: []string{u.ID}}
		S.X2.Comms = append(S.X2.Comms, c)
		save()
		js(w, map[string]string{"id": c.ID})
	})
	api("comms", func(u *User, w http.ResponseWriter, r *http.Request) {
		q := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
		list := []*Comm{}
		for _, c := range S.X2.Comms {
			if q == "" || strings.Contains(strings.ToLower(c.Name), q) || strings.Contains(strings.ToLower(c.Desc), q) {
				list = append(list, c)
			}
		}
		sort.Slice(list, func(a, b int) bool { return len(list[a].Members) > len(list[b].Members) })
		out := []map[string]any{}
		for i := 0; i < len(list) && i < 60; i++ {
			c := list[i]
			out = append(out, map[string]any{"ID": c.ID, "Name": c.Name, "Desc": c.Desc, "N": len(c.Members), "Joined": has(c.Members, u.ID)})
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
		for i := len(S.Posts) - 1; i >= 0 && len(ps) < 60; i-- {
			if p := S.Posts[i]; p.Comm == c.ID && !hid(u, p.User) {
				ps = append(ps, pv(p))
			}
		}
		mod := c.Owner == u.ID || has(c.Mods, u.ID)
		res := map[string]any{"Name": c.Name, "Desc": c.Desc, "Rules": c.Rules, "N": len(c.Members), "Joined": has(c.Members, u.ID), "Owner": c.Owner == u.ID, "Mod": mod, "Posts": ps}
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
			if !mod || rn(d) > 200 || rn(ru) > 500 {
				fail(w, 403, "Not allowed")
				return
			}
			c.Desc, c.Rules = d, ru
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
			if len(c.Banned) < 200 {
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
