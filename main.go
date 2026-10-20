package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
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
	Avatar, Cover string
	Blocked []string
}
type Comment struct {
	User, Text string
	TS         int64
	Likes      []string
}
type Post struct {
	ID, User, Text, Media, Kind string
	TS                          int64
	Likes                       []string
	Comments                    []Comment
}
type Msg struct {
	From, To, Text, Kind, Media string
	TS                          int64
	Read bool
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
	SessTS map[string]int64
	Log []string
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
	return http.DefaultClient.Do(r)
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
	resp, e := http.DefaultClient.Do(r)
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
		if rows, e := neon("SELECT value::text AS v FROM store WHERE key='state'"); e == nil && len(rows) > 0 {
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

func save() { // call while holding mu; only the newest state is written
	b, _ := json.Marshal(S)
	select {
	case <-saveCh:
	default:
	}
	saveCh <- b
}
func writer() {
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
	S.Notifs = append(S.Notifs, Notif{to, from, typ, post, now(), false})
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
	return map[string]any{"ID": u.ID, "Username": u.Username, "Verified": u.Verified, "On": now()-u.Seen < 120 && !u.Hide, "Seen": seen, "Av": u.Avatar}
}
func pv(p *Post) map[string]any {
	cs := []map[string]any{}
	for _, c := range p.Comments {
		cs = append(cs, map[string]any{"U": uv(c.User), "Text": c.Text, "TS": c.TS, "Likes": c.Likes})
	}
	return map[string]any{"ID": p.ID, "U": uv(p.User), "Text": p.Text, "Media": p.Media, "Kind": p.Kind, "TS": p.TS, "Likes": p.Likes, "C": cs}
}
func fp(id string) *Post {
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
func okMedia(m string) bool { return m == "" || strings.HasPrefix(m, env("SUPABASE_URL")+"/storage/") }
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
	go writer()
	admin := env("ADMIN_EMAIL")
	if admin == "" {
		admin = "whitefrostff@gmail.com"
	}
	ok := map[string]bool{"ok": true}

	// removes a user and everything that points at them
	purge := func(id string) {
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
		resp, e := http.Get("https://oauth2.googleapis.com/tokeninfo?id_token=" + url.QueryEscape(in(r)["credential"]))
		if e != nil {
			fail(w, 502, "Google check failed")
			return
		}
		defer resp.Body.Close()
		var t struct{ Aud, Email, Name, Sub string }
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
		}
		if t.Email == admin {
			u.Admin, u.Verified = true, true
		}
		tok := uid() + uid()
		S.Sess[tok] = t.Sub
		S.SessTS[tok] = now()
		save()
		js(w, map[string]any{"token": tok, "me": u})
	})

	// Email sign-in: Firebase checks the password, we check the Firebase token.
	http.HandleFunc("/api/fblogin", func(w http.ResponseWriter, r *http.Request) {
		b := in(r)
		body, _ := json.Marshal(map[string]string{"idToken": b["token"]})
		resp, e := http.Post("https://identitytoolkit.googleapis.com/v1/accounts:lookup?key="+url.QueryEscape(env("FIREBASE_API_KEY")), "application/json", bytes.NewReader(body))
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
			if n == "" || len(n) > 60 || !old13(b["dob"]) || b["terms"] != "1" {
				fail(w, 400, "Finish creating your account first")
				return
			}
			u = &User{ID: f.LocalId, Email: f.Email, Name: n, DOB: b["dob"], Agreed: now(), Following: []string{}}
			S.Users[u.ID] = u
		}
		if f.EmailVerified && f.Email == admin {
			u.Admin, u.Verified = true, true
		}
		tok := uid() + uid()
		S.Sess[tok] = u.ID
		S.SessTS[tok] = now()
		save()
		js(w, map[string]any{"token": tok, "me": u})
	})

	http.HandleFunc("/api/upload", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		u := S.Users[S.Sess[r.Header.Get("X-Token")]]
		mu.Unlock()
		ct := r.Header.Get("Content-Type")
		if u == nil {
			fail(w, 401, "Please log in")
			return
		}
		ct = strings.Split(ct, ";")[0]
		if !strings.HasPrefix(ct, "image/") && !strings.HasPrefix(ct, "video/") && !strings.HasPrefix(ct, "audio/") {
			fail(w, 400, "Only images and videos")
			return
		}
		name := uid() + "." + strings.Split(ct, "/")[1]
		resp, e := sb("POST", "/storage/v1/object/media/"+name, http.MaxBytesReader(w, r.Body, 50<<20), ct)
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
		if n < 60 || n > 300 {
			n = 60
		}
		for i := len(S.Posts) - 1; i >= 0 && len(out) < n; i-- {
			if o := S.Users[S.Posts[i].User]; o != nil && blk(u, o) {
				continue
			}
			out = append(out, pv(S.Posts[i]))
		}
		js(w, out)
	})

	api("post", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		if strings.TrimSpace(b["text"]) == "" && b["media"] == "" {
			fail(w, 400, "Write something or add a photo or video")
			return
		}
		if len(b["text"]) > 2000 || !okMedia(b["media"]) {
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

	api("comment", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		p, t := fp(b["id"]), strings.TrimSpace(b["text"])
		if p == nil || t == "" || len(t) > 300 {
			fail(w, 400, "Invalid comment")
			return
		}
		p.Comments = append(p.Comments, Comment{User: u.ID, Text: t, TS: now()})
		note(p.User, u.ID, "comment", p.ID)
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

	api("follow", func(u *User, w http.ResponseWriter, r *http.Request) {
		id := in(r)["id"]
		if id == u.ID || S.Users[id] == nil {
			fail(w, 400, "Invalid user")
			return
		}
		if has(u.Following, id) {
			u.Following = del(u.Following, id)
		} else {
			u.Following = append(u.Following, id)
			note(id, u.ID, "follow", "")
		}
		save()
		js(w, u)
	})

	api("search", func(u *User, w http.ResponseWriter, r *http.Request) {
		q := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
		out := []map[string]any{}
		for _, o := range S.Users {
			if o.ID == u.ID || o.Username == "" || blk(u, o) {
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
		if S.Users[b["to"]] == nil || blk(u, S.Users[b["to"]]) || (t == "" && b["media"] == "") || len(t) > 1000 || !okMedia(b["media"]) {
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
		if !u.Admin || len(t) > 200 {
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
				if p.User != u.ID && !u.Admin {
					fail(w, 403, "Not allowed")
					return
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
			why = why[:200]
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
		fol, lk, ps := 0, 0, []map[string]any{}
		for _, x := range S.Users {
			if has(x.Following, id) {
				fol++
			}
		}
		for i := len(S.Posts) - 1; i >= 0; i-- {
			if p := S.Posts[i]; p.User == id {
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
		if len(b) > 120 {
			fail(w, 400, "Bio is too long (120 characters max)")
			return
		}
		if n := strings.TrimSpace(bd["name"]); n != "" && len(n) <= 60 {
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
		if (strings.TrimSpace(b["text"]) == "" && b["media"] == "") || len(b["text"]) > 300 || !okMedia(b["media"]) {
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
		if n == "" || len(n) > 40 || len(m) < 2 {
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
		if g == nil || !has(g.Members, u.ID) || t == "" || len(t) > 1000 {
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
		if n == "" || len(n) > 40 || len(b["desc"]) > 200 {
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
		if c == nil || c.Owner != u.ID || t == "" || len(t) > 500 {
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
		if p == nil {
			fail(w, 404, "Post not found")
			return
		}
		js(w, pv(p))
	})
	api("editpost", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		p, t := fp(b["id"]), strings.TrimSpace(b["text"])
		if p == nil || p.User != u.ID || len(t) > 2000 || (t == "" && p.Media == "") {
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
			if o := S.Users[p.User]; o != nil && blk(u, o) {
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
	http.ListenAndServe(":"+port, nil)
}
