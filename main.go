package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
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
type State struct {
	Users    map[string]*User
	Posts    []*Post
	Msgs     []Msg
	Notifs   []Notif
	Sess     map[string]string
	Groups   []*Group
	Statuses []*Status
	Channels []*Channel
}

var (
	S   = State{Users: map[string]*User{}, Sess: map[string]string{}}
	mu  sync.Mutex
	env = os.Getenv
)

// ---------- storage: Supabase (table "store", bucket "media") or local state.json ----------
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

func load() {
	if env("SUPABASE_URL") == "" {
		b, _ := os.ReadFile("state.json")
		json.Unmarshal(b, &S)
	} else if r, e := sb("GET", "/rest/v1/store?key=eq.state&select=value", nil, ""); e == nil {
		defer r.Body.Close()
		var a []struct{ Value State }
		json.NewDecoder(r.Body).Decode(&a)
		if len(a) > 0 {
			S = a[0].Value
		}
	}
	if S.Users == nil {
		S.Users = map[string]*User{}
	}
	if S.Sess == nil {
		S.Sess = map[string]string{}
	}
}

func save() { // call while holding mu
	b, _ := json.Marshal(S)
	go func() {
		if env("SUPABASE_URL") == "" {
			os.WriteFile("state.json", b, 0644)
			return
		}
		v, _ := json.Marshal([]map[string]any{{"key": "state", "value": json.RawMessage(b)}})
		if r, e := sb("POST", "/rest/v1/store", bytes.NewReader(v), "application/json"); e == nil {
			r.Body.Close()
		}
	}()
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
	return map[string]any{"ID": u.ID, "Username": u.Username, "Verified": u.Verified, "On": now()-u.Seen < 120 && !u.Hide}
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
		if u.Banned {
			fail(w, 403, "This account is banned")
			return
		}
		u.Seen = now()
		f(u, w, r)
	})
}

func main() {
	load()
	admin := env("ADMIN_EMAIL")
	if admin == "" {
		admin = "whitefrostff@gmail.com"
	}

	http.HandleFunc("/api/config", func(w http.ResponseWriter, r *http.Request) {
		js(w, map[string]string{"Client": env("GOOGLE_CLIENT_ID")})
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
		for i := len(S.Posts) - 1; i >= 0 && len(out) < 60; i-- {
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
		if len(b["text"]) > 2000 || (b["media"] != "" && !strings.HasPrefix(b["media"], env("SUPABASE_URL")+"/storage/")) {
			fail(w, 400, "Invalid post")
			return
		}
		k := b["kind"]
		if k != "image" && k != "video" {
			k = ""
		}
		S.Posts = append(S.Posts, &Post{ID: uid(), User: u.ID, Text: b["text"], Media: b["media"], Kind: k, TS: now(), Likes: []string{}})
		save()
		js(w, map[string]bool{"ok": true})
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
			if o.ID == u.ID || o.Username == "" {
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
		out := []Msg{}
		for _, m := range S.Msgs {
			if (m.From == u.ID && m.To == p) || (m.From == p && m.To == u.ID) {
				out = append(out, m)
			}
		}
		js(w, out)
	})

	api("msg", func(u *User, w http.ResponseWriter, r *http.Request) {
		b := in(r)
		t := strings.TrimSpace(b["text"])
		if S.Users[b["to"]] == nil || (t == "" && b["media"] == "") || len(t) > 1000 || (b["media"] != "" && !strings.HasPrefix(b["media"], env("SUPABASE_URL")+"/storage/")) {
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
		js(w, map[string]bool{"ok": true})
	})

	api("verify", func(u *User, w http.ResponseWriter, r *http.Request) {
		o := S.Users[in(r)["id"]]
		if !u.Admin || o == nil {
			fail(w, 403, "Admins only")
			return
		}
		o.Verified = !o.Verified
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

	api("admin", func(u *User, w http.ResponseWriter, r *http.Request) {
		if !u.Admin {
			fail(w, 403, "Admins only")
			return
		}
		us := []map[string]any{}
		for _, o := range S.Users {
			m := uv(o.ID)
			m["Banned"], m["Email"] = o.Banned, o.Email
			us = append(us, m)
		}
		js(w, map[string]any{"Users": len(S.Users), "Posts": len(S.Posts), "Msgs": len(S.Msgs), "L": us})
	})

	api("ban", func(u *User, w http.ResponseWriter, r *http.Request) {
		o := S.Users[in(r)["id"]]
		if !u.Admin || o == nil || o.Admin {
			fail(w, 403, "Not allowed")
			return
		}
		o.Banned = !o.Banned
		save()
		js(w, map[string]bool{"Banned": o.Banned})
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
				js(w, map[string]bool{"ok": true})
				return
			}
		}
		fail(w, 404, "Post not found")
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
		js(w, map[string]bool{"ok": true})
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
		js(w, map[string]bool{"ok": true})
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
		js(w, map[string]bool{"ok": true})
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
		js(w, map[string]bool{"ok": true})
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
		js(w, map[string]bool{"ok": true})
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
