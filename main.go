package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

type User struct {
	Name  string `json:"name"`
	Email string `json:"email"`
	Salt  string `json:"salt"`
	Hash  string `json:"hash"`
	Pic   string `json:"pic"`
	Bio   string `json:"bio"`
	Hide  bool   `json:"hide"`
	DOB     string `json:"dob"`
	Gender  string `json:"gender"`
	Pending bool   `json:"pending"`
	FullName string `json:"fullName"`
	EmailOK  bool   `json:"emailOk"`
	Badge    bool   `json:"badge"`
	Banned   bool   `json:"banned"`
	Created  int64  `json:"created"`
}
type Comment struct {
	U string `json:"u"`
	T string `json:"t"`
}
type Post struct {
	ID       string    `json:"id"`
	User     string    `json:"user"`
	Text     string    `json:"text"`
	Type     string    `json:"type"`
	Src      string    `json:"src"`
	T        int64     `json:"t"`
	Likes    []string  `json:"likes"`
	Comments []Comment `json:"comments"`
}
type Msg struct {
	ID      string            `json:"id"`
	From    string            `json:"from"`
	To      string            `json:"to"`
	T       string            `json:"t"`
	At      int64             `json:"at"`
	RID     string            `json:"rid"`
	RT      string            `json:"rt"`
	RF      string            `json:"rf"`
	Read    bool              `json:"read"`
	Deleted bool              `json:"del"`
	Reacts  map[string]string `json:"reacts"`
	Src     string            `json:"src"`
	Kind    string            `json:"kind"`
}
type Notif struct {
	ID   string `json:"id"`
	To   string `json:"to"`
	From string `json:"from"`
	Type string `json:"type"`
	Post string `json:"post"`
	At   int64  `json:"at"`
	Read bool   `json:"read"`
}
type DB struct {
	Users    map[string]*User  `json:"users"`
	Posts    []*Post           `json:"posts"`
	Msgs     []*Msg            `json:"msgs"`
	Sessions map[string]string `json:"sessions"`
	Follows  map[string][]string `json:"follows"`
	Notifs   []*Notif `json:"notifs"`
	Blocks   map[string][]string `json:"blocks"`
	Reports  []*Report `json:"reports"`
	Announce string `json:"announce"`
	AnnounceID string `json:"announceId"`
}

var (
	db      = &DB{Users: map[string]*User{}, Sessions: map[string]string{}}
	mu      sync.Mutex
	seen    = map[string]int64{}
	nameRe  = regexp.MustCompile(`^[a-z0-9_.]{3,20}$`)
	kinds   = map[string]string{"image/jpeg": ".jpg", "image/png": ".png", "image/gif": ".gif", "image/webp": ".webp", "audio/ogg": ".ogg", "audio/mpeg": ".mp3", "audio/wave": ".wav", "video/mp4": ".mp4", "video/webm": ".webm"}
)

func pubUser(u *User) map[string]any {
	return map[string]any{"name": u.Name, "email": u.Email, "pic": u.Pic, "bio": u.Bio, "hide": u.Hide, "dob": u.DOB, "gender": u.Gender, "fullName": u.FullName, "badge": u.Badge, "admin": isAdminUser(u)}
}
// notify must be called with mu held.
func notify(to, from, typ, post string) {
	if to == "" || to == from {
		return
	}
	if typ == "like" {
		for _, n := range db.Notifs {
			if n.To == to && n.From == from && n.Type == "like" && n.Post == post && !n.Read {
				return
			}
		}
	}
	db.Notifs = append(db.Notifs, &Notif{rnd(5), to, from, typ, post, time.Now().UnixMilli(), false})
	if len(db.Notifs) > 1000 {
		db.Notifs = db.Notifs[len(db.Notifs)-1000:]
	}
}
func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

var dataDir = envOr("DATA_DIR", ".")
var uploadDir = filepath.Join(dataDir, "uploads")

// save queues a snapshot of everything to be written (Postgres or data.json). Call with mu held.
func save() {
	b, err := json.Marshal(db)
	if err != nil {
		log.Println("save:", err)
		return
	}
	enqueue(b)
}
func rnd(n int) string { b := make([]byte, n); rand.Read(b); return hex.EncodeToString(b) }
func hash(p, salt string) string {
	h := []byte(salt + p)
	for i := 0; i < 50000; i++ {
		s := sha256.Sum256(h)
		h = s[:]
	}
	return hex.EncodeToString(h)
}
func out(w http.ResponseWriter, v any) { w.Header().Set("Content-Type", "application/json"); json.NewEncoder(w).Encode(v) }
func fail(w http.ResponseWriter, code int, m string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": m})
}
func who(r *http.Request) string {
	c, err := r.Cookie("sid")
	if err != nil {
		return ""
	}
	mu.Lock()
	defer mu.Unlock()
	u := db.Sessions[c.Value]
	if x := db.Users[u]; x == nil || x.Banned {
		return ""
	}
	if u != "" {
		seen[u] = time.Now().Unix()
	}
	return u
}
func login(w http.ResponseWriter, u string) {
	sid := rnd(24)
	db.Sessions[sid] = u
	http.SetCookie(w, &http.Cookie{Name: "sid", Value: sid, Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode, MaxAge: 30 * 86400})
}

func main() {
	loadState()
	for _, m := range db.Msgs {
		if m.ID == "" {
			m.ID, m.Read = rnd(5), true
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) { http.ServeFile(w, r, "index.html") })
	mux.Handle("GET /uploads/", http.StripPrefix("/uploads/", http.FileServer(http.Dir(uploadDir))))

	mux.HandleFunc("POST /api/logout", func(w http.ResponseWriter, r *http.Request) {
		if c, err := r.Cookie("sid"); err == nil {
			mu.Lock()
			delete(db.Sessions, c.Value)
			save()
			mu.Unlock()
		}
		out(w, "ok")
	})
	mux.HandleFunc("GET /api/me", func(w http.ResponseWriter, r *http.Request) {
		me := who(r)
		mu.Lock()
		defer mu.Unlock()
		if db.Users[me] == nil {
			fail(w, 401, "Not logged in.")
			return
		}
		out(w, pubUser(db.Users[me]))
	})
	mux.HandleFunc("POST /api/me", func(w http.ResponseWriter, r *http.Request) {
		me := who(r)
		if me == "" {
			fail(w, 401, "Log in first.")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 12<<20)
		r.ParseMultipartForm(12 << 20)
		pic, kind, err := saveUpload(r, "pic")
		if err != nil || (pic != "" && kind != "image") {
			fail(w, 400, "Profile photo must be an image.")
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if pic != "" {
			db.Users[me].Pic = pic
		}
		if _, ok := r.MultipartForm.Value["bio"]; ok {
			b := r.FormValue("bio")
			if len(b) > 150 {
				b = b[:150]
			}
			db.Users[me].Bio = b
		}
		if _, ok := r.MultipartForm.Value["fullName"]; ok {
			if f := []rune(strings.TrimSpace(r.FormValue("fullName"))); len(f) <= 40 {
				db.Users[me].FullName = string(f)
			}
		}
		if _, ok := r.MultipartForm.Value["gender"]; ok {
			if g := r.FormValue("gender"); g == "" || g == "female" || g == "male" || g == "other" || g == "none" {
				db.Users[me].Gender = g
			}
		}
		if d := r.FormValue("dob"); d != "" && okDOB(d) {
			db.Users[me].DOB = d
		}
		save()
		out(w, pubUser(db.Users[me]))
	})
	mux.HandleFunc("DELETE /api/me", func(w http.ResponseWriter, r *http.Request) {
		me := who(r)
		mu.Lock()
		defer mu.Unlock()
		keep := db.Posts[:0]
		for _, p := range db.Posts {
			if p.User != me {
				keep = append(keep, p)
			}
		}
		db.Posts = keep
		delete(db.Users, me)
		delete(db.Follows, me)
		for k, v := range db.Sessions {
			if v == me {
				delete(db.Sessions, k)
			}
		}
		save()
		out(w, "ok")
	})
	mux.HandleFunc("GET /api/users", func(w http.ResponseWriter, r *http.Request) {
		me := who(r)
		if me == "" {
			fail(w, 401, "Log in first.")
			return
		}
		mu.Lock()
		defer mu.Unlock()
		now := time.Now().Unix()
		fol, cnt, mine := map[string]int{}, map[string]int{}, map[string]bool{}
		for _, l := range db.Follows {
			for _, t := range l {
				fol[t]++
			}
		}
		for _, p := range db.Posts {
			cnt[p.User]++
		}
		for _, t := range db.Follows[me] {
			mine[t] = true
		}
		fm := map[string]bool{}
		for k, l := range db.Follows {
			for _, t := range l {
				if t == me {
					fm[k] = true
				}
			}
		}
		pub := map[string]map[string]any{}
		for k, u := range db.Users {
			if u.Pending || u.Banned || (k != me && blocked(me, k)) {
				continue
			}
			pub[k] = map[string]any{"fullName": u.FullName, "badge": u.Badge, "pic": u.Pic, "bio": u.Bio, "hide": u.Hide, "online": k == me || (!u.Hide && now-seen[k] < 20),
				"followers": fol[k], "followingCount": len(db.Follows[k]), "posts": cnt[k], "isFollowing": mine[k], "followsMe": fm[k]}
		}
		out(w, pub)
	})
	mux.HandleFunc("GET /api/posts", func(w http.ResponseWriter, r *http.Request) {
		me := who(r)
		if me == "" {
			fail(w, 401, "Log in first.")
			return
		}
		mu.Lock()
		defer mu.Unlock()
		res := []*Post{}
		for i := len(db.Posts) - 1; i >= 0; i-- {
			if a := db.Users[db.Posts[i].User]; a == nil || a.Banned || a.Pending || (db.Posts[i].User != me && blocked(me, db.Posts[i].User)) {
				continue
			}
			res = append(res, db.Posts[i])
		}
		out(w, res)
	})
	mux.HandleFunc("POST /api/posts", func(w http.ResponseWriter, r *http.Request) {
		me := who(r)
		if me == "" {
			fail(w, 401, "Log in first.")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 60<<20)
		if r.ParseMultipartForm(8<<20) != nil {
			fail(w, 400, "Upload too large (60 MB max).")
			return
		}
		src, kind, err := saveUpload(r, "media")
		if err != nil {
			fail(w, 400, "Only JPG, PNG, GIF, WebP, MP4 and WebM files are allowed.")
			return
		}
		if kind == "audio" {
			fail(w, 400, "Only photos and videos can be posted.")
			return
		}
		text := strings.TrimSpace(r.FormValue("text"))
		if text == "" && src == "" {
			fail(w, 400, "Write something or add a photo.")
			return
		}
		if len(text) > 1000 {
			text = text[:1000]
		}
		if kind == "" {
			kind = "text"
		}
		mu.Lock()
		defer mu.Unlock()
		p := &Post{ID: rnd(6), User: me, Text: text, Type: kind, Src: src, T: time.Now().UnixMilli(), Likes: []string{}, Comments: []Comment{}}
		db.Posts = append(db.Posts, p)
		save()
		out(w, p)
	})
	find := func(id string) *Post {
		for _, p := range db.Posts {
			if p.ID == id {
				return p
			}
		}
		return nil
	}
	mux.HandleFunc("POST /api/posts/{id}/like", func(w http.ResponseWriter, r *http.Request) {
		me := who(r)
		mu.Lock()
		defer mu.Unlock()
		p := find(r.PathValue("id"))
		if me == "" || p == nil {
			fail(w, 404, "Not found.")
			return
		}
		for i, u := range p.Likes {
			if u == me {
				p.Likes = append(p.Likes[:i], p.Likes[i+1:]...)
				save()
				out(w, p)
				return
			}
		}
		p.Likes = append(p.Likes, me)
		notify(p.User, me, "like", p.ID)
		save()
		out(w, p)
	})
	mux.HandleFunc("POST /api/posts/{id}/comment", func(w http.ResponseWriter, r *http.Request) {
		me := who(r)
		var b struct{ Text string }
		json.NewDecoder(io.LimitReader(r.Body, 1<<14)).Decode(&b)
		mu.Lock()
		defer mu.Unlock()
		p := find(r.PathValue("id"))
		if me == "" || p == nil || strings.TrimSpace(b.Text) == "" {
			fail(w, 400, "Could not add comment.")
			return
		}
		p.Comments = append(p.Comments, Comment{me, strings.TrimSpace(b.Text)})
		notify(p.User, me, "comment", p.ID)
		save()
		out(w, p)
	})
	mux.HandleFunc("DELETE /api/posts/{id}", func(w http.ResponseWriter, r *http.Request) {
		me := who(r)
		mu.Lock()
		defer mu.Unlock()
		for i, p := range db.Posts {
			if p.ID == r.PathValue("id") && p.User == me {
				db.Posts = append(db.Posts[:i], db.Posts[i+1:]...)
				save()
				out(w, "ok")
				return
			}
		}
		fail(w, 404, "Not found.")
	})
	mux.HandleFunc("GET /api/messages", func(w http.ResponseWriter, r *http.Request) {
		me, with := who(r), r.URL.Query().Get("with")
		if me == "" {
			fail(w, 401, "Log in first.")
			return
		}
		mu.Lock()
		defer mu.Unlock()
		res, changed := []*Msg{}, false
		for _, m := range db.Msgs {
			if (m.From == me && m.To == with) || (m.From == with && m.To == me) {
				if m.To == me && !m.Read {
					m.Read, changed = true, true
				}
				res = append(res, m)
			}
		}
		if changed {
			save()
		}
		if len(res) > 300 {
			res = res[len(res)-300:]
		}
		out(w, res)
	})
	mux.HandleFunc("POST /api/messages", func(w http.ResponseWriter, r *http.Request) {
		me := who(r)
		var b struct{ To, Text, Reply string }
		json.NewDecoder(io.LimitReader(r.Body, 1<<15)).Decode(&b)
		text := strings.TrimSpace(b.Text)
		mu.Lock()
		defer mu.Unlock()
		if me == "" || db.Users[b.To] == nil || text == "" || len(text) > 4000 || blocked(me, b.To) {
			fail(w, 400, "Could not send message.")
			return
		}
		m := &Msg{ID: rnd(5), From: me, To: b.To, T: text, At: time.Now().UnixMilli()}
		linkReply(m, b.Reply)
		db.Msgs = append(db.Msgs, m)
		save()
		out(w, m)
	})
	mux.HandleFunc("POST /api/messages/{id}/react", func(w http.ResponseWriter, r *http.Request) {
		me := who(r)
		var b struct{ Emoji string }
		json.NewDecoder(io.LimitReader(r.Body, 1<<10)).Decode(&b)
		mu.Lock()
		defer mu.Unlock()
		for _, m := range db.Msgs {
			if m.ID == r.PathValue("id") && me != "" && (m.From == me || m.To == me) && !m.Deleted {
				if m.Reacts == nil {
					m.Reacts = map[string]string{}
				}
				if b.Emoji == "" || m.Reacts[me] == b.Emoji {
					delete(m.Reacts, me)
				} else if len([]rune(b.Emoji)) <= 4 {
					m.Reacts[me] = b.Emoji
				}
				save()
				out(w, "ok")
				return
			}
		}
		fail(w, 404, "Not found.")
	})
	mux.HandleFunc("DELETE /api/messages/{id}", func(w http.ResponseWriter, r *http.Request) {
		me := who(r)
		mu.Lock()
		defer mu.Unlock()
		for _, m := range db.Msgs {
			if m.ID == r.PathValue("id") && me != "" && m.From == me {
				m.Deleted, m.T, m.RT, m.Reacts, m.Src, m.Kind = true, "", "", nil, "", ""
				save()
				out(w, "ok")
				return
			}
		}
		fail(w, 404, "Not found.")
	})
	mux.HandleFunc("GET /api/unread", func(w http.ResponseWriter, r *http.Request) {
		me := who(r)
		if me == "" {
			fail(w, 401, "Log in first.")
			return
		}
		mu.Lock()
		defer mu.Unlock()
		res := map[string]map[string]any{}
		for _, m := range db.Msgs {
			if m.To == me && !m.Read && !m.Deleted {
				e := res[m.From]
				if e == nil {
					e = map[string]any{"n": 0, "last": ""}
					res[m.From] = e
				}
				e["n"], e["last"] = e["n"].(int)+1, msgLabel(m)
			}
		}
		out(w, res)
	})
	mux.HandleFunc("POST /api/password", func(w http.ResponseWriter, r *http.Request) {
		me := who(r)
		var b struct{ Old, New string }
		json.NewDecoder(io.LimitReader(r.Body, 1<<12)).Decode(&b)
		mu.Lock()
		defer mu.Unlock()
		u := db.Users[me]
		if u == nil || u.Hash != hash(b.Old, u.Salt) {
			fail(w, 400, "Your current password is wrong.")
			return
		}
		if len(b.New) < 6 {
			fail(w, 400, "New password needs at least 6 characters.")
			return
		}
		u.Salt = rnd(8)
		u.Hash = hash(b.New, u.Salt)
		save()
		out(w, "ok")
	})
	mux.HandleFunc("POST /api/follow/{u}", func(w http.ResponseWriter, r *http.Request) {
		me, t := who(r), r.PathValue("u")
		mu.Lock()
		defer mu.Unlock()
		if me == "" || t == me || db.Users[t] == nil || blocked(me, t) {
			fail(w, 400, "Cannot follow that user.")
			return
		}
		l := db.Follows[me]
		for i, x := range l {
			if x == t {
				db.Follows[me] = append(l[:i], l[i+1:]...)
				save()
				out(w, "ok")
				return
			}
		}
		db.Follows[me] = append(l, t)
		notify(t, me, "follow", "")
		save()
		out(w, "ok")
	})
	mux.HandleFunc("POST /api/settings", func(w http.ResponseWriter, r *http.Request) {
		me := who(r)
		var b struct{ Hide bool }
		json.NewDecoder(io.LimitReader(r.Body, 1<<12)).Decode(&b)
		mu.Lock()
		defer mu.Unlock()
		if me == "" {
			fail(w, 401, "Log in first.")
			return
		}
		db.Users[me].Hide = b.Hide
		save()
		out(w, "ok")
	})

	mux.HandleFunc("GET /api/notifications", func(w http.ResponseWriter, r *http.Request) {
		me := who(r)
		if me == "" {
			fail(w, 401, "Log in first.")
			return
		}
		mu.Lock()
		defer mu.Unlock()
		res := []*Notif{}
		for i := len(db.Notifs) - 1; i >= 0 && len(res) < 50; i-- {
			if db.Notifs[i].To == me {
				res = append(res, db.Notifs[i])
			}
		}
		out(w, res)
	})
	mux.HandleFunc("POST /api/notifications/read", func(w http.ResponseWriter, r *http.Request) {
		me := who(r)
		mu.Lock()
		defer mu.Unlock()
		for _, n := range db.Notifs {
			if n.To == me {
				n.Read = true
			}
		}
		save()
		out(w, "ok")
	})

	registerAuth(mux)
	registerAdmin(mux)
	registerDM(mux)

	addr := ":8080"
	if p := os.Getenv("PORT"); p != "" {
		addr = ":" + p
	}
	log.Println("Amlink running on http://localhost" + addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
