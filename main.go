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
	From string `json:"from"`
	To   string `json:"to"`
	T    string `json:"t"`
	At   int64  `json:"at"`
}
type DB struct {
	Users    map[string]*User  `json:"users"`
	Posts    []*Post           `json:"posts"`
	Msgs     []*Msg            `json:"msgs"`
	Sessions map[string]string `json:"sessions"`
}

var (
	db      = &DB{Users: map[string]*User{}, Sessions: map[string]string{}}
	mu      sync.Mutex
	nameRe  = regexp.MustCompile(`^[a-z0-9_.]{3,20}$`)
	kinds   = map[string]string{"image/jpeg": ".jpg", "image/png": ".png", "image/gif": ".gif", "image/webp": ".webp", "video/mp4": ".mp4", "video/webm": ".webm"}
)

func save() { b, _ := json.Marshal(db); os.WriteFile("data.json", b, 0o600) }
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
	return db.Sessions[c.Value]
}
func login(w http.ResponseWriter, u string) {
	sid := rnd(24)
	db.Sessions[sid] = u
	http.SetCookie(w, &http.Cookie{Name: "sid", Value: sid, Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode, MaxAge: 30 * 86400})
}

// saveUpload stores an uploaded image or video and returns its URL and kind.
func saveUpload(r *http.Request, field string) (string, string, error) {
	f, _, err := r.FormFile(field)
	if err != nil {
		return "", "", nil
	}
	defer f.Close()
	head := make([]byte, 512)
	n, _ := f.Read(head)
	ct := http.DetectContentType(head[:n])
	ext, ok := kinds[ct]
	if !ok {
		return "", "", os.ErrInvalid
	}
	os.MkdirAll("uploads", 0o755)
	name := rnd(12) + ext
	dst, err := os.Create(filepath.Join("uploads", name))
	if err != nil {
		return "", "", err
	}
	defer dst.Close()
	dst.Write(head[:n])
	io.Copy(dst, f)
	return "/uploads/" + name, strings.SplitN(ct, "/", 2)[0], nil
}

func main() {
	if b, err := os.ReadFile("data.json"); err == nil {
		json.Unmarshal(b, db)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) { http.ServeFile(w, r, "index.html") })
	mux.Handle("GET /uploads/", http.StripPrefix("/uploads/", http.FileServer(http.Dir("uploads"))))

	mux.HandleFunc("POST /api/signup", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 12<<20)
		if r.ParseMultipartForm(12<<20) != nil {
			fail(w, 400, "Upload too large.")
			return
		}
		u, e := strings.ToLower(strings.TrimSpace(r.FormValue("username"))), strings.TrimSpace(r.FormValue("email"))
		if !nameRe.MatchString(u) || !strings.Contains(e, "@") || len(r.FormValue("password")) < 6 {
			fail(w, 400, "Check your username, email and password.")
			return
		}
		if r.FormValue("terms") != "1" {
			fail(w, 400, "Accept the Terms and Privacy Policy to continue.")
			return
		}
		pic, kind, err := saveUpload(r, "pic")
		if err != nil || (pic != "" && kind != "image") {
			fail(w, 400, "Profile photo must be an image.")
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if db.Users[u] != nil {
			fail(w, 409, "That username is taken.")
			return
		}
		salt := rnd(8)
		db.Users[u] = &User{Name: u, Email: e, Salt: salt, Hash: hash(r.FormValue("password"), salt), Pic: pic}
		login(w, u)
		save()
		out(w, db.Users[u])
	})
	mux.HandleFunc("POST /api/login", func(w http.ResponseWriter, r *http.Request) {
		var b struct{ Username, Password string }
		json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&b)
		mu.Lock()
		defer mu.Unlock()
		u := db.Users[strings.ToLower(b.Username)]
		if u == nil || u.Hash != hash(b.Password, u.Salt) {
			fail(w, 401, "Wrong username or password.")
			return
		}
		login(w, u.Name)
		save()
		out(w, u)
	})
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
		out(w, db.Users[me])
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
		save()
		out(w, db.Users[me])
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
		for k, v := range db.Sessions {
			if v == me {
				delete(db.Sessions, k)
			}
		}
		save()
		out(w, "ok")
	})
	mux.HandleFunc("GET /api/users", func(w http.ResponseWriter, r *http.Request) {
		if who(r) == "" {
			fail(w, 401, "Log in first.")
			return
		}
		mu.Lock()
		defer mu.Unlock()
		pub := map[string]map[string]string{}
		for k, u := range db.Users {
			pub[k] = map[string]string{"pic": u.Pic, "bio": u.Bio}
		}
		out(w, pub)
	})
	mux.HandleFunc("GET /api/posts", func(w http.ResponseWriter, r *http.Request) {
		if who(r) == "" {
			fail(w, 401, "Log in first.")
			return
		}
		mu.Lock()
		defer mu.Unlock()
		res := []*Post{}
		for i := len(db.Posts) - 1; i >= 0; i-- {
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
		res := []*Msg{}
		for _, m := range db.Msgs {
			if (m.From == me && m.To == with) || (m.From == with && m.To == me) {
				res = append(res, m)
			}
		}
		out(w, res)
	})
	mux.HandleFunc("POST /api/messages", func(w http.ResponseWriter, r *http.Request) {
		me := who(r)
		var b struct{ To, Text string }
		json.NewDecoder(io.LimitReader(r.Body, 1<<14)).Decode(&b)
		mu.Lock()
		defer mu.Unlock()
		if me == "" || db.Users[b.To] == nil || strings.TrimSpace(b.Text) == "" {
			fail(w, 400, "Could not send message.")
			return
		}
		db.Msgs = append(db.Msgs, &Msg{me, b.To, strings.TrimSpace(b.Text), time.Now().UnixMilli()})
		save()
		out(w, "ok")
	})

	addr := ":8080"
	if p := os.Getenv("PORT"); p != "" {
		addr = ":" + p
	}
	log.Println("Amlink running on http://localhost" + addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
