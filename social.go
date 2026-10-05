package main

import (
	"net/http"
	"strings"
	"time"
)

func registerSocial(mux *http.ServeMux) {
	// Repost (toggle): puts a copy of someone's post on your own profile, credited to the original author.
	mux.HandleFunc("POST /api/posts/{id}/repost", func(w http.ResponseWriter, r *http.Request) {
		me := who(r)
		var b struct{ Text string }
		readJSON(r, &b)
		mu.Lock()
		defer mu.Unlock()
		if me == "" {
			fail(w, 401, "Log in first.")
			return
		}
		var o *Post
		for _, p := range db.Posts {
			if p.ID == r.PathValue("id") {
				o = p
			}
		}
		if o == nil {
			fail(w, 404, "That post was not found.")
			return
		}
		origID, origUser, src := o.ID, o.User, o
		if o.Via == "repost" {
			origID, origUser = o.Orig, o.From
			for _, p := range db.Posts {
				if p.ID == origID {
					src = p
				}
			}
		}
		if au := db.Users[origUser]; au == nil || au.Banned || origUser == me || blocked(me, origUser) {
			fail(w, 400, "You can't repost this.")
			return
		}
		for i, p := range db.Posts {
			if p.User == me && p.Via == "repost" && p.Orig == origID {
				db.Posts = append(db.Posts[:i], db.Posts[i+1:]...)
				save()
				out(w, map[string]bool{"reposted": false})
				return
			}
		}
		t := []rune(strings.TrimSpace(b.Text))
		if len(t) > 300 {
			t = t[:300]
		}
		db.Posts = append(db.Posts, &Post{ID: rnd(6), User: me, Text: string(t), Type: src.Type, Src: src.Src, T: time.Now().UnixMilli(),
			Likes: []string{}, Comments: []Comment{}, Via: "repost", From: origUser, Orig: origID})
		notify(origUser, me, "repost", origID)
		save()
		out(w, map[string]bool{"reposted": true})
	})

	// Log out of every device.
	mux.HandleFunc("POST /api/logout-all", func(w http.ResponseWriter, r *http.Request) {
		me := who(r)
		mu.Lock()
		defer mu.Unlock()
		if me == "" {
			fail(w, 401, "Log in first.")
			return
		}
		for k, v := range db.Sessions {
			if v == me {
				delete(db.Sessions, k)
			}
		}
		save()
		out(w, "ok")
	})

	// Download your own data.
	mux.HandleFunc("GET /api/me/export", func(w http.ResponseWriter, r *http.Request) {
		me := who(r)
		mu.Lock()
		defer mu.Unlock()
		u := db.Users[me]
		if u == nil {
			fail(w, 401, "Log in first.")
			return
		}
		mine := []*Post{}
		for _, p := range db.Posts {
			if p.User == me {
				mine = append(mine, p)
			}
		}
		w.Header().Set("Content-Disposition", "attachment; filename=amlink-my-data.json")
		out(w, map[string]any{"profile": pubUser(u), "joined": u.Created, "posts": mine, "following": db.Follows[me], "blocked": db.Blocks[me]})
	})
}
