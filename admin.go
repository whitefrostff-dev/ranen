package main

import (
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Report struct {
	ID     string `json:"id"`
	By     string `json:"by"`
	Type   string `json:"type"`
	Target string `json:"target"`
	Reason string `json:"reason"`
	At     int64  `json:"at"`
	Status string `json:"status"`
}

// isAdminUser: an admin is an account whose VERIFIED email is listed in ADMIN_EMAILS.
func isAdminUser(u *User) bool {
	if u == nil || !u.EmailOK || u.Pending || u.Banned {
		return false
	}
	for _, e := range strings.Split(envOr("ADMIN_EMAILS", "whitefrostff@gmail.com"), ",") {
		if strings.EqualFold(strings.TrimSpace(e), u.Email) {
			return true
		}
	}
	return false
}

func has(l []string, x string) bool {
	for _, v := range l {
		if v == x {
			return true
		}
	}
	return false
}
func without(l []string, x string) []string {
	r := []string{}
	for _, v := range l {
		if v != x {
			r = append(r, v)
		}
	}
	return r
}

// blocked needs mu held. True if either user blocked the other.
func blocked(a, b string) bool { return has(db.Blocks[a], b) || has(db.Blocks[b], a) }

func adminOK(w http.ResponseWriter, r *http.Request) bool {
	me := who(r)
	mu.Lock()
	ok := isAdminUser(db.Users[me])
	mu.Unlock()
	if !ok {
		fail(w, 403, "Admins only.")
	}
	return ok
}

// setBan needs mu held.
func setBan(name string, ban bool) bool {
	u := db.Users[name]
	if u == nil || isAdminUser(u) {
		return false
	}
	u.Banned = ban
	if ban {
		for k, v := range db.Sessions {
			if v == name {
				delete(db.Sessions, k)
			}
		}
	}
	return true
}

// purgeUser needs mu held.
func purgeUser(name string) {
	keep := db.Posts[:0]
	for _, p := range db.Posts {
		if p.User != name {
			keep = append(keep, p)
		}
	}
	db.Posts = keep
	delete(db.Users, name)
	delete(db.Follows, name)
	delete(db.Blocks, name)
	for k, l := range db.Follows {
		db.Follows[k] = without(l, name)
	}
	for k, v := range db.Sessions {
		if v == name {
			delete(db.Sessions, k)
		}
	}
}

/* ---------- accurate search ---------- */

func lev(a, b string) int {
	ra, rb := []rune(a), []rune(b)
	prev := make([]int, len(rb)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(ra); i++ {
		cur := make([]int, len(rb)+1)
		cur[0] = i
		for j := 1; j <= len(rb); j++ {
			c := 1
			if ra[i-1] == rb[j-1] {
				c = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+c)
		}
		prev = cur
	}
	return prev[len(rb)]
}

// matchScore ranks how well query q (lowercase) matches a user. 0 means no match.
func matchScore(q string, u *User) int {
	n, f, b := strings.ToLower(u.Name), strings.ToLower(u.FullName), strings.ToLower(u.Bio)
	best := 0
	up := func(v int) {
		if v > best {
			best = v
		}
	}
	if n == q {
		up(100)
	}
	if strings.HasPrefix(n, q) {
		up(90 - min(len(n)-len(q), 20))
	}
	words := strings.Fields(f)
	if f != "" {
		if f == q {
			up(95)
		}
		if strings.HasPrefix(f, q) {
			up(85)
		}
		for _, w := range words {
			if strings.HasPrefix(w, q) {
				up(75)
			}
		}
		if strings.Contains(f, q) {
			up(55)
		}
	}
	if strings.Contains(n, q) {
		up(60)
	}
	if len([]rune(q)) >= 3 { // typo tolerance
		allow := 1
		if len(q) > 5 {
			allow = 2
		}
		if d := lev(n, q); d <= allow {
			up(40 - 5*d)
		}
		if len(n) > len(q) {
			if d := lev(n[:len(q)], q); d <= 1 {
				up(48 - 6*d)
			}
		}
		for _, w := range words {
			if d := lev(w, q); d <= allow {
				up(38 - 5*d)
			}
		}
		if strings.Contains(b, q) {
			up(20)
		}
	}
	return best
}

func followerSets() map[string]map[string]bool {
	m := map[string]map[string]bool{}
	for f, l := range db.Follows {
		for _, t := range l {
			if m[t] == nil {
				m[t] = map[string]bool{}
			}
			m[t][f] = true
		}
	}
	return m
}

func mutualCount(me, c string, fol map[string]map[string]bool) int {
	n := 0
	for _, x := range db.Follows[me] {
		if fol[c][x] {
			n++
		}
	}
	return n
}

func personView(me string, u *User, fol map[string]map[string]bool, mut int) map[string]any {
	reason := ""
	if mut == 1 {
		reason = "1 mutual friend"
	} else if mut > 1 {
		reason = strconv.Itoa(mut) + " mutual friends"
	} else if has(db.Follows[u.Name], me) {
		reason = "Follows you"
	}
	return map[string]any{"name": u.Name, "fullName": u.FullName, "pic": u.Pic, "bio": u.Bio, "badge": u.Badge,
		"online": !u.Hide && time.Now().Unix()-seen[u.Name] < 20, "isFollowing": has(db.Follows[me], u.Name),
		"followers": len(fol[u.Name]), "mutual": mut, "reason": reason}
}

type hit struct {
	s int
	p map[string]any
}

func sortHits(h []hit, limit int) []map[string]any {
	sort.Slice(h, func(i, j int) bool {
		if h[i].s != h[j].s {
			return h[i].s > h[j].s
		}
		return h[i].p["name"].(string) < h[j].p["name"].(string)
	})
	res := []map[string]any{}
	for i := 0; i < len(h) && i < limit; i++ {
		res = append(res, h[i].p)
	}
	return res
}

func registerAdmin(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/search", func(w http.ResponseWriter, r *http.Request) {
		me := who(r)
		if me == "" {
			fail(w, 401, "Log in first.")
			return
		}
		q := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(r.URL.Query().Get("q")), "@")))
		if q == "" || len(q) > 60 {
			out(w, []int{})
			return
		}
		mu.Lock()
		defer mu.Unlock()
		fol := followerSets()
		hits := []hit{}
		for k, u := range db.Users {
			if k == me || u.Pending || u.Banned || blocked(me, k) {
				continue
			}
			s := matchScore(q, u)
			if s == 0 {
				continue
			}
			mut := mutualCount(me, k, fol)
			hits = append(hits, hit{s*10 + min(mut, 5)*2 + min(len(fol[k]), 20)/4, personView(me, u, fol, mut)})
		}
		out(w, sortHits(hits, 30))
	})

	mux.HandleFunc("GET /api/suggestions", func(w http.ResponseWriter, r *http.Request) {
		me := who(r)
		if me == "" {
			fail(w, 401, "Log in first.")
			return
		}
		mu.Lock()
		defer mu.Unlock()
		fol := followerSets()
		now := time.Now().Unix()
		hits := []hit{}
		for k, u := range db.Users {
			if k == me || u.Pending || u.Banned || blocked(me, k) || has(db.Follows[me], k) {
				continue
			}
			mut := mutualCount(me, k, fol)
			s := mut*10 + min(len(fol[k]), 20)/2
			if has(db.Follows[k], me) {
				s += 15
			}
			if now-seen[k] < 600 {
				s += 3
			}
			hits = append(hits, hit{s, personView(me, u, fol, mut)})
		}
		out(w, sortHits(hits, 10))
	})

	mux.HandleFunc("POST /api/report", func(w http.ResponseWriter, r *http.Request) {
		me := who(r)
		var b struct{ Type, Target, Reason string }
		readJSON(r, &b)
		if rr := []rune(strings.TrimSpace(b.Reason)); len(rr) > 300 {
			b.Reason = string(rr[:300])
		} else {
			b.Reason = string(rr)
		}
		mu.Lock()
		defer mu.Unlock()
		valid := false
		switch b.Type {
		case "post":
			for _, p := range db.Posts {
				if p.ID == b.Target && p.User != me {
					valid = true
				}
			}
		case "user":
			valid = db.Users[b.Target] != nil && b.Target != me
		}
		if me == "" || !valid || b.Reason == "" {
			fail(w, 400, "Could not send the report.")
			return
		}
		for _, x := range db.Reports {
			if x.Status == "open" && x.By == me && x.Type == b.Type && x.Target == b.Target {
				out(w, "ok")
				return
			}
		}
		db.Reports = append(db.Reports, &Report{rnd(5), me, b.Type, b.Target, b.Reason, time.Now().UnixMilli(), "open"})
		save()
		out(w, "ok")
	})

	mux.HandleFunc("POST /api/block/{u}", func(w http.ResponseWriter, r *http.Request) {
		me, t := who(r), r.PathValue("u")
		mu.Lock()
		defer mu.Unlock()
		if me == "" || t == me || db.Users[t] == nil {
			fail(w, 400, "Cannot block that user.")
			return
		}
		if has(db.Blocks[me], t) {
			db.Blocks[me] = without(db.Blocks[me], t)
			save()
			out(w, "unblocked")
			return
		}
		db.Blocks[me] = append(db.Blocks[me], t)
		db.Follows[me] = without(db.Follows[me], t)
		db.Follows[t] = without(db.Follows[t], me)
		save()
		out(w, "blocked")
	})
	mux.HandleFunc("GET /api/blocks", func(w http.ResponseWriter, r *http.Request) {
		me := who(r)
		mu.Lock()
		defer mu.Unlock()
		out(w, append([]string{}, db.Blocks[me]...))
	})

	/* ---------- admin ---------- */
	mux.HandleFunc("GET /api/admin/stats", func(w http.ResponseWriter, r *http.Request) {
		if !adminOK(w, r) {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		now, week := time.Now().Unix(), time.Now().AddDate(0, 0, -7).Unix()
		s := map[string]any{"posts": len(db.Posts), "messages": len(db.Msgs), "announce": db.Announce}
		users, pending, banned, online, new7, open := 0, 0, 0, 0, 0, 0
		for k, u := range db.Users {
			if u.Pending {
				pending++
				continue
			}
			users++
			if u.Banned {
				banned++
			}
			if u.Created > week {
				new7++
			}
			if now-seen[k] < 20 {
				online++
			}
		}
		for _, x := range db.Reports {
			if x.Status == "open" {
				open++
			}
		}
		s["users"], s["pending"], s["banned"], s["online"], s["new7"], s["reports"] = users, pending, banned, online, new7, open
		out(w, s)
	})

	mux.HandleFunc("GET /api/admin/users", func(w http.ResponseWriter, r *http.Request) {
		if !adminOK(w, r) {
			return
		}
		q := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
		mu.Lock()
		defer mu.Unlock()
		cnt := map[string]int{}
		for _, p := range db.Posts {
			cnt[p.User]++
		}
		list := []*User{}
		for _, u := range db.Users {
			if q == "" || strings.Contains(strings.ToLower(u.Name+" "+u.Email+" "+u.FullName), q) {
				list = append(list, u)
			}
		}
		sort.Slice(list, func(i, j int) bool {
			if list[i].Created != list[j].Created {
				return list[i].Created > list[j].Created
			}
			return list[i].Name < list[j].Name
		})
		res := []map[string]any{}
		for i := 0; i < len(list) && i < 100; i++ {
			u := list[i]
			res = append(res, map[string]any{"name": u.Name, "email": u.Email, "fullName": u.FullName, "created": u.Created, "pending": u.Pending,
				"banned": u.Banned, "badge": u.Badge, "emailOk": u.EmailOK, "posts": cnt[u.Name], "admin": isAdminUser(u)})
		}
		out(w, res)
	})
	mux.HandleFunc("POST /api/admin/users/{name}/ban", func(w http.ResponseWriter, r *http.Request) {
		if !adminOK(w, r) {
			return
		}
		var b struct{ Banned bool }
		readJSON(r, &b)
		mu.Lock()
		defer mu.Unlock()
		if !setBan(r.PathValue("name"), b.Banned) {
			fail(w, 400, "Cannot change that account (not found, or it is an admin).")
			return
		}
		save()
		out(w, "ok")
	})
	mux.HandleFunc("POST /api/admin/users/{name}/badge", func(w http.ResponseWriter, r *http.Request) {
		if !adminOK(w, r) {
			return
		}
		var b struct{ Badge bool }
		readJSON(r, &b)
		mu.Lock()
		defer mu.Unlock()
		u := db.Users[r.PathValue("name")]
		if u == nil {
			fail(w, 404, "Not found.")
			return
		}
		u.Badge = b.Badge
		save()
		out(w, "ok")
	})
	mux.HandleFunc("DELETE /api/admin/users/{name}", func(w http.ResponseWriter, r *http.Request) {
		if !adminOK(w, r) {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		u := db.Users[r.PathValue("name")]
		if u == nil || isAdminUser(u) {
			fail(w, 400, "Cannot delete that account (not found, or it is an admin).")
			return
		}
		purgeUser(u.Name)
		save()
		out(w, "ok")
	})
	mux.HandleFunc("DELETE /api/admin/posts/{id}", func(w http.ResponseWriter, r *http.Request) {
		if !adminOK(w, r) {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		for i, p := range db.Posts {
			if p.ID == r.PathValue("id") {
				db.Posts = append(db.Posts[:i], db.Posts[i+1:]...)
				save()
				out(w, "ok")
				return
			}
		}
		fail(w, 404, "Not found.")
	})

	mux.HandleFunc("GET /api/admin/reports", func(w http.ResponseWriter, r *http.Request) {
		if !adminOK(w, r) {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		res := []map[string]any{}
		for i := len(db.Reports) - 1; i >= 0 && len(res) < 100; i-- {
			x := db.Reports[i]
			e := map[string]any{"id": x.ID, "by": x.By, "type": x.Type, "target": x.Target, "reason": x.Reason, "at": x.At, "status": x.Status, "author": "", "preview": ""}
			if x.Type == "post" {
				e["preview"] = "(post was deleted)"
				for _, p := range db.Posts {
					if p.ID == x.Target {
						t := []rune(p.Text)
						if len(t) > 120 {
							t = t[:120]
						}
						e["author"], e["preview"] = p.User, string(t)
						if len(t) == 0 {
							e["preview"] = "(photo or video post)"
						}
					}
				}
			} else {
				e["author"] = x.Target
			}
			res = append(res, e)
		}
		out(w, res)
	})
	mux.HandleFunc("POST /api/admin/reports/{id}/resolve", func(w http.ResponseWriter, r *http.Request) {
		if !adminOK(w, r) {
			return
		}
		var b struct{ Action string }
		readJSON(r, &b)
		mu.Lock()
		defer mu.Unlock()
		for _, x := range db.Reports {
			if x.ID != r.PathValue("id") {
				continue
			}
			author := x.Target
			if x.Type == "post" {
				author = ""
				for _, p := range db.Posts {
					if p.ID == x.Target {
						author = p.User
					}
				}
			}
			switch b.Action {
			case "dismiss":
				x.Status = "dismissed"
			case "delete_post":
				for i, p := range db.Posts {
					if p.ID == x.Target && x.Type == "post" {
						db.Posts = append(db.Posts[:i], db.Posts[i+1:]...)
						break
					}
				}
				x.Status = "actioned"
			case "ban":
				if author == "" || !setBan(author, true) {
					fail(w, 400, "Cannot suspend that account.")
					return
				}
				x.Status = "actioned"
			default:
				fail(w, 400, "Bad request.")
				return
			}
			save()
			out(w, "ok")
			return
		}
		fail(w, 404, "Not found.")
	})
	mux.HandleFunc("POST /api/admin/announce", func(w http.ResponseWriter, r *http.Request) {
		if !adminOK(w, r) {
			return
		}
		var b struct{ Text string }
		readJSON(r, &b)
		t := []rune(strings.TrimSpace(b.Text))
		if len(t) > 300 {
			t = t[:300]
		}
		mu.Lock()
		defer mu.Unlock()
		db.Announce, db.AnnounceID = string(t), ""
		if len(t) > 0 {
			db.AnnounceID = rnd(4)
		}
		save()
		out(w, "ok")
	})
}
