package main

import (
	"net/http"
	"strings"
	"time"
)

func msgLabel(m *Msg) string {
	if m.T != "" {
		return m.T
	}
	switch m.Kind {
	case "image":
		return "📷 Photo"
	case "video":
		return "🎥 Video"
	case "audio":
		return "🎤 Voice note"
	}
	return ""
}

// linkReply needs mu held. It quotes message id inside m when both belong to the same chat.
func linkReply(m *Msg, id string) {
	if id == "" {
		return
	}
	for _, o := range db.Msgs {
		if o.ID == id && !o.Deleted && ((o.From == m.From && o.To == m.To) || (o.From == m.To && o.To == m.From)) {
			rt := []rune(msgLabel(o))
			if len(rt) > 80 {
				rt = rt[:80]
			}
			m.RID, m.RF, m.RT = o.ID, o.From, string(rt)
		}
	}
}

func registerDM(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/messages/media", func(w http.ResponseWriter, r *http.Request) {
		me := who(r)
		if me == "" {
			fail(w, 401, "Log in first.")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 30<<20)
		if r.ParseMultipartForm(8<<20) != nil {
			fail(w, 400, "File too large (30 MB max).")
			return
		}
		to := r.FormValue("to")
		mu.Lock()
		ok := db.Users[to] != nil && !blocked(me, to)
		mu.Unlock()
		if !ok {
			fail(w, 400, "Could not send message.")
			return
		}
		src, kind, err := saveUpload(r, "media")
		if err != nil || src == "" {
			fail(w, 400, "Send a JPG, PNG, GIF, WebP, MP4 or WebM file, or a voice note.")
			return
		}
		if kind == "video" && r.FormValue("kind") == "voice" {
			kind = "audio"
		}
		mu.Lock()
		defer mu.Unlock()
		m := &Msg{ID: rnd(5), From: me, To: to, At: time.Now().UnixMilli(), Src: src, Kind: kind}
		if c := strings.TrimSpace(r.FormValue("text")); len(c) <= 1000 {
			m.T = c
		}
		linkReply(m, r.FormValue("reply"))
		db.Msgs = append(db.Msgs, m)
		save()
		out(w, m)
	})
}
