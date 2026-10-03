package main

import (
	"bytes"
	"crypto/rand"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"net/smtp"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

var emailRe = regexp.MustCompile(`^[^\s@]+@[^\s@]+\.[^\s@]+$`)

type codeEntry struct {
	Code  string
	Exp   time.Time
	Tries int
	Sent  time.Time
}

var (
	codes      = map[string]*codeEntry{}
	loginFails = map[string][]time.Time{}
)

func okDOB(s string) bool {
	t, err := time.Parse("2006-01-02", s)
	return err == nil && t.Before(time.Now().AddDate(-13, 0, 0)) && t.After(time.Now().AddDate(-120, 0, 0))
}

// byEmail needs mu held.
func byEmail(e string) *User {
	for _, u := range db.Users {
		if strings.EqualFold(u.Email, e) {
			return u
		}
	}
	return nil
}

// issueCode needs mu held. It returns false if a code was sent less than 45s ago.
func issueCode(email, purpose string) (string, bool) {
	k := purpose + "|" + email
	if e := codes[k]; e != nil && time.Since(e.Sent) < 45*time.Second {
		return "", false
	}
	n, _ := rand.Int(rand.Reader, big.NewInt(1000000))
	c := fmt.Sprintf("%06d", n)
	codes[k] = &codeEntry{c, time.Now().Add(10 * time.Minute), 0, time.Now()}
	return c, true
}

// checkCode needs mu held. Codes expire after 10 minutes and allow 5 attempts.
func checkCode(email, purpose, code string) bool {
	k := purpose + "|" + email
	e := codes[k]
	if e == nil || time.Now().After(e.Exp) || e.Tries >= 5 {
		delete(codes, k)
		return false
	}
	e.Tries++
	if subtle.ConstantTimeCompare([]byte(e.Code), []byte(strings.TrimSpace(code))) == 1 {
		delete(codes, k)
		return true
	}
	return false
}

// sendMail tries Resend (RESEND_API_KEY), then SMTP (SMTP_HOST), otherwise prints the email in the server console.
func sendMail(to, subject, text, html string) {
	if key := os.Getenv("RESEND_API_KEY"); key != "" {
		payload, _ := json.Marshal(map[string]any{"from": envOr("RESEND_FROM", "Amlink <onboarding@resend.dev>"), "to": []string{to}, "subject": subject, "text": text, "html": html})
		req, _ := http.NewRequest("POST", "https://api.resend.com/emails", bytes.NewReader(payload))
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
		if err != nil {
			log.Println("RESEND ERROR:", err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<12))
			log.Printf("RESEND ERROR %d: %s", resp.StatusCode, msg)
		}
		return
	}
	host := os.Getenv("SMTP_HOST")
	if host == "" {
		log.Printf("[EMAIL NOT CONFIGURED] to=%s | %s | %s", to, subject, text)
		return
	}
	user, pass := os.Getenv("SMTP_USER"), os.Getenv("SMTP_PASS")
	from := envOr("SMTP_FROM", user)
	msg := "From: Amlink <" + from + ">\r\nTo: " + to + "\r\nSubject: " + subject + "\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n" + text
	if err := smtp.SendMail(host+":"+envOr("SMTP_PORT", "587"), smtp.PlainAuth("", user, pass, host), from, []string{to}, []byte(msg)); err != nil {
		log.Println("sendMail:", err)
	}
}

func mailCode(email, purpose, code string) {
	what := map[string]string{"verify": "verify your email", "login": "log in", "reset": "reset your password"}[purpose]
	text := "Use this code to " + what + ": " + code + "\n\nIt expires in 10 minutes. If you did not ask for it, ignore this email.\n\n- Amlink"
	html := `<div style="font-family:Arial,sans-serif;max-width:420px;margin:auto;padding:24px"><h2 style="margin:0 0 12px">Amlink</h2><p>Use this code to ` + what + `:</p><p style="font-size:34px;letter-spacing:8px;font-weight:700;background:#F1F8FF;border-radius:12px;padding:14px;text-align:center">` + code + `</p><p style="color:#666;font-size:13px">It expires in 10 minutes. If you did not ask for it, you can ignore this email.</p></div>`
	go sendMail(email, "Your Amlink code: "+code, text, html)
}

func readJSON(r *http.Request, v any) { json.NewDecoder(io.LimitReader(r.Body, 1<<15)).Decode(v) }

func registerAuth(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/config", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		out(w, map[string]string{"google": os.Getenv("GOOGLE_CLIENT_ID"), "announce": db.Announce, "announceId": db.AnnounceID})
	})

	mux.HandleFunc("POST /api/signup", func(w http.ResponseWriter, r *http.Request) {
		var b struct {
			Username, Email, Password, DOB, Gender, FullName string
			Terms                                  bool
		}
		readJSON(r, &b)
		u, e := strings.ToLower(strings.TrimSpace(b.Username)), strings.ToLower(strings.TrimSpace(b.Email))
		switch {
		case !nameRe.MatchString(u):
			fail(w, 400, "Username: 3 to 20 letters, numbers, dots or underscores.")
			return
		case !emailRe.MatchString(e):
			fail(w, 400, "Enter a valid email.")
			return
		case len(b.Password) < 6:
			fail(w, 400, "Password needs at least 6 characters.")
			return
		case !okDOB(b.DOB):
			fail(w, 400, "Enter your real date of birth. You must be 13 or older.")
			return
		case b.Gender != "female" && b.Gender != "male" && b.Gender != "other" && b.Gender != "none":
			fail(w, 400, "Choose a gender option.")
			return
		case !b.Terms:
			fail(w, 400, "Accept the Terms and Privacy Policy to continue.")
			return
		}
		fn := strings.TrimSpace(b.FullName)
		if r := []rune(fn); len(r) > 40 {
			fn = string(r[:40])
		}
		mu.Lock()
		defer mu.Unlock()
		if x := byEmail(e); x != nil {
			if !x.Pending {
				fail(w, 409, "That email is already registered. Try logging in.")
				return
			}
			delete(db.Users, x.Name)
		}
		if x := db.Users[u]; x != nil {
			if !x.Pending {
				fail(w, 409, "That username is taken.")
				return
			}
			delete(db.Users, u)
		}
		salt := rnd(8)
		db.Users[u] = &User{Name: u, Email: e, Salt: salt, Hash: hash(b.Password, salt), DOB: b.DOB, Gender: b.Gender, Pending: true, FullName: fn, Created: time.Now().Unix()}
		if c, ok := issueCode(e, "verify"); ok {
			mailCode(e, "verify", c)
		}
		save()
		out(w, map[string]any{"needsCode": true, "email": e})
	})

	mux.HandleFunc("POST /api/login", func(w http.ResponseWriter, r *http.Request) {
		var b struct{ Username, Password string }
		readJSON(r, &b)
		id := strings.ToLower(strings.TrimSpace(b.Username))
		mu.Lock()
		defer mu.Unlock()
		recent := []time.Time{}
		for _, t := range loginFails[id] {
			if time.Since(t) < 10*time.Minute {
				recent = append(recent, t)
			}
		}
		loginFails[id] = recent
		if len(recent) >= 8 {
			fail(w, 429, "Too many attempts. Try again in a few minutes.")
			return
		}
		u := db.Users[id]
		if u == nil {
			u = byEmail(id)
		}
		if u == nil || u.Hash != hash(b.Password, u.Salt) {
			loginFails[id] = append(recent, time.Now())
			fail(w, 401, "Wrong username/email or password.")
			return
		}
		if u.Banned {
			fail(w, 403, "This account has been suspended.")
			return
		}
		if u.Pending {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(403)
			json.NewEncoder(w).Encode(map[string]any{"error": "Verify your email to continue. We are sending you a code.", "needsCode": true, "email": u.Email})
			return
		}
		login(w, u.Name)
		save()
		out(w, pubUser(u))
	})

	mux.HandleFunc("POST /api/code/send", func(w http.ResponseWriter, r *http.Request) {
		var b struct{ Email, Purpose string }
		readJSON(r, &b)
		e := strings.ToLower(strings.TrimSpace(b.Email))
		if (b.Purpose == "verify" || b.Purpose == "login" || b.Purpose == "reset") && emailRe.MatchString(e) {
			mu.Lock()
			if u := byEmail(e); u != nil && (b.Purpose != "verify" || u.Pending) {
				if c, ok := issueCode(e, b.Purpose); ok {
					mailCode(e, b.Purpose, c)
				}
			}
			mu.Unlock()
		}
		out(w, map[string]bool{"ok": true}) // same answer whether or not the email exists
	})

	mux.HandleFunc("POST /api/code/verify", func(w http.ResponseWriter, r *http.Request) {
		var b struct{ Email, Code, Purpose, Password string }
		readJSON(r, &b)
		e := strings.ToLower(strings.TrimSpace(b.Email))
		if b.Purpose == "reset" && len(b.Password) < 6 {
			fail(w, 400, "New password needs at least 6 characters.")
			return
		}
		mu.Lock()
		defer mu.Unlock()
		u := byEmail(e)
		if u == nil || !checkCode(e, b.Purpose, b.Code) {
			fail(w, 400, "That code is wrong or expired.")
			return
		}
		if u.Banned {
			fail(w, 403, "This account has been suspended.")
			return
		}
		u.Pending, u.EmailOK = false, true
		switch b.Purpose {
		case "verify", "login":
			login(w, u.Name)
			save()
			out(w, pubUser(u))
		case "reset":
			u.Salt = rnd(8)
			u.Hash = hash(b.Password, u.Salt)
			for k, v := range db.Sessions {
				if v == u.Name {
					delete(db.Sessions, k)
				}
			}
			save()
			out(w, map[string]bool{"ok": true})
		default:
			fail(w, 400, "Bad request.")
		}
	})

	mux.HandleFunc("POST /api/google", func(w http.ResponseWriter, r *http.Request) {
		cid := os.Getenv("GOOGLE_CLIENT_ID")
		if cid == "" {
			fail(w, 400, "Google login is not set up yet.")
			return
		}
		var b struct{ Credential string }
		readJSON(r, &b)
		resp, err := (&http.Client{Timeout: 8 * time.Second}).Get("https://oauth2.googleapis.com/tokeninfo?id_token=" + url.QueryEscape(b.Credential))
		if err != nil {
			fail(w, 502, "Could not reach Google. Try again.")
			return
		}
		defer resp.Body.Close()
		var g struct {
			Aud           string `json:"aud"`
			Email         string `json:"email"`
			EmailVerified string `json:"email_verified"`
		}
		if resp.StatusCode != 200 || json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&g) != nil || g.Aud != cid || g.EmailVerified != "true" || !emailRe.MatchString(g.Email) {
			fail(w, 401, "Google sign-in failed.")
			return
		}
		e := strings.ToLower(g.Email)
		mu.Lock()
		defer mu.Unlock()
		u := byEmail(e)
		isNew := u == nil
		if isNew {
			base := regexp.MustCompile(`[^a-z0-9_.]`).ReplaceAllString(strings.Split(e, "@")[0], "")
			if len(base) < 3 {
				base += "user"
			}
			if len(base) > 16 {
				base = base[:16]
			}
			name := base
			for db.Users[name] != nil {
				name = base + rnd(2)
			}
			u = &User{Name: name, Email: e, Salt: rnd(8), Hash: rnd(16), Created: time.Now().Unix()}
			db.Users[name] = u
		}
		if u.Banned {
			fail(w, 403, "This account has been suspended.")
			return
		}
		u.Pending, u.EmailOK = false, true
		login(w, u.Name)
		save()
		pv := pubUser(u)
		pv["isNew"] = isNew
		out(w, pv)
	})
}
