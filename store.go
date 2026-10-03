package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

var (
	pg     *sql.DB
	saveCh = make(chan []byte, 1)
)

// loadState reads everything from Postgres (DATABASE_URL) or, if that is not set, from data.json.
func loadState() {
	var raw []byte
	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		if !strings.Contains(dsn, "default_query_exec_mode") { // works with Supabase poolers
			sep := "?"
			if strings.Contains(dsn, "?") {
				sep = "&"
			}
			dsn += sep + "default_query_exec_mode=simple_protocol"
		}
		var err error
		if pg, err = sql.Open("pgx", dsn); err != nil {
			log.Fatal("database:", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if _, err = pg.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS amlink_state (id int PRIMARY KEY, data jsonb NOT NULL, updated_at timestamptz NOT NULL DEFAULT now())`); err != nil {
			log.Fatal("database connection failed (not starting, so nothing is overwritten): ", err)
		}
		var s string
		switch err = pg.QueryRowContext(ctx, `SELECT data::text FROM amlink_state WHERE id = 1`).Scan(&s); {
		case err == sql.ErrNoRows:
			log.Println("database connected, starting empty")
		case err != nil:
			log.Fatal("database read failed: ", err)
		default:
			raw = []byte(s)
			log.Println("database connected")
		}
	} else if b, err := os.ReadFile(filepath.Join(dataDir, "data.json")); err == nil {
		raw = b
	} else {
		log.Printf("no data.json in %s yet, starting empty", dataDir)
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, db); err != nil {
			log.Fatalf("saved data is unreadable (%v). Not starting, so it is not overwritten.", err)
		}
	}
	log.Printf("loaded %d users", len(db.Users))
	if db.Users == nil {
		db.Users = map[string]*User{}
	}
	if db.Sessions == nil {
		db.Sessions = map[string]string{}
	}
	if db.Follows == nil {
		db.Follows = map[string][]string{}
	}
	if db.Blocks == nil {
		db.Blocks = map[string][]string{}
	}
	go func() {
		for b := range saveCh {
			writeState(b)
		}
	}()
	go func() { // flush on shutdown/redeploy
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		mu.Lock()
		b, _ := json.Marshal(db)
		mu.Unlock()
		writeState(b)
		os.Exit(0)
	}()
}

// enqueue keeps only the newest snapshot waiting to be written.
func enqueue(b []byte) {
	for {
		select {
		case saveCh <- b:
			return
		default:
			select {
			case <-saveCh:
			default:
			}
		}
	}
}

func writeState(b []byte) {
	if pg != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if _, err := pg.ExecContext(ctx, `INSERT INTO amlink_state (id, data, updated_at) VALUES (1, $1::jsonb, now()) ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, updated_at = now()`, string(b)); err != nil {
			log.Println("DATABASE SAVE FAILED:", err)
		}
		return
	}
	os.MkdirAll(dataDir, 0o755)
	p := filepath.Join(dataDir, "data.json")
	if err := os.WriteFile(p+".tmp", b, 0o600); err != nil {
		log.Println("save:", err)
		return
	}
	os.Rename(p+".tmp", p)
	bd := filepath.Join(dataDir, "backups")
	os.MkdirAll(bd, 0o755)
	bp := filepath.Join(bd, "data-"+time.Now().Format("2006-01-02")+".json")
	if _, err := os.Stat(bp); err != nil {
		os.WriteFile(bp, b, 0o600)
		if fs, _ := filepath.Glob(filepath.Join(bd, "data-*.json")); len(fs) > 14 {
			for _, f := range fs[:len(fs)-14] {
				os.Remove(f)
			}
		}
	}
}

// saveUpload stores an uploaded file in Supabase Storage (if SUPABASE_URL and SUPABASE_KEY are set) or on local disk.
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
	name := rnd(12) + ext
	kind := strings.SplitN(ct, "/", 2)[0]
	base, key := strings.TrimRight(os.Getenv("SUPABASE_URL"), "/"), os.Getenv("SUPABASE_KEY")
	if base != "" && key != "" {
		bucket := envOr("SUPABASE_BUCKET", "uploads")
		body, _ := io.ReadAll(io.MultiReader(bytes.NewReader(head[:n]), f))
		req, _ := http.NewRequest("POST", base+"/storage/v1/object/"+bucket+"/"+name, bytes.NewReader(body))
		req.Header.Set("apikey", key)
		if !strings.HasPrefix(key, "sb_") {
			req.Header.Set("Authorization", "Bearer "+key)
		}
		req.Header.Set("Content-Type", ct)
		resp, err := (&http.Client{Timeout: 60 * time.Second}).Do(req)
		if err != nil {
			return "", "", err
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<12))
			log.Printf("STORAGE UPLOAD FAILED %d: %s", resp.StatusCode, msg)
			return "", "", os.ErrInvalid
		}
		return base + "/storage/v1/object/public/" + bucket + "/" + name, kind, nil
	}
	os.MkdirAll(uploadDir, 0o755)
	dst, err := os.Create(filepath.Join(uploadDir, name))
	if err != nil {
		return "", "", err
	}
	defer dst.Close()
	dst.Write(head[:n])
	io.Copy(dst, f)
	return "/uploads/" + name, kind, nil
}
