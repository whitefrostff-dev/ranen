# Amlink

Go server + one `index.html`. Files: `main.go auth.go dm.go admin.go store.go index.html go.mod`.

## First-time setup (once, on your computer)
    go get github.com/jackc/pgx/v5
    go mod tidy
    go run .            # http://localhost:8080
Commit `go.mod` AND `go.sum` to GitHub (Render needs both). Needs Go 1.22+.
If Render complains about the Go version, add an env var `GO_VERSION` matching `go version` on your computer.

## Environment variables
| Name | What it does |
|---|---|
| `DATABASE_URL` | Supabase Postgres connection string (Connect > pooler). Without it, data.json is used |
| `RESEND_API_KEY` | Sends the email codes through Resend |
| `RESEND_FROM` | e.g. `Amlink <noreply@yourdomain.com>` (needs a domain verified in Resend) |
| `ADMIN_EMAILS` | Comma-separated admin emails (default: whitefrostff@gmail.com) |
| `SUPABASE_URL`, `SUPABASE_KEY`, `SUPABASE_BUCKET` | Keeps photos/videos/voice notes in Supabase Storage (bucket must be Public; default name `uploads`) |
| `GOOGLE_CLIENT_ID` | Turns on "Continue with Google" |
| `DATA_DIR`, `PORT` | Local data folder (when not using a database), port (default 8080) |

Set these in Render: your service > Environment. Never put them in your code or on GitHub.

## Admin
Sign up (or log in with an email code) using an email listed in `ADMIN_EMAILS` and verify it.
An "Admin panel" appears in the menu: stats, reports, users (suspend, delete, verified badge), announcements.

## Notes
- Without `RESEND_API_KEY` the codes are printed in the server log instead of emailed.
- Resend: until you verify a domain, it only delivers to your own Resend account email.
- Never commit data.json, backups/ or uploads/ (the .gitignore blocks them).
