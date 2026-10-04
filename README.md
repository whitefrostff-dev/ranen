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
| `SUPABASE_URL`, `SUPABASE_KEY`, `SUPABASE_BUCKET` | Keeps photos/videos/voice notes in Supabase Storage (bucket must be Public; default name `uploads`) |
| `GOOGLE_CLIENT_ID` | Turns on "Continue with Google" (the main way to sign up and log in) |
| `DATA_DIR`, `PORT` | Local data folder (when not using a database), port (default 8080) |

Set these in Render: your service > Environment. Never put them in your code or on GitHub.

## Google sign-in (main login)
1. console.cloud.google.com: create a project, set up the Google Auth Platform / OAuth consent screen (External).
2. Credentials > Create client > **Web application**. Under **Authorized JavaScript origins** add your site, e.g. `https://your-app.onrender.com` (https only; add `http://localhost:8080` for local tests if Google accepts it).
3. Copy the **Client ID** into Render as `GOOGLE_CLIENT_ID`.
4. While the app is in "Testing", only the test users you list can sign in. **Publish the app** to let everyone in.
New users finish a 3-page form (username, details, terms). Email codes / passwords only appear on the site if email sending (`RESEND_API_KEY` or `SMTP_HOST`) is configured.

## Admin
Only the Google-verified account **whitefrostff@gmail.com** is an admin (fixed in `admin.go`).
Panel: stats, announcements, pause sign-ups, reports, users (suspend, delete, badge, clear photo/bio), all posts and comments, activity log, CSV export. Admins cannot read private messages.

## Notes
- Without `RESEND_API_KEY` the codes are printed in the server log instead of emailed.
- Resend: until you verify a domain, it only delivers to your own Resend account email.
- Never commit data.json, backups/ or uploads/ (the .gitignore blocks them).
