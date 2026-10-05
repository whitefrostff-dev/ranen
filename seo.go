package main

import (
	"net/http"
	"os"
	"strings"
)

// siteURL is the public address of the site. Set SITE_URL in Render (e.g. https://your-app.onrender.com) to be exact.
func siteURL(r *http.Request) string {
	if u := strings.TrimRight(os.Getenv("SITE_URL"), "/"); u != "" {
		return u
	}
	host := r.Host
	for _, c := range host {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '-' || c == ':') {
			return "https://localhost"
		}
	}
	proto := "https"
	if p := r.Header.Get("X-Forwarded-Proto"); p == "http" || p == "https" {
		proto = p
	} else if strings.HasPrefix(host, "localhost") || strings.HasPrefix(host, "127.0.0.1") {
		proto = "http"
	}
	return proto + "://" + host
}

// serveIndex sends index.html with the real site address filled in for search engines.
func serveIndex(w http.ResponseWriter, r *http.Request) {
	b, err := os.ReadFile("index.html")
	if err != nil {
		http.Error(w, "index.html is missing", 500)
		return
	}
	gsv := ""
	if t := os.Getenv("GOOGLE_SITE_VERIFICATION"); t != "" && !strings.ContainsAny(t, "\"'<> \n") {
		gsv = `<meta name="google-site-verification" content="` + t + `">`
	}
	h := strings.ReplaceAll(string(b), "{{SITE}}", siteURL(r))
	h = strings.ReplaceAll(h, "{{GSV}}", gsv)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(h))
}

func registerSEO(mux *http.ServeMux) {
	mux.HandleFunc("GET /robots.txt", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write([]byte("User-agent: *\nAllow: /\nDisallow: /api/\nDisallow: /uploads/\n\nSitemap: " + siteURL(r) + "/sitemap.xml\n"))
	})
	mux.HandleFunc("GET /sitemap.xml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
		w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>` + "\n" + `<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><url><loc>` + siteURL(r) + `/</loc></url></urlset>` + "\n"))
	})
}
