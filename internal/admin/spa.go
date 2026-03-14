package admin

import (
	"io/fs"
	"net/http"
	"strings"
)

// SPAFileServer serves embedded static files with a catch-all fallback
// to index.html for client-side routing.
func SPAFileServer(fsys fs.FS) http.Handler {
	fileServer := http.FileServer(http.FS(fsys))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip API and well-known paths.
		path := r.URL.Path
		if strings.HasPrefix(path, "/api/") ||
			path == "/metrics" ||
			path == "/healthz" {
			http.NotFound(w, r)
			return
		}

		// Try to serve the file directly.
		p := strings.TrimPrefix(path, "/")
		if p == "" {
			p = "index.html"
		}
		if _, err := fs.Stat(fsys, p); err == nil {
			fileServer.ServeHTTP(w, r)
			return
		}

		// Fallback: serve index.html for SPA routing.
		r.URL.Path = "/"
		fileServer.ServeHTTP(w, r)
	})
}
