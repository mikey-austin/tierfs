package adminui

import "embed"

// DistFS holds the built admin UI assets (produced by npm run build).
// During development, run "make ui" first or the embed will contain
// only the .gitkeep placeholder.
//
//go:embed all:dist
var DistFS embed.FS
