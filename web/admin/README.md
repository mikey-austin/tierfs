# TierFS Admin UI

React SPA built with Vite. Serves at `:3000` in development; embeds into the
Go binary as a static asset for production.

## Development

```bash
cd web/admin
npm install
npm run dev          # starts Vite dev server on :3000
                     # API/metrics calls proxy to localhost:9100
```

## Production build

```bash
npm run build        # outputs to web/admin/dist/
```

The Go HTTP server in `internal/admin/server.go` embeds `dist/` via `//go:embed`
and serves the SPA at `/admin` on the same `:9100` metrics port.

## Connecting to real data

The UI currently uses simulated data generators. To wire to live TierFS data:

1. Replace the `makeFiles()`, `makeMetrics()`, `makeJobs()`, `makeLogs()` calls
   in `src/App.jsx` with `fetch()` calls to the TierFS HTTP API.
2. The metrics view can scrape `/metrics` (Prometheus exposition) directly using
   the `promjs` client, or you can expose a `/api/v1/metrics/summary` endpoint
   that returns the key counters as JSON.

Suggested API surface (to implement in Go):

```
GET /api/v1/files               → paginated file list with tier/state filters
GET /api/v1/replication/queue   → pending CopyJob list
GET /api/v1/tiers               → tier status + byte/file counts
GET /api/v1/writeguard          → active write handles
GET /api/v1/logs?tail=100       → last N log lines as JSON
GET /metrics                    → Prometheus exposition (already exists)
GET /healthz                    → liveness (already exists)
```
