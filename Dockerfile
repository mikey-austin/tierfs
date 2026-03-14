# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM golang:1.26.1-bookworm AS builder

WORKDIR /src

# Cache dependencies before copying source.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath \
    -ldflags "-s -w -X main.version=$(git describe --tags --always --dirty 2>/dev/null || echo dev)" \
    -o /out/tierfs ./cmd/tierfs

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

# fuse3 needed for fusermount3; ca-certificates for TLS to S3 endpoints.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        fuse3 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Allow non-root container processes to use FUSE.
RUN echo "user_allow_other" >> /etc/fuse.conf

COPY --from=builder /out/tierfs /usr/local/bin/tierfs

# Runtime directories; override via bind mounts in production.
RUN mkdir -p \
    /etc/tierfs \
    /var/lib/tierfs \
    /var/log/tierfs \
    /tmp/tierfs-stage \
    /share

VOLUME ["/share", "/var/lib/tierfs", "/var/log/tierfs"]

EXPOSE 9100

# FUSE requires SYS_ADMIN capability and /dev/fuse device.
# Run with: docker run --cap-add SYS_ADMIN --device /dev/fuse ...
ENTRYPOINT ["/usr/local/bin/tierfs"]
CMD ["-config", "/etc/tierfs/tierfs.toml"]
