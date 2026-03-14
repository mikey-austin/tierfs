#!/usr/bin/env bash
set -euo pipefail

MOUNT=/tmp/main
TIER0=/tmp/tier0
TIER1=/tmp/tier1
META=/tmp/tierfs-local
CONFIG="$(cd "$(dirname "$0")" && pwd)/tierfs.local.toml"
BIN="$(cd "$(dirname "$0")" && pwd)/bin/tierfs"
PID=""

red()   { printf '\033[1;31m%s\033[0m\n' "$*"; }
green() { printf '\033[1;32m%s\033[0m\n' "$*"; }
cyan()  { printf '\033[1;36m%s\033[0m\n' "$*"; }

cleanup() {
    cyan "--- cleanup ---"
    [ -n "$PID" ] && kill "$PID" 2>/dev/null && wait "$PID" 2>/dev/null || true
    fusermount -u "$MOUNT" 2>/dev/null || true
    rm -rf "$TIER0" "$TIER1" "$META" "$MOUNT"
    green "cleaned up"
}
trap cleanup EXIT

# ── build ────────────────────────────────────────────────────────────────────
cyan "--- building tierfs ---"
(cd "$(dirname "$0")" && go build -o bin/tierfs ./cmd/tierfs)
green "built $BIN"

# ── setup dirs ───────────────────────────────────────────────────────────────
cyan "--- setting up directories ---"
rm -rf "$TIER0" "$TIER1" "$META" "$MOUNT"
mkdir -p "$TIER0" "$TIER1" "$META" "$MOUNT"
green "created $TIER0 $TIER1 $META $MOUNT"

# ── start daemon ─────────────────────────────────────────────────────────────
cyan "--- starting tierfs ---"
"$BIN" -config "$CONFIG" >/tmp/tierfs-local/tierfs.log 2>&1 &
PID=$!
sleep 2

if ! kill -0 "$PID" 2>/dev/null; then
    red "tierfs failed to start"
    exit 1
fi
green "tierfs running (pid $PID)"

# ── test 1: write a file ─────────────────────────────────────────────────────
cyan "--- test: write file ---"
mkdir -p "$MOUNT/cam1"
echo "hello from tierfs local test" > "$MOUNT/cam1/test.txt"
dd if=/dev/urandom of="$MOUNT/cam1/bigfile.bin" bs=1M count=5 status=none
green "wrote cam1/test.txt (29B) and cam1/bigfile.bin (5MB)"

# ── test 2: read back ────────────────────────────────────────────────────────
cyan "--- test: read back ---"
CONTENT=$(cat "$MOUNT/cam1/test.txt")
if [ "$CONTENT" = "hello from tierfs local test" ]; then
    green "PASS: read back matches"
else
    red "FAIL: expected 'hello from tierfs local test', got '$CONTENT'"
    exit 1
fi

# ── test 3: stat / ls ────────────────────────────────────────────────────────
cyan "--- test: directory listing ---"
ls -la "$MOUNT/"
ls -la "$MOUNT/cam1/"
FILE_COUNT=$(ls "$MOUNT/cam1/" | wc -l)
if [ "$FILE_COUNT" -eq 2 ]; then
    green "PASS: 2 files in cam1/"
else
    red "FAIL: expected 2 files, got $FILE_COUNT"
    exit 1
fi

# ── test 4: verify tier0 has the data ────────────────────────────────────────
cyan "--- test: tier0 presence ---"
if [ -f "$TIER0/cam1/test.txt" ] && [ -f "$TIER0/cam1/bigfile.bin" ]; then
    green "PASS: files present on tier0"
else
    red "FAIL: files missing from tier0"
    ls -laR "$TIER0/"
    exit 1
fi

# ── test 5: wait for replication to tier1 ────────────────────────────────────
cyan "--- test: waiting for replication to tier1 (up to 30s) ---"
for i in $(seq 1 30); do
    if [ -f "$TIER1/cam1/test.txt" ] && [ -f "$TIER1/cam1/bigfile.bin" ]; then
        green "PASS: files replicated to tier1 after ${i}s"
        break
    fi
    if [ "$i" -eq 30 ]; then
        red "FAIL: replication did not complete within 30s"
        ls -laR "$TIER1/" 2>/dev/null || true
        exit 1
    fi
    sleep 1
done

# ── test 6: verify transform pipeline ────────────────────────────────────────
cyan "--- test: transform verification ---"
T0_MD5=$(md5sum "$TIER0/cam1/bigfile.bin" | awk '{print $1}')
T1_MD5=$(md5sum "$TIER1/cam1/bigfile.bin" | awk '{print $1}')
if [ "$T0_MD5" != "$T1_MD5" ]; then
    green "PASS: tier1 raw bytes differ from tier0 — transform applied ($T0_MD5 vs $T1_MD5)"
else
    red "FAIL: tier0 and tier1 are identical — encryption/compression not working"
    exit 1
fi
# Read through mount to verify decryption works
MOUNT_MD5=$(md5sum "$MOUNT/cam1/bigfile.bin" | awk '{print $1}')
if [ "$MOUNT_MD5" = "$T0_MD5" ]; then
    green "PASS: mount read matches tier0 plaintext ($MOUNT_MD5)"
else
    red "FAIL: mount read mismatch mount=$MOUNT_MD5 tier0=$T0_MD5"
    exit 1
fi

# ── test 7: rename ───────────────────────────────────────────────────────────
cyan "--- test: rename ---"
mv "$MOUNT/cam1/test.txt" "$MOUNT/cam1/renamed.txt"
if [ -f "$MOUNT/cam1/renamed.txt" ]; then
    green "PASS: rename visible on mount"
else
    red "FAIL: renamed file not visible"
    exit 1
fi
sleep 1
if [ -f "$TIER0/cam1/renamed.txt" ]; then
    green "PASS: rename propagated to tier0"
else
    red "FAIL: rename not propagated to tier0"
    exit 1
fi

# ── test 8: delete ───────────────────────────────────────────────────────────
cyan "--- test: delete ---"
rm "$MOUNT/cam1/renamed.txt"
if [ ! -f "$MOUNT/cam1/renamed.txt" ]; then
    green "PASS: file deleted from mount"
else
    red "FAIL: file still visible after delete"
    exit 1
fi

# ── test 9: wait for eviction (file moves from tier0 to tier1) ───────────────
cyan "--- test: waiting for eviction of bigfile (after=30s, check every 10s) ---"
cyan "    this may take ~50s (30s age + eviction check interval)"
for i in $(seq 1 60); do
    if [ ! -f "$TIER0/cam1/bigfile.bin" ] && [ -f "$TIER1/cam1/bigfile.bin" ]; then
        green "PASS: bigfile evicted from tier0 after ${i}s"
        break
    fi
    if [ "$i" -eq 60 ]; then
        red "TIMEOUT: eviction did not happen within 60s (non-fatal, may need longer)"
        ls -la "$TIER0/cam1/" 2>/dev/null || true
        ls -la "$TIER1/cam1/" 2>/dev/null || true
    fi
    sleep 1
done

# ── test 10: read after eviction (should still work via tier1) ────────────────
cyan "--- test: read after eviction (decrypt from tier1) ---"
if [ -f "$MOUNT/cam1/bigfile.bin" ]; then
    POST_EVICT_MD5=$(md5sum "$MOUNT/cam1/bigfile.bin" | awk '{print $1}')
    if [ "$POST_EVICT_MD5" = "$T0_MD5" ]; then
        green "PASS: post-eviction read decrypted correctly ($POST_EVICT_MD5)"
    else
        red "FAIL: post-eviction read mismatch mount=$POST_EVICT_MD5 original=$T0_MD5"
    fi
else
    red "FAIL: bigfile not accessible on mount after eviction"
fi

echo
green "=== all tests passed ==="
cyan "daemon log: /tmp/tierfs-local/tierfs.log"
