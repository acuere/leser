#!/bin/sh
# leser installer — download the latest static binary for this OS/arch.
#
#   curl -fsSL https://raw.githubusercontent.com/acuere/leser/main/scripts/install.sh | sh
#
# Env overrides:
#   LESER_VERSION   pin a release tag (default: latest)
#   LESER_INSTALL   install dir (default: /usr/local/bin, falls back to ~/.local/bin)
set -eu

REPO="acuere/leser"
VERSION="${LESER_VERSION:-latest}"

log()  { printf '\033[1;34m==>\033[0m %s\n' "$1" >&2; }
err()  { printf '\033[1;31merror:\033[0m %s\n' "$1" >&2; exit 1; }

# --- detect platform ---
os="$(uname -s | tr '[:upper:]' '[:lower:]')"
arch="$(uname -m)"
case "$os" in
  linux|darwin) ;;
  *) err "unsupported OS: $os (use Docker or build from source)";;
esac
case "$arch" in
  x86_64|amd64) arch="amd64";;
  arm64|aarch64) arch="arm64";;
  *) err "unsupported arch: $arch";;
esac

# --- resolve version ---
if [ "$VERSION" = "latest" ]; then
  VERSION="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
    | grep '"tag_name"' | head -1 | cut -d'"' -f4)"
  [ -n "$VERSION" ] || err "could not resolve latest version (no releases yet?)"
fi
log "installing leser ${VERSION} (${os}/${arch})"

# --- choose install dir ---
dir="${LESER_INSTALL:-/usr/local/bin}"
if [ ! -w "$dir" ] 2>/dev/null && [ "$(id -u)" -ne 0 ]; then
  dir="$HOME/.local/bin"
  mkdir -p "$dir"
  log "no write access to /usr/local/bin; installing to $dir"
fi

# --- download + verify ---
base="https://github.com/${REPO}/releases/download/${VERSION}"
asset="leser_${os}_${arch}"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

log "downloading ${asset}"
curl -fSL --progress-bar "${base}/${asset}" -o "${tmp}/leser" \
  || err "download failed: ${base}/${asset}"

# checksum verification if the release ships checksums.txt
if curl -fsSL "${base}/checksums.txt" -o "${tmp}/checksums.txt" 2>/dev/null; then
  log "verifying checksum"
  ( cd "$tmp" && grep " ${asset}\$" checksums.txt | sed "s/${asset}/leser/" \
    | { command -v sha256sum >/dev/null && sha256sum -c - || shasum -a 256 -c - ; } ) \
    || err "checksum verification failed"
else
  log "no checksums.txt published; skipping verification"
fi

chmod +x "${tmp}/leser"
mv "${tmp}/leser" "${dir}/leser"

log "installed to ${dir}/leser"
case ":$PATH:" in
  *":$dir:"*) ;;
  *) log "add to PATH:  export PATH=\"$dir:\$PATH\"";;
esac
log "run:  leser serve"
