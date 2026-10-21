#!/bin/sh
# Install amp-harness from this checkout:
#   - npm dependencies
#   - `amp-harness` on PATH (symlink in ~/.local/bin, or $BIN_DIR)
#   - the pi provider extension (~/.pi/agent/extensions/ampersand-provider.ts)
set -eu
REPO="$(cd "$(dirname "$0")/.." && pwd)"
BIN_DIR="${BIN_DIR:-$HOME/.local/bin}"
EXT_DIR="${PI_EXTENSIONS_DIR:-$HOME/.pi/agent/extensions}"

(cd "$REPO" && npm install --no-fund --no-audit --silent)

mkdir -p "$BIN_DIR"
ln -sf "$REPO/bin/amp-harness" "$BIN_DIR/amp-harness"
echo "linked $BIN_DIR/amp-harness"
case ":$PATH:" in *":$BIN_DIR:"*) ;; *) echo "note: add $BIN_DIR to your PATH" ;; esac

mkdir -p "$EXT_DIR"
cat > "$EXT_DIR/ampersand-provider.ts" <<EOF
// Ampersand (humansand.ai Agents API) provider for pi. Source: $REPO
export { default } from "$REPO/src/extension.ts";
EOF
echo "installed pi extension $EXT_DIR/ampersand-provider.ts"

[ -n "${AMPERSAND_API_KEY:-}" ] || echo "note: export AMPERSAND_API_KEY (get one at https://ampersand.humansand.ai/)"
