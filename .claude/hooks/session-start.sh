#!/bin/bash
# Claude Code cloud sessions only: get Node 24, the `vp` CLI and the pnpm
# workspace install so lint, typecheck and tests run. vite.plus is blocked in
# the cloud, so `vp` comes from npm and Node 24 from nodejs.org. Native
# macOS/iOS builds and desktop packaging stay local.
set -euo pipefail

if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

cd "${CLAUDE_PROJECT_DIR:-.}"

NODE_DIR="$HOME/.local/node24"
if ! node -e 'process.exit(parseInt(process.versions.node, 10) >= 24 ? 0 : 1)' 2>/dev/null; then
  if [ ! -x "$NODE_DIR/bin/node" ]; then
    mkdir -p "$NODE_DIR"
    version=$(curl -fsSL https://nodejs.org/dist/latest-v24.x/SHASUMS256.txt \
      | sed -n 's/.*node-\(v[0-9.]*\)-linux-x64\.tar\.xz$/\1/p' | head -1)
    curl -fsSL "https://nodejs.org/dist/latest-v24.x/node-${version}-linux-x64.tar.xz" \
      | tar -xJ -C "$NODE_DIR" --strip-components=1
  fi
  export PATH="$NODE_DIR/bin:$PATH"
fi

if ! command -v vp >/dev/null 2>&1; then
  npm install -g vite-plus --no-audit --no-fund
fi

if [ -n "${CLAUDE_ENV_FILE:-}" ]; then
  printf 'export PATH=%q:"$PATH"\n' "$NODE_DIR/bin" >> "$CLAUDE_ENV_FILE"
fi

# SessionStart also fires on resume, clear and compact. `vp i` is a no-op when
# node_modules already matches the lockfile.
CI=true vp i

# The npm-installed `vp` is only a bootstrap: its vitest differs from the one the
# workspace pins and fails every suite. Prefer the workspace's own `vp`.
if [ -n "${CLAUDE_ENV_FILE:-}" ]; then
  printf 'export PATH=%q:"$PATH"\n' "$PWD/node_modules/.bin" >> "$CLAUDE_ENV_FILE"
fi
