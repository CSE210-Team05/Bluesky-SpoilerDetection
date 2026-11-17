#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

required_files=(
  docs/index.html
  docs/app.js
  docs/styles.css
  docs/metrics.json
)

for file in "${required_files[@]}"; do
  if [[ ! -s "$file" ]]; then
    echo "Missing or empty Pages file: $file" >&2
    exit 1
  fi
done

python3 -m json.tool docs/metrics.json >/dev/null

if find docs -type l -print -quit | grep -q .; then
  echo "GitHub Pages artifact must not contain symbolic links" >&2
  exit 1
fi

echo "GitHub Pages dashboard is ready in docs/"
