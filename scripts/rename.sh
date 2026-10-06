#!/usr/bin/env bash
# Rename the Go module and the image/service names after cloning the template.
#   ./scripts/rename.sh github.com/acme/billing-service
set -euo pipefail

NEW="${1:?usage: scripts/rename.sh <new/module/path>}"
OLD="github.com/yourorg/go-clean-template"
NEW_NAME="$(basename "$NEW")"
NEW_ORG_PATH="${NEW#github.com/}"      # acme/billing-service
OLD_ORG_PATH="yourorg/go-clean-template"

cd "$(dirname "$0")/.."

# sed -i differs between GNU and BSD (macOS)
sedi() { if sed --version >/dev/null 2>&1; then sed -i "$@"; else sed -i '' "$@"; fi; }

files=$(grep -rIl --exclude-dir=.git --exclude-dir=bin --exclude=go.sum -e "$OLD" -e "$OLD_ORG_PATH" -e "go-clean-template" . || true)
for f in $files; do
  sedi -e "s#$OLD#$NEW#g" -e "s#$OLD_ORG_PATH#$NEW_ORG_PATH#g" -e "s#go-clean-template#$NEW_NAME#g" "$f"
done

go mod tidy
echo "renamed to $NEW. Next: make check"
