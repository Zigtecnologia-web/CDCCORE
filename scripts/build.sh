#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST_DIR="$ROOT_DIR/dist"
MODULE_DIR="$ROOT_DIR/consumer"

TARGETS=(
    "linux amd64"
    "linux arm64"
    "darwin amd64"
    "darwin arm64"
)

rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"

for target in "${TARGETS[@]}"; do
    read -r goos goarch <<< "$target"
    output="$DIST_DIR/cdc-postgres-$goos-$goarch"

    echo "Building $goos/$goarch..."
    (
        cd "$MODULE_DIR"
        CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" go build -trimpath -o "$output" .
    )
done

echo
echo "Build completed."
echo
echo "dist/"
for target in "${TARGETS[@]}"; do
    read -r goos goarch <<< "$target"
    echo "  cdc-postgres-$goos-$goarch"
done
