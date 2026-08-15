#!/usr/bin/env bash
# Build the reproducible eurobot image (and optionally push it).
#
# Reproducibility requires the SOURCE_DATE_EPOCH build arg — this script is
# the canonical way to build; two builds of the same commit produce the
# same registry digest.
#
# Usage:
#   scripts/build-image.sh                  # tag = pyproject version
#   scripts/build-image.sh 0.1.3            # explicit tag
#   scripts/build-image.sh 0.1.3 --push     # build and push to Docker Hub
set -euo pipefail
cd "$(dirname "$0")/.."

IMAGE="paluugi/eurobot"
EPOCH=946684800   # must match SOURCE_DATE_EPOCH in the Dockerfile

VERSION="${1:-$(grep -m1 '^version' pyproject.toml | cut -d'"' -f2)}"
PUSH="${2:-}"

# --provenance=false --sbom=false: build attestations embed build timestamps
# and would change the pushed digest on every rebuild.
ARGS=(
  --provenance=false
  --sbom=false
  --build-arg SOURCE_DATE_EPOCH="$EPOCH"
  -t "$IMAGE:$VERSION"
  -t "$IMAGE:latest"
)

docker build "${ARGS[@]}" .

if [[ "$PUSH" == "--push" ]]; then
  docker push "$IMAGE:$VERSION"
  docker push "$IMAGE:latest"
fi
