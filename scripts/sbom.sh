#!/usr/bin/env bash
# Generate an SBOM for the pgllens image (or, without syft, for the locked
# dependency set). Usage: scripts/sbom.sh [output-path] [image-ref]
#
# CycloneDX JSON, because that is what most vulnerability tooling and most
# client security questionnaires ingest.
set -euo pipefail

OUT="${1:-sbom.cdx.json}"
IMAGE="${2:-pgllens:latest}"

if command -v syft >/dev/null 2>&1; then
  echo "Generating SBOM for ${IMAGE} with syft -> ${OUT}"
  syft "${IMAGE}" -o cyclonedx-json="${OUT}"
  exit 0
fi

echo "syft not found; falling back to the locked Python dependency set." >&2
echo "Install syft (https://github.com/anchore/syft) for a full image SBOM." >&2
uv export --frozen --no-dev --no-emit-project --format requirements-txt \
  -o "${OUT%.json}.requirements.txt"
echo "Wrote ${OUT%.json}.requirements.txt (hash-pinned dependency list)."
