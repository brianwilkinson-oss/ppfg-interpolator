#!/usr/bin/env bash
set -euo pipefail

ROOT="$(pwd)"
BINARY_NAME="${BINARY_NAME:-corva}"
PYINSTALLER_CMD=(uv run pyinstaller)

DATASET_FILE="${ROOT}/docs/dataset.json"
GROUPS_FILE="${ROOT}/groups/generated_groups.json"

if [[ ! -f "${DATASET_FILE}" ]]; then
  echo "Missing ${DATASET_FILE}. Regenerate docs/dataset.json before building." >&2
  exit 1
fi

if [[ ! -f "${GROUPS_FILE}" ]]; then
  mkdir -p "$(dirname "${GROUPS_FILE}")"
  echo '{"groups":[]}' > "${GROUPS_FILE}"
fi

case "$(uname -s)" in
  *CYGWIN*|*MINGW*|*MSYS*|Windows_NT)
    SEP=";"
    ;;
  *)
    SEP=":"
    ;;
esac

DATA_ARGS=(
  "--add-data" "${DATASET_FILE}${SEP}docs"
  "--add-data" "${GROUPS_FILE}${SEP}groups"
)

TARGET="${ROOT}/src/corva_cli/__main__.py"

echo "Syncing dependencies via uv..."
uv sync

echo "Building ${BINARY_NAME}..."
"${PYINSTALLER_CMD[@]}" --onefile --name "${BINARY_NAME}" "${TARGET}" "${DATA_ARGS[@]}"

echo "Binary created at ${ROOT}/dist/${BINARY_NAME}"
