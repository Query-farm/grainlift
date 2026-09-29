#!/usr/bin/env bash
# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
suite="$repo_root/validation/e2e"
e2e_python=${GRAINLIFT_VALIDATION_PYTHON:-3.13}
mode=${1:-all}
if [[ $# -gt 0 ]]; then
  shift
fi
case "$mode" in
  quality|test|all) ;;
  *) echo "usage: validation/run_e2e.sh [quality|test|all] [pytest arguments]" >&2; exit 2 ;;
esac

if [[ "$mode" != test ]]; then
  uv run --project "$suite" --python "$e2e_python" --locked ruff check "$suite"
  uv run --project "$suite" --python "$e2e_python" --locked ruff format --check "$suite"
  uv run --project "$suite" --python "$e2e_python" --locked mypy --config-file "$suite/pyproject.toml" "$suite"
  uvx --python "$e2e_python" --from pydoclint==0.9.1 pydoclint --config "$suite/pyproject.toml" "$suite"/*.py
fi
if [[ "$mode" != quality ]]; then
  : "${GRAINLIFT_SERVER:?set GRAINLIFT_SERVER to the compiled Rust server}"
  : "${GRAINLIFT_DRIVER:?set GRAINLIFT_DRIVER to the compiled Grainlift ADBC library}"
  uv run --project "$suite" --python "$e2e_python" --locked python "$suite/run.py" "$@"
fi
