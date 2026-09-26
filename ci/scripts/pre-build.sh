#!/usr/bin/env bash
# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The generated workflow invokes this hook before adbc-make. Its upstream
# Compose fallback lists two build platforms, which the default Docker builder
# cannot load. Each CI runner builds only its own requested architecture.
set -euo pipefail

configuration=${1:?expected test or release}
platform=${2:?expected target platform}
architecture=${3:?expected target architecture}

if [[ ${GITHUB_ACTIONS:-false} != true || $configuration != release || $platform != linux ]]; then
  exit 0
fi
case "$architecture" in
  amd64) image_architecture=x86_64 ;;
  arm64) image_architecture=aarch64 ;;
  *) echo "Unsupported Linux build architecture" >&2; exit 2 ;;
esac

dev_root=$(pixi run python -c 'from pathlib import Path; import adbc_drivers_dev.make_config as config; print(Path(config.__file__).parent)')
test -f "$dev_root/compose.yaml"
override=$(mktemp "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/grainlift-compose.XXXXXX")
cat > "$override" <<EOF
services:
  manylinux-rust:
    build:
      args:
        MANYLINUX: manylinux2014_$image_architecture
      platforms: !override
        - linux/$architecture
EOF

# These values are loaded by the generated workflow's next shell step. Preserve
# any existing environment file contents; neither file belongs in source control.
printf 'export RUST=%q\n' '1.97.1' >> .env.build
printf 'export COMPOSE_FILE=%q\n' "$dev_root/compose.yaml:$override" >> .env.build
