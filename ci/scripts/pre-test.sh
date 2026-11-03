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

set -euo pipefail

service_name=${3:-}
if [[ -z "$service_name" ]]; then
  exit 0
fi

export PATH="$HOME/.local/bin:$PATH"
if ! command -v dbc >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -fsSL https://dbc.columnar.tech/install.sh | bash -s -- --version v0.3.0
fi

case "$service_name" in
  sqlite)
    driver="sqlite=1.12.0"
    ;;
  duckdb)
    driver="duckdb=2.0.0"
    ;;
  postgresql)
    driver="postgresql=1.12.0"
    container_name="grainlift-foundry-${GITHUB_RUN_ID:-$$}"
    container_id=$(docker run --detach --rm \
      --name "$container_name" \
      --env POSTGRES_DB=postgres \
      --env POSTGRES_PASSWORD=postgres \
      --env POSTGRES_USER=postgres \
      --publish 127.0.0.1::5432 \
      postgres:14)
    printf '%s\n' "$container_id" >.grainlift-validation-container
    for _ in {1..60}; do
      if docker exec "$container_id" pg_isready --username postgres >/dev/null 2>&1; then
        break
      fi
      sleep 1
    done
    docker exec "$container_id" pg_isready --username postgres >/dev/null
    port_mapping=$(docker port "$container_id" 5432/tcp)
    export ADBC_POSTGRESQL_URI="postgresql://postgres:postgres@127.0.0.1:${port_mapping##*:}/postgres"
    ;;
  *)
    echo "Unsupported Foundry validation service: $service_name" >&2
    exit 2
    ;;
esac

dbc install "$driver" --level user

cargo build --locked --release --workspace

GRAINLIFT_SKIP_BUILD=1 \
  GRAINLIFT_BACKEND="$service_name" \
  GRAINLIFT_ENV_FILE="$PWD/.env.override" \
  ./validation/run_external.sh serve "$service_name" &
runner_pid=$!
printf '%s\n' "$runner_pid" >.grainlift-validation.pid

for _ in {1..300}; do
  if [[ -s .env.override ]]; then
    exit 0
  fi
  if ! kill -0 "$runner_pid" 2>/dev/null; then
    wait "$runner_pid"
  fi
  sleep 0.1
done

echo "Grainlift validation service did not publish its environment" >&2
exit 1
