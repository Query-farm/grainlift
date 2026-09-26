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
repo=${1:?Pass the absolute Grainlift checkout}
evidence=${2:?Pass an empty evidence directory}
driver=${3:?Pass the unchanged native driver shared library}
server=${4:?Pass the release Rust synthetic server}
mkdir -p "$evidence"
test ! -e "$evidence/stages.tsv"
cd "$repo/validation/regression"

run_case() {
  local label=$1 host=$2 batch=$3
  local status=0
  printf '%s\n' "$label" > "$evidence/current-stage.txt"
  timeout --kill-after=30 300 .venv/bin/python -m soak.matched \
    --host "$host" --driver "$driver" --rust-server "$server" \
    --queries 1000 --warmup 10 --batch-rows "$batch" --output "$evidence/$label.json" \
    > "$evidence/$label.log" 2>&1 || status=$?
  printf '%s\t%s\n' "$label" "$status" >> "$evidence/stages.tsv"
  if [[ $status != 0 ]]; then return "$status"; fi
}
# Rotate host order to reduce the chance of confusing drift with implementation.
run_case rust-1 rust 512
run_case python-direct-1 python-direct 512
run_case python-isolated-1 python-isolated 512
run_case python-isolated-2 python-isolated 512
run_case rust-2 rust 512
run_case python-direct-2 python-direct 512
run_case python-direct-3 python-direct 512
run_case python-isolated-3 python-isolated 512
run_case rust-3 rust 512
run_case rust-one-batch rust 4096
run_case python-direct-one-batch python-direct 4096
printf '%s\n' complete > "$evidence/current-stage.txt"
