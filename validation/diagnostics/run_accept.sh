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
evidence=${2:?Pass a new evidence directory}
driver=${3:?Pass the unchanged native driver shared library}
before=${4:?Pass the baseline Rust synthetic server}
after=${5:?Pass the readiness Rust synthetic server}
certificates=${6:?Pass the private test certificate directory}
mkdir -p "$evidence"
test ! -e "$evidence/stages.tsv"
cd "$repo/validation/regression"
run_case() {
  local mode=$1 label=$2 binary=$3 repetition=$4
  local status=0
  local name="$mode-$label-$repetition"
  local -a command
  if [[ $mode == cold ]]; then
    command=(soak.cold_connections --queries 100)
  else
    command=(soak.matched --host rust --transport mtls --queries 500)
  fi
  printf '%s\n' "$name" > "$evidence/current-stage.txt"
  timeout --kill-after=30 300 .venv/bin/python -m "${command[@]}" \
    --driver "$driver" --rust-server "$binary" --tls-dir "$certificates" \
    --warmup 10 --output "$evidence/$name.json" > "$evidence/$name.log" 2>&1 || status=$?
  printf '%s\t%s\n' "$name" "$status" >> "$evidence/stages.tsv"
  return "$status"
}
for repetition in 1 2 3; do
  for mode in cold warm; do
    if [[ $repetition == 2 ]]; then
      run_case "$mode" after "$after" "$repetition"
      run_case "$mode" before "$before" "$repetition"
    else
      run_case "$mode" before "$before" "$repetition"
      run_case "$mode" after "$after" "$repetition"
    fi
  done
done
printf '%s\n' complete > "$evidence/current-stage.txt"
