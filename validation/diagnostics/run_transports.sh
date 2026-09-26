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
server=${4:?Pass the release Rust synthetic server}
certificates=${5:?Pass the private test certificate directory}
mkdir -p "$evidence"
test ! -e "$evidence/stages.tsv"
cd "$repo/validation/regression"
run_case() {
  local label=$1 host=$2 transport=$3 batch=${4:-512}
  local status=0
  printf '%s\n' "$label" > "$evidence/current-stage.txt"
  timeout --kill-after=30 300 .venv/bin/python -m soak.matched \
    --host "$host" --transport "$transport" --tls-dir "$certificates" \
    --driver "$driver" --rust-server "$server" --queries 1000 --warmup 10 \
    --batch-rows "$batch" --output "$evidence/$label.json" \
    > "$evidence/$label.log" 2>&1 || status=$?
  printf '%s\t%s\n' "$label" "$status" >> "$evidence/stages.tsv"
  if [[ $status != 0 ]]; then return "$status"; fi
}
run_case rust-http-1 rust http
run_case python-http-1 python-direct http
run_case rust-mtls-1 rust mtls
run_case python-mtls-1 python-direct mtls
run_case python-mtls-2 python-direct mtls
run_case rust-mtls-2 rust mtls
run_case python-http-2 python-direct http
run_case rust-http-2 rust http
run_case python-http-3 python-direct http
run_case python-mtls-3 python-direct mtls
run_case rust-http-3 rust http
run_case rust-mtls-3 rust mtls
run_case rust-http-one-batch rust http 4096
run_case python-http-one-batch python-direct http 4096
run_case rust-mtls-one-batch rust mtls 4096
run_case python-mtls-one-batch python-direct mtls 4096
printf '%s\n' complete > "$evidence/current-stage.txt"
