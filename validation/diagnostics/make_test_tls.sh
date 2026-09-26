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
directory=${1:?Pass a new private certificate directory}
test ! -e "$directory"
umask 077
mkdir -p "$directory"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -subj "/CN=Grainlift Benchmark CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -keyout "$directory/ca-key.pem" -out "$directory/ca.pem" >/dev/null 2>&1
for name in server client other; do
  if [[ $name == server ]]; then
    san="DNS:localhost,IP:127.0.0.1"
    usage="serverAuth"
  else
    san="URI:spiffe://benchmark.test/$name"
    usage="clientAuth,serverAuth"
  fi
  openssl req -new -newkey rsa:2048 -nodes -subj "/CN=$name" \
    -addext "subjectAltName=$san" -addext "basicConstraints=critical,CA:FALSE" \
    -addext "keyUsage=critical,digitalSignature,keyEncipherment" \
    -addext "extendedKeyUsage=$usage" \
    -keyout "$directory/$name-key.pem" -out "$directory/$name.csr" >/dev/null 2>&1
  openssl x509 -req -days 1 -sha256 -copy_extensions copy \
    -in "$directory/$name.csr" -CA "$directory/ca.pem" -CAkey "$directory/ca-key.pem" \
    -CAcreateserial -out "$directory/$name.pem" >/dev/null 2>&1
done
