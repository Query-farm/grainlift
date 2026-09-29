<!--
  Copyright (c) 2026 ADBC Drivers Contributors
  Copyright (c) 2026 Query Farm LLC

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# URI-derived scopes and Iroh key files — 2026-09-27

`result.json` records a successful EC2 run of
`validation/iroh_disconnect.py --public-target` with the updated
`adbc_scanner/test/iroh_sqlite_contention.py` helper. Each DuckDB client creates
one named ADBC secret containing `URI` and `grainlift.iroh.secret_key_file`,
without `SCOPE` or inline private key material. The helper asserts the inferred
scope equals the URI, then uses the secret for its command handle and separate
read-only attached catalog.

Alice and Bob observed committed changes. Alice was killed mid-transaction;
Bob recovered write access in 34.991 seconds, rollback was confirmed, and a
second connection using Alice's key survived. The server survived and shut
down cleanly. Session TTL remained 3600 seconds. Expected SQLite busy errors
are recorded during QUIC peer-loss detection. This is a functional regression,
not a load benchmark or recovery-time guarantee.

Linux ARM64, Rust 1.97.1, Python 3.13, DuckDB 1.5.5, ADBC driver manager/SQLite
1.12.0, direct Iroh with relays disabled, SQLite WAL mode. The server matches
the [public-key run](../ec2-20260927-iroh-public-keys/README.md).
Artifacts used here (SHA-256):

| Artifact | SHA-256 |
| --- | --- |
| Grainlift debug client | `435ec912f11c0e9aa1daf18e199aef73dcdd97f45ea4254dbb1966360301c403` |
| ADBC Scanner extension | `af581be903d21bc8f0303c618fb8eca32d1eebe9a7a3ebc9774ecbed58a4d209` |
| Contention helper | `08b93dd3f31835ec039e3f0ce9a63c7e0318c465d3c2015476e89445979f1deb` |

Grainlift: 97 workspace tests passed, Clippy passed. ADBC Scanner: the two
secret SQL suites passed 40 assertions, and all 26 Python regression tests
passed. Coverage includes inferred/explicit scopes, real SQLite automatic
secret lookup, rejected empty/missing URI defaults, key-file limits,
permissions, symlinks, conflicting key sources, redacted errors, and preventing
identity options from being forwarded to the downstream database.
