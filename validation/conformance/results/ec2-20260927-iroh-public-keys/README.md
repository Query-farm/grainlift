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

# Public Iroh keys — 2026-09-27

`result.json` records `validation/iroh_disconnect.py --public-target` on the
requested EC2 host, using the debug server built with the public-target policy.
The helper's entire `[iroh.principals]` allowlist was removed and replaced by
`iroh.public_targets = ["sqlite"]`. HTTP credentials and named target
permissions stayed in place. No client code or wire protocol was changed.

Three separate DuckDB processes connected: Alice, Bob, and another connection
using Alice's key. Bob observed Alice's committed data. After SIGKILL during
Alice's next write transaction, Bob recovered write access in 35.055 seconds;
the uncommitted write was rolled back and the second same-key connection
remained usable. Session TTL was 3600 seconds, so lease expiry could not make
the 120-second test pass. The server survived and shut down cleanly.
Expected SQLite busy errors are included in the result. This is a functional
crash test, not a load benchmark or a recovery-time guarantee.

Environment: Linux `6.18.41-94.142.amzn2023.aarch64`, Rust 1.97.1, Python 3.13,
DuckDB 1.5.5, ADBC driver manager/SQLite 1.12.0. Direct Iroh, relays disabled,
SQLite WAL mode. Client, extension, and helper artifacts match the
[earlier disconnect run](../ec2-20260927-iroh-disconnect/README.md).
The server binary used here had SHA-256
`d307dfaf92db4e210e778d45b60f1ccf42ca75d2a712a1ebd45cf8a928f30dfc`.

Additional Rust coverage checks cross-key session and handle isolation,
per-key quotas, rejection of private targets even with permissive general
authorization, named-principal/key namespace collisions, verified-evidence
requirements, default allowlist rejection, unchanged non-Iroh grants, and
disconnect/shutdown cleanup. See the
[reproduction instructions](../../../../docs/iroh-disconnect-validation.md).
