# Iroh client crash regression — 2026-09-27

`result.json` records a successful run of `validation/iroh_disconnect.py` on
the EC2 host requested by the user. This is a functional crash-recovery test,
not a load benchmark or a latency guarantee.

- Linux `6.18.41-94.142.amzn2023.aarch64`, ARM64; Rust 1.97.1.
- DuckDB 1.5.5, ADBC driver manager/SQLite driver 1.12.0,
  cryptography 50.0.1; Python 3.13.
- Three independent DuckDB client processes, including two using Alice's
  identity, against one SQLite WAL database over direct Iroh (relays disabled).
- Session TTL 3600 seconds; reaper interval 30 seconds.
- Bob regained write access 35.056 seconds after Alice was killed with SIGKILL.
  Expected SQLite busy errors occurred while QUIC detected peer loss. There
  were no unexpected application errors. The uncommitted write was rolled
  back; committed data survived; the second Alice connection and server stayed
  usable. Subsequent server shutdown was clean.

The Grainlift server was a debug build of the disconnect-cleanup change on
base `4b3ac4306de16af79f86c1b2e4f97b3f487a53b6`. This initial test binary used a
temporary local Cargo patch to the same transport implementation committed as
`93cc4b8974494b32da460df1e66f64afdbbf119f`. The final workspace manifest and lock
file use that public Git revision, with no local path patches; all 84 Grainlift
workspace tests and Clippy subsequently passed against the Git pin. The
upstream full workspace passed 636 tests (four ignored) and Clippy with all
features.

The pre-existing release-mode Grainlift client was used unchanged. The helper
matches `adbc_scanner` revision `3485fb0893b624e57d572aa27e37857749a75d6a`.
SHA-256 hashes of the artifacts used for this recorded run:

| Artifact | SHA-256 |
| --- | --- |
| Grainlift server | `720dda24d7735739108c352dd39eaa974690f9ff20b94b359d9f3c05b3c10598` |
| Grainlift ADBC client | `6ed0ce017a0043e30bdf0178ebb0d0897c099e0abf03f5506b07fe2ffb52639e` |
| adbc_scanner extension | `09c2b15d1c64676dd92f904fed8e1f4dd380df1438fe960ef9551cb0d2b0bc3f` |
| adbc_scanner contention helper | `8290b452808533ad39b096dd8a719a5788014c06c28015367929040962849636` |

See [reproduction instructions](../../../../docs/iroh-disconnect-validation.md).
