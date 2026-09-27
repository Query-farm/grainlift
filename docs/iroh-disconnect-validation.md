# Iroh disconnect cleanup regression

`validation/iroh_disconnect.py` checks an abandoned SQLite write transaction
through DuckDB and the `adbc_scanner` extension. It runs three separate client
processes: Alice, Bob, and another connection using Alice's identity. The test
commits a value, leaves a second write uncommitted, and kills the first Alice
process with SIGKILL. Bob must regain write access, observe rollback of the
uncommitted write, and preserve the committed data. The other Alice connection
must remain usable. The server must survive and shut down cleanly afterwards.

The server's idle-session timeout stays at 3600 seconds with a 30-second reaper
interval. The test has a 120-second recovery deadline, so TTL expiry cannot
make it pass. Silent crashes still require QUIC peer-loss detection; this is
not a zero-latency cleanup guarantee. The database, identities, configuration,
and listener are temporary and separate from any running service. Relays are
disabled for this local-network regression.

Use a Python environment containing DuckDB and cryptography, a built
`adbc_scanner` extension matching that DuckDB version, the Grainlift ADBC shared
library, and the SQLite ADBC shared library:

```sh
python validation/iroh_disconnect.py \
  --server target/debug/grainlift-server \
  --driver /path/to/libadbc_driver_grainlift.so \
  --extension /path/to/adbc_scanner.duckdb_extension \
  --sqlite-driver /path/to/libadbc_driver_sqlite.so \
  --harness ../adbc_scanner/test/iroh_sqlite_contention.py \
  --output /tmp/iroh-disconnect.json
```

The helper comes from the sibling `adbc_scanner` repository and supplies the
independent DuckDB processes and temporary Iroh configuration. Output contains
timings and classified errors, without raw driver error messages, credentials,
or endpoint keys. Expected SQLite busy errors are recorded until cleanup
releases the abandoned write lock.

Rust regression tests additionally cover individual stream closure, immediate
explicit physical disconnect, statement/result/connection cleanup, listener
shutdown, first-stream timeout, transport task cancellation, and the race
between downstream connection creation and peer disconnection. Run the
Grainlift workspace suite and the upstream `vgi-rpc-iroh` suite for this coverage.

The transport lifecycle hook currently comes from pinned upstream revision
`93cc4b8974494b32da460df1e66f64afdbbf119f`
([upstream pull request](https://github.com/Query-farm/vgi-rpc-rust/pull/5)).
The workspace patches pin its shared Rust dependency graph together; no local
checkout is required. Replace these patches with the transport's crates.io
release before publishing Grainlift crates.
