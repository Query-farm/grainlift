<!--
Copyright (c) 2026 ADBC Drivers Contributors
Copyright (c) 2026 Query Farm LLC
SPDX-License-Identifier: Apache-2.0
-->

# Grainlift ADBC gateway

The Grainlift ADBC gateway exposes a database through the standard ADBC client
API. Its PyPI distribution, `grainlift-adbc-gateway`, contains the Rust server executable and depends on Apache's SQLite
ADBC wheel. Python locates that library and launches the native server; query
execution stays in Rust and the downstream driver.
On Unix the launcher replaces itself with the server. On Windows it waits for
the native child and lets console Ctrl-C initiate the server's graceful
shutdown; service managers that forcibly stop it must terminate the process tree.

This is not the [`grainlift`](https://pypi.org/project/grainlift/) package on
PyPI, which is the Python toolkit for writing your own Grainlift workers. Clients
of either connect through the [`adbc-driver-grainlift`](python-driver.md) driver.

## Serve a SQLite file

Run it straight from PyPI with uv:

```console
uvx grainlift-adbc-gateway serve sqlite ./database.sqlite
```

To create a database intentionally:

```console
uvx grainlift-adbc-gateway serve sqlite ./database.sqlite --create
```

The service listens on `127.0.0.1:8080` and exposes target `sqlite`. It creates
`.grainlift-token` in the current directory with a random 256-bit bearer token,
or reuses that file on subsequent starts. Tokens are never printed. On Unix,
new token files use mode 0600 and existing files must have no group/other access.
On Windows, restrict the containing directory's ACL to the service account.
Existing token files must be regular files, not symlinks, at most 4096 bytes,
and contain at least 32 non-whitespace ASCII characters (a final newline is OK).
Keep token files out of source control.

```console
uvx grainlift-adbc-gateway serve sqlite ./database.sqlite \
  --listen 127.0.0.1:9400 --token-file ./private/service-token
```

Database filenames are treated as filesystem paths, not arbitrary SQLite URIs.
Existing files are opened with `mode=rw`; missing files require `--create`.
The parent directories for database and token files must already exist.
SQLite's ordinary transaction and writer-locking constraints still apply.
This exposes SQL access under the server account; it is not a SQL or filesystem
sandbox. Only grant the token to callers trusted to use that access.

Clients use the native Grainlift ADBC driver with:

- `grainlift.uri`: `grainlift+http://127.0.0.1:8080`
- `grainlift.target`: `sqlite`
- `grainlift.auth.bearer_token`: the contents of the token file, stripped of its newline

Clients cannot override the configured database URI. The shorthand only allows
loopback HTTP; use the existing server configuration for remote deployments,
TLS termination, mTLS, Iroh, multiple targets, JWT authentication, and quotas.

## Serve an ADBC connection profile

In an existing server configuration, a target can select a server-side profile:

```toml
[targets.analytics]
profile = "reporting"
allowed_client_connection_options = ["adbc.connection.autocommit"]
```

The profile's `driver` and `[Options]` supply the database configuration. Use a
profile name found by the ADBC driver manager, or an absolute profile path.
Set any environment variables referenced by the profile in the server process.
Do not set `driver` alongside `profile` on the target. Keep your existing
authentication settings and grant access to `analytics` through
`auth.target_permissions`.

```console
grainlift-adbc-gateway serve --config grainlift.toml
```

See [connection profiles](../README.md#reuse-an-adbc-connection-profile) for a
complete profile example, option precedence, search paths, and reload behavior.
Configuration checks validate the target declaration without reading the
profile or connecting to its database.

## Create an Iroh identity

The Rust CLI creates identities for both servers and clients:

```console
grainlift-adbc-gateway identity create alice.key > alice.id
grainlift-adbc-gateway identity show alice.key
```

`create` writes a new 32-byte Ed25519 private key as 64 hexadecimal characters
and a newline. It prints only the derived public endpoint ID to stdout;
redirection above saves that shareable ID in `alice.id`. `show` derives the same
ID from an existing key, including keys created by the earlier Python helper.
These commands do not read server configuration, load a database driver, or
start a listener. The native `grainlift-server` accepts the same commands.
They are also available without installing through
`uvx grainlift-adbc-gateway identity create alice.key`.

Creation refuses to overwrite an existing file or symlink. It writes and syncs
a private temporary file before publishing the completed key without replacing
the destination, and removes the temporary file if publication fails. The
parent directory must exist. Unix key files have mode 0600; on Windows use a
directory whose ACL is restricted to the identity owner. Keep the key file
private and reuse it to retain a stable endpoint identity. Only share the
public ID. `show` accepts regular files up to 256 bytes and rejects symlinks
and group/other permissions on Unix. Neither command prints private keys.

For a server, use the key path as `iroh.secret_key_file`. For a client, use
`grainlift.iroh.secret_key_file` in the ADBC driver options and authorize its
public ID in the server's `iroh.principals` map. The file is read on the client
machine when opening an ADBC connection; neither its path nor contents are
forwarded as downstream database options. Prefer absolute paths. The file must
be regular, not a symlink, at most 256 bytes, and private on Unix (0600 or more
restrictive). An inline `grainlift.iroh.secret_key` remains supported, but
specifying both forms is an error. Existing connections retain their loaded
identity; a changed file takes effect on newly opened ADBC connections.

For a shared target that should accept any verified Iroh key, use
`public_targets = ["sqlite"]` under `[iroh]` instead of registering each client.
The server derives a distinct principal from each unlisted key and limits it
to those targets. Named mappings remain optional for additional privileges.
Keep the server's existing HTTP authentication configuration. Public targets
grant downstream database access to any verified Iroh peer, including writes
when the target allows them. See [security](security.md) for the full policy.

## Connect from DuckDB

DuckDB 2.0 and Haybarn 2.0 can load the native Grainlift ADBC client through the
published [`adbc_scanner` extension](https://query.farm/products/extensions/adbc_scanner).
The separate [Python client wheel](python-driver.md) can supply the native
library path through `adbc_driver_grainlift.driver_path()` after installation.
The connection path is DuckDB → ADBC scanner → Grainlift client driver →
Grainlift server → SQLite ADBC driver. The server wheel contains the server;
the client also needs the separate Grainlift ADBC shared library.

Start the service as above, or from a locally built platform wheel:

```console
uvx --from /path/to/grainlift_adbc_gateway-0.4.0-py3-none-<platform>.whl grainlift-adbc-gateway \
  serve sqlite ./database.sqlite --create
```

In DuckDB, use an absolute client-library path for your platform (`.dylib` on
macOS, `.so` on Linux, or `.dll` on Windows) and the server's token-file path:

```sql
SET allow_extension_repositories = 'allowed';
INSTALL adbc_scanner FROM query_farm;
LOAD adbc_scanner FROM query_farm;

SET VARIABLE grainlift_token = (
    SELECT trim(content, chr(10) || chr(13))
    FROM read_text('/absolute/path/to/.grainlift-token')
);
CREATE SECRET grainlift_client (
    TYPE adbc,
    DRIVER '/absolute/path/to/libadbc_driver_grainlift.dylib',
    URI 'grainlift+http://127.0.0.1:8080',
    EXTRA_OPTIONS MAP {
        'grainlift.target': 'sqlite',
        'grainlift.auth.bearer_token': getvariable('grainlift_token')
    }
);
ATTACH '' AS gl (TYPE adbc, SECRET 'grainlift_client');

SELECT * FROM adbc_scan('gl', 'SELECT 42 AS answer', columns := {'answer': 'BIGINT'});
SELECT * FROM adbc_tables('gl');
DETACH gl;
```

Haybarn's signed extension catalog uses `INSTALL adbc_scanner FROM community;
LOAD adbc_scanner;` instead of the three repository statements above.
DuckDB's `query_farm` repository verifies extension signatures; unsigned loading
is not needed. Use extension builds matching the engine version.

Pass string driver options in a secret's `EXTRA_OPTIONS` map, or typed options
directly to `ATTACH`. The extension uses the attachment alias in its functions;
the old `adbc_connect`/`adbc_disconnect` handle API has been removed. Use
`BEGIN`, `COMMIT`, and `ROLLBACK` for transactions.
The token is read from the file instead of embedded in SQL history. The SQL
string given to `adbc_scan` runs on the server; its result can be joined to local
DuckDB tables or files. Use `CALL adbc_execute(...)` for server-side DDL and DML;
SQLite cannot describe arbitrary queries through ADBC, so `adbc_scan` needs the
explicit `columns` declaration shown above; `adbc_scan_table` can use table
metadata. `EXPLAIN CALL adbc_execute(...)` does not execute the write.

The earlier handle-based recipe was validated on EC2 Linux AArch64 with DuckDB
1.5.5, community extension `7a21dda`,
and the CLI wheel from commit `4b3ac43`: a 10,000-row scan, local join, table
discovery, insert, persisted-write check after shutdown, and the token-file SQL
example all passed. This historical check used the published extension without
rebuilding DuckDB or the extension.

Further lock-contention tests found a bug in published extension `7a21dda`:
`EXPLAIN SELECT adbc_execute(...)` executed a write, and a forced SQLite lock
timeout took approximately three times the configured busy timeout. Disabling
the DuckDB optimizer prevented the `EXPLAIN` write and restored the expected
timeout. The DuckDB 2.0 recipe uses the newer attachment API and does not need
that workaround. The updated contention harness verifies rollback/retry,
connection reuse, and `EXPLAIN` without writes with the optimizer enabled.

## Configuration and persistent installation

```console
uv tool install grainlift-adbc-gateway
grainlift-adbc-gateway serve --config grainlift.toml
grainlift-adbc-gateway check --config grainlift.toml
```

`check` validates configuration and policy; it does not load drivers, connect to
databases, or start listeners. `GRAINLIFT_CONFIG` and `GRAINLIFT_SERVER_ID` work
as before. `--config`/`GRAINLIFT_CONFIG` cannot be combined with `serve sqlite`.
Malformed configuration diagnostics report a byte offset when available and
omit source text and key names because these can contain credentials.
For configuration-based targets, use installed driver names or explicit native
library paths as documented in the main README.

The standalone `grainlift-server` executable accepts the same commands. Its
existing `grainlift-server --config grainlift.toml` invocation is unchanged.
For SQLite shorthand outside the Python package, install the driver with
`dbc install sqlite --level user`, or supply `--driver /path/to/library`.
`GRAINLIFT_SQLITE_DRIVER` is the equivalent environment option. An explicit
`--driver` takes precedence over the environment and packaged driver.

## Build and validate a wheel

The PyPI package description comes from
[`packaging/gateway/README.md`](../packaging/gateway/README.md); this guide
contains the detailed CLI reference.

Building from source requires Rust 1.97 or newer. End users of a supported
platform wheel do not need a Rust toolchain.

```console
uv build --wheel --out-dir dist
uvx --from ./dist/grainlift_adbc_gateway-0.4.0-py3-none-<platform>.whl grainlift-adbc-gateway --help
```

Use the actual wheel filename. Without `--from`, `uvx grainlift-adbc-gateway`
resolves the published PyPI package.
CI builds platform wheels, installs each wheel in an isolated environment,
and tests the CLI. Publishing requires a configured PyPI trusted publisher and
the explicit release workflow; building wheels does not publish them.
