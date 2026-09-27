<!--
Copyright (c) 2026 ADBC Drivers Contributors
Copyright (c) 2026 Query Farm LLC
SPDX-License-Identifier: Apache-2.0
-->

# Grainlift command-line service

Grainlift exposes a database through the standard ADBC client API. Its PyPI
distribution contains the Rust server executable and depends on Apache's SQLite
ADBC wheel. Python locates that library and launches the native server; query
execution stays in Rust and the downstream driver.
On Unix the launcher replaces itself with the server. On Windows it waits for
the native child and lets console Ctrl-C initiate the server's graceful
shutdown; service managers that forcibly stop it must terminate the process tree.

## Serve a SQLite file

After the platform wheels have been published to PyPI:

```console
uvx grainlift serve sqlite ./database.sqlite
```

To create a database intentionally:

```console
uvx grainlift serve sqlite ./database.sqlite --create
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
uvx grainlift serve sqlite ./database.sqlite \
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

## Configuration and persistent installation

```console
uv tool install grainlift
grainlift serve --config grainlift.toml
grainlift check --config grainlift.toml
```

`check` validates configuration and policy; it does not load drivers, connect to
databases, or start listeners. `GRAINLIFT_CONFIG` and `GRAINLIFT_SERVER_ID` work
as before. `--config`/`GRAINLIFT_CONFIG` cannot be combined with `serve sqlite`.
For configuration-based targets, use installed driver names or explicit native
library paths as documented in the main README.

The standalone `grainlift-server` executable accepts the same commands. Its
existing `grainlift-server --config grainlift.toml` invocation is unchanged.
For SQLite shorthand outside the Python package, install the driver with
`dbc install sqlite --level user`, or supply `--driver /path/to/library`.
`GRAINLIFT_SQLITE_DRIVER` is the equivalent environment option. An explicit
`--driver` takes precedence over the environment and packaged driver.

## Build and validate a wheel

Building from source requires Rust 1.97 or newer. End users of a supported
platform wheel do not need a Rust toolchain.

```console
uv build --wheel --out-dir dist
uvx --from ./dist/grainlift-0.4.0-py3-none-<platform>.whl grainlift --help
```

Use the actual wheel filename. A local wheel can be used to serve a database
before publication; `uvx grainlift` resolves the published PyPI package.
CI builds platform wheels, installs each wheel in an isolated environment,
and tests the CLI. Publishing requires a configured PyPI trusted publisher and
the explicit release workflow; building wheels does not publish them.
