<!-- Copyright (c) 2026 ADBC Drivers Contributors; Query Farm LLC. SPDX-License-Identifier: Apache-2.0 -->

# Candidate v7: supported Python hosts and published Rust transport

The [immutable candidate](https://github.com/Query-farm/grainlift/releases/tag/python-candidate-v7)
contains protocol 0.4 SDK and example wheels, reproduced byte-for-byte from their
source distributions, with hash-locked dependencies and copied regression tests.
Python uses stock registry VGI-RPC 0.47.1. Native builds use published VGI-RPC
0.27.3, including readiness-based TCP/mTLS acceptance, without local Cargo patches.

Archive SHA-256:
`1a2fc4e154f4549b5027290fe9555afd23f207b23d27e52d0731b381cae106ab`.
Manifest SHA-256:
`563cc9c67779cb1420a4ec3b921dfcbe7e87d739c47776698f33cb10df24ee1e`.

Source identities:

- SDK: `05e84a5b75cde760cd0f6554fe4c372671bd80df`.
- Python example: `b59c71d8553860065560ab9da3675fa5c19681d3`.
- Native driver: Grainlift `afe1fd0b774e759830603d4d29ddac87575f108f`.
- Rust example: `4aca4a0c6b7ee98fd616303c568f13af5c2cd0a0`, pinned to that Grainlift revision.
- Upstream transport release: `v0.27.3`, commit `e60c342c2ae86730abe7a863f79cbd11f2f89910`.

All builds and tests in this record ran on the authorized EC2 Linux ARM64 host,
using Rust 1.97.1. The installed-wheel gate clears source import overrides and
checks imports from the fresh virtual environment. Ruff, formatting, strict mypy,
and isolated pydoclint validate the installed SDK contents.

| Suite | Python 3.13.15 | Python 3.14.7 |
| --- | ---: | ---: |
| SDK | 512 passed | 512 passed |
| Python example | 14 passed | 14 passed |
| Native regression | 172 passed | 172 passed |
| Total | 698 passed | 698 passed |

Both integrated runs passed without failures or skips. Each summary records the
same native driver SHA-256, `da57c346a300ad6296c14d12f814e37c06d4cf1ce505ffce601c9f9b5c861e29`,
and Rust example SHA-256, `fc01f3c00c73ec68697f05a23565a9907580e0888850306e849091322af0eb83`.
JUnit, installed versions and summaries are retained in `evidence313/` and
`evidence314/`. `baseline/` separately records the same 698 tests passing with
the previous native binaries; it does not establish transport performance.

The Rust workspace passed 70 tests, formatting, strict Clippy and a release
build. Real SQLite C-ABI smoke checks passed over HTTP, TCP, mTLS and Iroh
using the release server and driver. The Rust example passed five tests, formatting, strict Clippy and a
release build. Release-harness integrity/environment tests passed all 40 cases.
The regression Python quality gates, shell syntax/Shellcheck and Python compile
checks also passed. An initial manual mypy invocation omitted the workflow's
import-path setting; the corrected invocation used the pinned CI environment.

The original CI failures exposed fixture deadlines that mixed worker startup,
session expiry and request timeouts, plus undeclared Windows system DLLs used by
Iroh network discovery. Updated tests retain the dedicated startup/idle limits
and deliberately exercise slow startup before request timeout. The new candidate
also exposed missing native-suite environment variables and optional hosting
dependencies: the skip-failing gate caught these before qualification. All
native suites now receive the same driver, and CI supplies a pinned Rust server
and freshly generated private certificates. No certificates are committed or
uploaded with evidence. The Rust example declares its own workspace so nested
CI checkouts build correctly.

The upstream [release](https://github.com/Query-farm/vgi-rpc-rust/actions/runs/36282052362),
Grainlift [native CI](https://github.com/Query-farm/grainlift/actions/runs/36282913098),
and Rust example [CI](https://github.com/Query-farm/grainlift-rust-hello-world/actions/runs/36283046661)
passed. The [combined wheel matrix](https://github.com/Query-farm/grainlift/actions/workflows/python-regression.yml)
and [packaging matrix](https://github.com/Query-farm/grainlift/actions/workflows/script_test.yaml)
are required remote gates; inspect the run for the release revision separately
from this EC2 evidence.

This candidate does not publish the SDK/example to PyPI or qualify hours-long
production stability. Real deployment quotas, certificate renewal, session
affinity and sustained downstream workloads remain rollout gates in the
[readiness record](../../../docs/python-release-readiness.md).
