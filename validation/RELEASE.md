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

# Python release candidate gate

[Candidate v6 evidence](release-results/candidate-v6/README.md) records 609 passing
tests on each of Python 3.13.12 and 3.14.7 against the matching protocol 0.4 native
driver. Both local wheels reproduce from their source distributions, using
unmodified registry VGI-RPC 0.47.1. Its
[prerelease](https://github.com/Query-farm/grainlift/releases/tag/python-candidate-v6)
is published and configured for the
[combined runtime matrix](https://github.com/Query-farm/grainlift/actions/runs/36246917438);
the quality job and all four runtime jobs passed. Remote
combined runtime results are separate from local evidence.

[Historical candidate v4 evidence](release-results/candidate-v4/README.md) records 440 passing
tests on each of Python 3.13.12 and 3.14.7, including the expanded operation surface.
Its [GitHub prerelease](https://github.com/Query-farm/grainlift/releases/tag/python-candidate-v4)
and [combined CI run](https://github.com/Query-farm/grainlift/actions/runs/36221096021)
identify the exact published protocol 0.2 candidate. Its quality job and all four runtime jobs
passed. The SDK and hello-world installed-wheel matrices also passed on
Linux/macOS and Python 3.13/3.14.

Protocol 0.4 requires candidate v6: the older artifacts are not
wire-compatible with the current native driver. Their recorded results remain
historical evidence, not validation of the typed-response migration.

[Historical candidate v2 evidence](release-results/candidate-v2/README.md) records two
successful fresh-environment runs: 240 tests on each of Python 3.13.12 and
3.14.7, plus SDK quality gates. These runs used macOS arm64; candidate v2 later
passed the [Linux/macOS runtime matrix](https://github.com/Query-farm/grainlift/actions/runs/36218130106).
That historical candidate covers the query-only SDK. The expanded ADBC operation
surface must be built and validated as a new candidate.

`release_bundle.py` builds the Python toolkit and hello-world example as wheels
and source distributions. Published VGI-RPC 0.47.1 is resolved from the package
index with pinned hashes, without a custom transport wheel. It rebuilds every local wheel
from its source distribution and requires byte-identical results. The build
backend and its dependencies have a hash-locked constraints file. The bundle
contains the exact wheels, a universal hash-locked dependency closure, copied
tests, and a source-provenance manifest. Local path overrides in `uv.sources`
are disabled for builds.

Each package has an explicit source-distribution allowlist and excludes agent
worktrees, Git metadata, virtual environments, caches, dotenv files, keys,
certificates, and credential files from both wheel and source payloads. The
builder rejects those paths in generated artifacts and copied candidate files.
This caught and removed local agent worktrees and Hypothesis caches from the
VGI-RPC source distribution during this review.

The validator installs these wheels into a new temporary environment, checks
dependency consistency and installed import locations, and runs three suites:
the entire toolkit suite, the entire hello-world suite, and the Grainlift native
ADBC regression suite. Historical bundles retain their fourth transport suite.
It clears inherited `PYTHONPATH`, `PYTHONHOME`, and `VIRTUAL_ENV`; tests run from
copied directories outside all source checkouts. Any skipped test fails the
release gate. Registry dependencies are downloaded as binary wheels with
mandatory hashes; no dependency source builds are allowed during validation.

This tests installation from built packages without source-checkout imports.
Building a new candidate requires the two Grainlift Python sibling checkouts.
The bundle does not contain third-party dependency wheels, so installing a
candidate requires access to the package registry/cache.

The same fresh environment runs the SDK's Ruff checks, formatting check, and
strict mypy against source extracted from the exact SDK wheel plus its copied
tests. Pydoclint runs in an isolated environment to avoid the known parser
dependency conflict with VGI-RPC. Quality versions are pinned in the candidate
lock; pydoclint is pinned separately to 0.9.1. Static analysis uses extracted
source, while runtime tests continue to import installed packages.

## Run locally

Use Python 3.13 or 3.14, uv 0.11.7, and the project's Rust toolchain. Stop source
edits before building; the script rejects package inputs that change during a
build. Output directories must be new to prevent stale artifacts from passing.

```sh
cargo build --locked -p adbc-driver-grainlift
cargo build --locked --manifest-path ../grainlift-rust-hello-world/Cargo.toml
python3.13 validation/release_bundle.py build \
  --output target/python-candidate --python 3.13

# Use OpenSSL 3 (including on macOS); never upload the private test keys.
bash validation/diagnostics/make_test_tls.sh target/python-candidate-tls
export GRAINLIFT_SYNTHETIC_RUST_SERVER="$(cd ../grainlift-rust-hello-world && pwd)/target/debug/grainlift-rust-hello-world"
export GRAINLIFT_MATCHED_TLS_DIR="$PWD/target/python-candidate-tls"

# On Linux, use libadbc_driver_grainlift.so instead.
python3.13 validation/release_bundle.py check \
  --bundle target/python-candidate/bundle --python 3.13 \
  --driver target/debug/libadbc_driver_grainlift.dylib \
  --evidence target/python-candidate/evidence313
python3.13 validation/release_bundle.py check \
  --bundle target/python-candidate/bundle --python 3.14 \
  --driver target/debug/libadbc_driver_grainlift.dylib \
  --evidence target/python-candidate/evidence314
```

`SHA256SUMS` identifies the candidate archive. `manifest.json` records each
wheel, lockfile, test file, package source hash, and available Git HEAD. A dirty
or unborn repository is explicitly recorded; a HEAD hash alone does not
identify these local changes. Validation evidence includes per-suite JUnit
XML, installed versions, the driver and synthetic server hashes, and the manifest hash. Full logs and
archives stay under ignored `target/`; durable summary evidence belongs in
`validation/release-results/`.

The release utility has its own tests for tampered hashes, manifest path
escapes, archive symlinks/path traversal, and downloads immediately below, at,
and above the configured limit:

```sh
uv run --no-project --python 3.13 --with pytest==9.1.1 \
  python -m pytest -o addopts= validation/release_tests -q
```

## Runtime CI

`.github/workflows/python-regression.yml` has a wheel-runtime matrix for
Python 3.13/3.14 on Linux and macOS. Windows is not claimed as a supported
deployment platform by this gate. The matrix tests the exact reviewed wheel
candidate independently of subsequent changes in the public source repositories.

The matrix instead accepts a reviewed candidate archive via a public HTTPS URL
and its SHA-256 digest. The archive hash must come from the locally reviewed
`SHA256SUMS`, not from the download site. `fetch` checks that hash before safe,
bounded extraction and verifies every manifest entry. Workflow dispatch asks
for `candidate_url` and `candidate_sha256`. Setting repository variables
`GRAINLIFT_PYTHON_CANDIDATE_URL` and `GRAINLIFT_PYTHON_CANDIDATE_SHA256` enables
the same runtime matrix on every push/PR against that pinned candidate. Source
changes in the sibling projects require building and configuring a new
candidate. The workflow rebuilds the native driver from the current Grainlift
checkout and uploads runtime evidence even if validation fails. It also builds
the synthetic Rust example from an explicit Git revision and generates private
test certificates outside the evidence directory. All three native test suites
receive the same compiled driver. Missing optional hosting dependencies or
native comparison inputs must not silently reduce coverage: skips fail the gate.

Without those variables, normal push/PR runs execute quality checks while
runtime jobs are skipped. Successful local runs are not evidence that the
Linux/macOS GitHub matrix has passed. Before release, configure this matrix,
run it, and require its checks in branch protection. The public SDK repositories
can build future candidates from immutable commits and supply the resulting
reviewed artifact.

After the exact candidate v7 archive and `SHA256SUMS` have been uploaded to the
`python-candidate-v7` prerelease in `Query-farm/grainlift`, enable automatic runs
and explicitly dispatch the workflow (the initial source push may have occurred
before the variables were configured):

```sh
candidate_url=https://github.com/Query-farm/grainlift/releases/download/python-candidate-v7/grainlift-python-candidate.tar.gz
candidate_sha256=1a2fc4e154f4549b5027290fe9555afd23f207b23d27e52d0731b381cae106ab
gh variable set GRAINLIFT_PYTHON_CANDIDATE_URL --repo Query-farm/grainlift --body "$candidate_url"
gh variable set GRAINLIFT_PYTHON_CANDIDATE_SHA256 --repo Query-farm/grainlift --body "$candidate_sha256"
gh workflow run python-regression.yml --repo Query-farm/grainlift --ref main \
  -f candidate_url="$candidate_url" -f candidate_sha256="$candidate_sha256"
```

Changing an uploaded asset cannot silently change the candidate: every runner
checks the pinned SHA-256 before extraction and installation. Retain the
reviewed checksum separately from the download site.

## Remaining publication decision

Protocol 0.4 uses published VGI-RPC `0.47.1`; it requires no new transport release.
Choose the SDK/example release versions, then rebuild and pass these exact gates
with a new protocol 0.4 candidate. Do not reuse the modified transport wheel from
historical candidates or validate the current driver against their old SDK.
Public repositories and a GitHub
prerelease do not publish these wheels to a package index or approve a
production rollout.
