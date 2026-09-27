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

"""Reject tampering and unsafe archives before executing release candidates."""

from __future__ import annotations

import hashlib
import io
import json
import os
import tarfile
from pathlib import Path

import pytest

from validation import release_bundle


@pytest.mark.parametrize("fails", [False, True])
def test_native_suite_environment_is_restored(monkeypatch: pytest.MonkeyPatch, fails: bool) -> None:
    """All suites receive the candidate driver without leaking it to later work."""
    monkeypatch.setenv("GRAINLIFT_DRIVER", "original-driver")
    monkeypatch.delenv("GRAINLIFT_NATIVE_DRIVER", raising=False)
    monkeypatch.setenv("GRAINLIFT_MATCHED_DRIVER", "original-matched-driver")
    driver = Path("/candidate/driver.so")
    try:
        with release_bundle.native_driver_environment(driver):
            assert os.environ["GRAINLIFT_DRIVER"] == str(driver)
            assert os.environ["GRAINLIFT_NATIVE_DRIVER"] == str(driver)
            assert os.environ["GRAINLIFT_MATCHED_DRIVER"] == str(driver)
            if fails:
                raise RuntimeError("suite failed")
    except RuntimeError:
        assert fails
    assert os.environ["GRAINLIFT_DRIVER"] == "original-driver"
    assert "GRAINLIFT_NATIVE_DRIVER" not in os.environ
    assert os.environ["GRAINLIFT_MATCHED_DRIVER"] == "original-matched-driver"


def _archive(extra: tarfile.TarInfo | None = None) -> bytes:
    payload = b"reviewed wheel candidate"
    manifest = json.dumps({"files": {"payload": hashlib.sha256(payload).hexdigest()}}).encode()
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w:gz") as archive:
        for path, content in (("bundle/payload", payload), ("bundle/manifest.json", manifest)):
            entry = tarfile.TarInfo(path)
            entry.size = len(content)
            archive.addfile(entry, io.BytesIO(content))
        if extra is not None:
            archive.addfile(extra)
    return stream.getvalue()


def _serve(monkeypatch: pytest.MonkeyPatch, content: bytes) -> None:
    monkeypatch.setattr(release_bundle, "urlopen", lambda *args, **kwargs: io.BytesIO(content))


@pytest.mark.parametrize("size", [4095, 4096, 4097])
def test_download_boundary(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, size: int) -> None:
    """Allow the exact download budget and reject one byte beyond it."""
    monkeypatch.setattr(release_bundle, "MAX_DOWNLOAD_BYTES", 4096)
    content = _archive().ljust(size, b"\x00")
    _serve(monkeypatch, content)
    output = tmp_path / "candidate"
    if size > 4096:
        with pytest.raises(ValueError, match="download limit"):
            release_bundle.fetch("https://example.invalid/candidate", hashlib.sha256(content).hexdigest(), output)
        assert not (output / "bundle").exists()
    else:
        release_bundle.fetch("https://example.invalid/candidate", hashlib.sha256(content).hexdigest(), output)
        assert (output / "bundle/payload").read_text() == "reviewed wheel candidate"


def test_archive_hash_is_checked_before_extraction(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A different valid archive cannot replace the reviewed artifact."""
    _serve(monkeypatch, _archive())
    output = tmp_path / "candidate"
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        release_bundle.fetch("https://example.invalid/candidate", "0" * 64, output)
    assert not (output / "bundle").exists()


@pytest.mark.parametrize("entry_kind", ["symlink", "traversal"])
def test_unsafe_archive_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, entry_kind: str) -> None:
    """Even a hash-approved malformed archive cannot write outside its directory."""
    entry = tarfile.TarInfo("../escaped" if entry_kind == "traversal" else "bundle/link")
    if entry_kind == "symlink":
        entry.type = tarfile.SYMTYPE
        entry.linkname = "../../escaped"
    content = _archive(entry)
    _serve(monkeypatch, content)
    with pytest.raises((ValueError, tarfile.FilterError)):
        release_bundle.fetch(
            "https://example.invalid/candidate", hashlib.sha256(content).hexdigest(), tmp_path / "candidate"
        )
    assert not (tmp_path / "escaped").exists()


def test_manifest_detects_tampering(tmp_path: Path) -> None:
    """The local directory check detects content changed after archive extraction."""
    payload = tmp_path / "payload"
    payload.write_text("reviewed")
    (tmp_path / "manifest.json").write_text(json.dumps({"files": {"payload": release_bundle.digest(payload)}}))
    release_bundle.verify(tmp_path)
    payload.write_text("modified")
    with pytest.raises(ValueError, match="Invalid release artifact"):
        release_bundle.verify(tmp_path)


def test_manifest_cannot_reference_outside_files(tmp_path: Path) -> None:
    """Manifest paths are validated before reading an external file."""
    (tmp_path / "manifest.json").write_text(json.dumps({"files": {"../outside": "0" * 64}}))
    with pytest.raises(ValueError, match="Invalid release artifact"):
        release_bundle.verify(tmp_path)


@pytest.mark.parametrize(
    "name",
    [
        "package/.claude/worktrees/agent/.git",
        "package/.hypothesis/examples/cached",
        "package/.venv/bin/python",
        "package/src/grainlift/__pycache__/api.pyc",
        "package/src/grainlift/.env.production",
        "package/tests/server.pem",
        "package/tests/client.key",
        "package/config/credentials.json",
    ],
)
def test_local_machine_files_are_forbidden(name: str) -> None:
    """Protect archives against the ignored-worktree packaging regression."""
    assert release_bundle.forbidden_artifact_path(name)


@pytest.mark.parametrize("name", ["grainlift/credentials.py", "vgi_rpc/access_log.schema.json", "grainlift/py.typed"])
def test_application_code_and_package_data_are_allowed(name: str) -> None:
    """Credential-management source and required package data remain distributable."""
    assert not release_bundle.forbidden_artifact_path(name)


def test_historical_candidate_requires_all_original_suites() -> None:
    """Absent suite metadata retains the historical transport plus full application gate."""
    assert release_bundle.validation_suites({}) == ("transport", "toolkit", "hello", "regression")


def test_stock_transport_candidate_keeps_all_application_suites() -> None:
    """Published transport dependencies remove only the separately bundled transport tests."""
    suites = ["toolkit", "hello", "regression"]
    assert release_bundle.validation_suites({"suites": suites}) == tuple(suites)


@pytest.mark.parametrize(
    "suites",
    [
        [],
        ["hello", "regression"],
        ["toolkit", "regression"],
        ["toolkit", "hello"],
        ["toolkit", "hello", "regression", "regression"],
        ["toolkit", "hello", "regression", "unknown"],
        ["toolkit", "hello", "regression", "../other"],
        ["toolkit", "hello", "regression", 1],
        ["toolkit", "hello", "regression", []],
        None,
        "toolkit,hello,regression",
        {"toolkit": True, "hello": True, "regression": True},
    ],
)
def test_malformed_suite_selection_cannot_bypass_release_gate(suites: object) -> None:
    """Missing, duplicated, unknown, or wrongly typed suite lists cannot yield partial success."""
    with pytest.raises(ValueError, match="Release validation"):
        release_bundle.validation_suites({"suites": suites})
