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
"""Build and validate an exact, unpublished Python release candidate from wheels."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import tarfile
import tempfile
import time
import zipfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.request import urlopen
from xml.etree import ElementTree

ROOT = Path(__file__).resolve().parents[1]
PACKAGES = ("grainlift-python", "grainlift-hello-world-python")
TRANSPORT_REQUIREMENT = "vgi-rpc[http]==0.47.1"
MAX_DOWNLOAD_BYTES = 100 * 1024 * 1024


def digest(path: Path) -> str:
    """Return the SHA-256 digest of a file."""
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def forbidden_artifact_path(name: str) -> bool:
    """Identify local machine state and credential-like files excluded from releases."""
    path = Path(name)
    forbidden = {
        ".git",
        ".claude",
        ".codex",
        ".agents",
        ".hypothesis",
        ".venv",
        "venv",
        "__pycache__",
        ".mypy_cache",
        ".ruff_cache",
        ".pytest_cache",
        "credentials",
        "credentials.json",
        "secrets.json",
        "id_rsa",
        "id_ed25519",
    }
    return (
        bool(forbidden.intersection(path.parts))
        or path.name.startswith(".env")
        or path.suffix.lower()
        in {
            ".pyc",
            ".pem",
            ".key",
            ".crt",
            ".cer",
            ".p12",
            ".pfx",
        }
    )


def run(command: list[str], cwd: Path, log: Path) -> None:
    """Execute a command without inherited Python import overrides."""
    environment = os.environ.copy()
    for key in ("PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV"):
        environment.pop(key, None)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    with log.open("a") as stream:
        stream.write(f"\n{command!r}\n")
        stream.flush()
        subprocess.run(command, cwd=cwd, env=environment, stdout=stream, stderr=subprocess.STDOUT, check=True)


def provenance(source: Path) -> dict[str, Any]:
    """Record committed identity and hashes of actual package build inputs."""
    files = {}
    for name in ("pyproject.toml", "README.md", "LICENSE", "LICENSE.md", "src", "vgi_rpc"):
        candidate = source / name
        paths = candidate.rglob("*") if candidate.is_dir() else [candidate]
        for path in paths:
            relative = path.relative_to(source)
            excluded = forbidden_artifact_path(str(relative)) or any(part.startswith(".") for part in relative.parts)
            if path.is_file() and not excluded and path.suffix != ".pyc":
                files[str(path.relative_to(source))] = digest(path)
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=source, capture_output=True, text=True)
    status = subprocess.run(["git", "status", "--porcelain"], cwd=source, capture_output=True, text=True, check=True)
    return {"head": head.stdout.strip() if head.returncode == 0 else None, "dirty": bool(status.stdout), "files": files}


def build(output: Path, python: str) -> None:
    """Build wheels, lock their dependency closure, and package copied tests."""
    output.mkdir(parents=True, exist_ok=False)
    bundle = output / "bundle"
    wheels = bundle / "wheels"
    wheels.mkdir(parents=True)
    log = output / "build.log"
    build_input = bundle / "build-requirements.in"
    build_input.write_text("hatchling==1.27.0\n")
    run(
        [
            "uv",
            "pip",
            "compile",
            str(build_input),
            "--universal",
            "--python-version",
            "3.13",
            "--generate-hashes",
            "--no-header",
            "--output-file",
            str(bundle / "build-requirements.txt"),
        ],
        bundle,
        log,
    )
    inputs = {}
    for package in PACKAGES:
        source = ROOT.parent / package
        inputs[package] = provenance(source)
        run(
            [
                "uv",
                "build",
                str(source),
                "--no-sources",
                "--wheel",
                "--sdist",
                "--python",
                python,
                "--build-constraints",
                str(bundle / "build-requirements.txt"),
                "--require-hashes",
                "--out-dir",
                str(wheels),
                "--no-create-gitignore",
            ],
            bundle,
            log,
        )
        if inputs[package] != provenance(source):
            raise RuntimeError(f"Build inputs changed while building {package}; rebuild after edits finish")
    rebuilt = output / "rebuilt"
    for sdist in sorted(wheels.glob("*.tar.gz")):
        with tarfile.open(sdist) as source_archive:
            if any(forbidden_artifact_path(member.name) for member in source_archive):
                raise RuntimeError(f"Source archive includes local workspace or cache files: {sdist.name}")
        run(
            [
                "uv",
                "build",
                str(sdist),
                "--wheel",
                "--no-sources",
                "--python",
                python,
                "--build-constraints",
                str(bundle / "build-requirements.txt"),
                "--require-hashes",
                "--out-dir",
                str(rebuilt),
                "--no-create-gitignore",
            ],
            bundle,
            log,
        )
    for wheel in wheels.glob("*.whl"):
        with zipfile.ZipFile(wheel) as wheel_archive:
            if any(forbidden_artifact_path(member) for member in wheel_archive.namelist()):
                raise RuntimeError(f"Wheel includes local workspace or credential files: {wheel.name}")
        if digest(wheel) != digest(rebuilt / wheel.name):
            raise RuntimeError(f"Wheel does not reproduce from its sdist: {wheel.name}")
    requirements = [f"./wheels/{wheel.name}" for wheel in sorted(wheels.glob("*.whl"))]
    requirements.extend(
        (
            TRANSPORT_REQUIREMENT,
            "adbc-driver-manager==1.12.0",
            "pyarrow==25.0.1",
            "pytest==9.1.1",
            "psutil==7.2.2",
            "ruff==0.16.9",
            "mypy==2.3.1",
            "pyarrow-stubs==20.0.0.20260819",
            "cryptography==50.0.1",
            "granian==2.8.3",
        )
    )
    (bundle / "requirements.in").write_text("\n".join(requirements) + "\n")
    run(
        [
            "uv",
            "pip",
            "compile",
            "requirements.in",
            "--universal",
            "--python-version",
            "3.13",
            "--generate-hashes",
            "--no-header",
            "--output-file",
            "requirements.txt",
        ],
        bundle,
        log,
    )
    ignored = shutil.ignore_patterns("__pycache__", "*.pyc", ".pytest_cache", ".ruff_cache")
    for package, directory in (("grainlift-python", "toolkit"), ("grainlift-hello-world-python", "hello")):
        shutil.copytree(ROOT.parent / package / "tests", bundle / directory / "tests", ignore=ignored)
    shutil.copy(ROOT.parent / "grainlift-python/pyproject.toml", bundle / "toolkit/pyproject.toml")
    for directory in ("tests", "soak", "deployment"):
        shutil.copytree(ROOT / "validation/regression" / directory, bundle / "regression" / directory, ignore=ignored)
    shutil.copy(ROOT / "validation/regression/pyproject.toml", bundle / "regression/pyproject.toml")
    if any(forbidden_artifact_path(str(path.relative_to(bundle))) for path in bundle.rglob("*")):
        raise RuntimeError("Candidate contains local workspace or credential files")
    manifest = {
        "schema_version": 1,
        "built_at_unix": time.time(),
        "sources": inputs,
        "uv_version": subprocess.check_output(["uv", "--version"], text=True).strip(),
        "sdist_wheels_identical": True,
        "source_archives_clean": True,
        "suites": ["toolkit", "hello", "regression"],
        "transport": {"source": "package-index", "requirement": TRANSPORT_REQUIREMENT},
        "files": {str(path.relative_to(bundle)): digest(path) for path in sorted(bundle.rglob("*")) if path.is_file()},
    }
    (bundle / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    archive = output / "grainlift-python-candidate.tar.gz"
    with tarfile.open(archive, "w:gz") as stream:
        stream.add(bundle, arcname="bundle")
    (output / "SHA256SUMS").write_text(f"{digest(archive)}  {archive.name}\n")
    print(f"Built {archive}; SHA-256 {digest(archive)}")


def verify(bundle: Path) -> None:
    """Check every manifest entry and reject paths escaping the bundle."""
    manifest = json.loads((bundle / "manifest.json").read_text())
    for relative, expected in manifest["files"].items():
        path = (bundle / relative).resolve()
        if not path.is_relative_to(bundle.resolve()) or digest(path) != expected:
            raise ValueError(f"Invalid release artifact: {relative}")


def validation_suites(manifest: dict[str, Any]) -> tuple[str, ...]:
    """Require the full application gate while accepting historical transport suites."""
    requested = manifest.get("suites", ["transport", "toolkit", "hello", "regression"])
    if not isinstance(requested, list) or any(not isinstance(suite, str) for suite in requested):
        raise ValueError("Release validation suites must be a list of names")
    required = {"toolkit", "hello", "regression"}
    selected = set(requested)
    if not required.issubset(selected) or selected - (required | {"transport"}) or len(selected) != len(requested):
        raise ValueError("Release validation requires each application suite exactly once")
    return tuple(requested)


@contextmanager
def native_driver_environment(driver: Path) -> Iterator[None]:
    """Supply both native-suite driver variables and restore the caller's values."""
    names = ("GRAINLIFT_DRIVER", "GRAINLIFT_NATIVE_DRIVER")
    previous = {name: os.environ.get(name) for name in names}
    try:
        for name in names:
            os.environ[name] = str(driver)
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def check(bundle: Path, python: str, driver: Path, evidence: Path) -> None:
    """Install hashed wheels into a fresh environment and run native tests."""
    verify(bundle)
    manifest = json.loads((bundle / "manifest.json").read_text())
    selected_suites = validation_suites(manifest)
    driver = driver.resolve(strict=True)
    evidence.mkdir(parents=True, exist_ok=False)
    log = evidence / "validation.log"
    with tempfile.TemporaryDirectory(prefix="grainlift-wheel-validation-") as temporary:
        work = Path(temporary)
        shutil.copytree(bundle, work / "candidate")
        candidate = work / "candidate"
        run(["uv", "venv", "--python", python, str(work / "venv")], work, log)
        executable = work / "venv/bin/python"
        run(
            [
                "uv",
                "pip",
                "install",
                "--python",
                str(executable),
                "--require-hashes",
                "--only-binary",
                ":all:",
                "-r",
                "requirements.txt",
            ],
            candidate,
            log,
        )
        run(["uv", "pip", "check", "--python", str(executable)], candidate, log)
        run(
            [
                str(executable),
                "-I",
                "-c",
                "import grainlift, grainlift_hello_world, vgi_rpc, pathlib, sys; "
                "modules=[grainlift,grainlift_hello_world,vgi_rpc]; "
                "assert all(pathlib.Path(m.__file__).is_relative_to(sys.prefix) for m in modules); "
                "print({m.__name__:m.__file__ for m in modules})",
            ],
            work,
            log,
        )
        installed = subprocess.check_output(["uv", "pip", "freeze", "--python", str(executable)], text=True)
        (evidence / "installed.txt").write_text(installed)
        toolkit = candidate / "toolkit"
        sdk_wheels = list((candidate / "wheels").glob("grainlift_python-*.whl"))
        if len(sdk_wheels) != 1:
            raise ValueError("Candidate must contain exactly one toolkit wheel")
        with zipfile.ZipFile(sdk_wheels[0]) as archive:
            for member in archive.namelist():
                if member.startswith("grainlift/") and not member.endswith("/"):
                    path = (toolkit / "src" / member).resolve()
                    if not path.is_relative_to((toolkit / "src").resolve()):
                        raise ValueError("Invalid toolkit wheel path")
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(archive.read(member))
        run([str(executable), "-m", "ruff", "check", "src", "tests"], toolkit, log)
        run([str(executable), "-m", "ruff", "format", "--check", "src", "tests"], toolkit, log)
        run([str(executable), "-m", "mypy", "src", "tests"], toolkit, log)
        run(
            [
                "uvx",
                "--python",
                str(executable),
                "--from",
                "pydoclint==0.9.1",
                "pydoclint",
                "--config",
                "pyproject.toml",
                "src",
                "tests",
            ],
            toolkit,
            log,
        )
        suites = {}
        with native_driver_environment(driver):
            # Older immutable candidates include a locally built transport and
            # its tests. New candidates exercise the published dependency through
            # the SDK and native suites instead of rebuilding upstream VGI-RPC.
            for suite in selected_suites:
                junit = evidence / f"{suite}.xml"
                run(
                    [str(executable), "-m", "pytest", "tests", "-q", "-p", "no:cacheprovider", f"--junitxml={junit}"],
                    candidate / suite,
                    log,
                )
                nodes = ElementTree.parse(junit).getroot().findall("testsuite")
                counts = {
                    key: sum(int(node.attrib[key]) for node in nodes)
                    for key in ("tests", "failures", "errors", "skipped")
                }
                if counts["skipped"]:
                    raise RuntimeError(f"Release validation must not skip tests: {suite}")
                suites[suite] = counts
        summary = {
            "platform": platform.platform(),
            "python": subprocess.check_output([str(executable), "--version"], text=True).strip(),
            "manifest_sha256": digest(bundle / "manifest.json"),
            "driver_sha256": digest(driver),
            "suites": suites,
            "source_imports": False,
            "toolkit_quality": ["ruff", "ruff-format", "strict-mypy", "isolated-pydoclint"],
            "transport": manifest.get("transport", {"source": "bundled-wheel"}),
        }
        (evidence / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps(summary, indent=2))


def fetch(url: str, sha256: str, output: Path) -> None:
    """Download a size-limited candidate and verify it before extraction."""
    if not url.startswith("https://") or len(sha256) != 64 or any(c not in "0123456789abcdef" for c in sha256):
        raise ValueError("A HTTPS URL and lowercase SHA-256 digest are required")
    output.mkdir(parents=True, exist_ok=False)
    archive = output / "candidate.tar.gz"
    with urlopen(url, timeout=30) as response, archive.open("wb") as stream:
        total = 0
        while block := response.read(1024 * 1024):
            total += len(block)
            if total > MAX_DOWNLOAD_BYTES:
                raise ValueError("Candidate exceeds the 100 MiB download limit")
            stream.write(block)
    if digest(archive) != sha256:
        raise ValueError("Candidate SHA-256 mismatch")
    with tarfile.open(archive) as stream:
        members = []
        expanded_size = 0
        for member in stream:
            members.append(member)
            expanded_size += member.size
            if len(members) > 10000 or expanded_size > 500 * 1024 * 1024:
                raise ValueError("Candidate exceeds extraction limits")
            if not (member.isfile() or member.isdir()):
                raise ValueError("Candidate must contain only regular files and directories")
        stream.extractall(output, filter="data")
    verify(output / "bundle")


def main() -> None:
    """Run the selected release-candidate preparation or validation gate."""
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    builder = subparsers.add_parser("build")
    builder.add_argument("--output", type=Path, required=True)
    builder.add_argument("--python", default="3.13")
    checker = subparsers.add_parser("check")
    checker.add_argument("--bundle", type=Path, required=True)
    checker.add_argument("--python", default="3.13")
    checker.add_argument("--driver", type=Path, required=True)
    checker.add_argument("--evidence", type=Path, required=True)
    downloader = subparsers.add_parser("fetch")
    downloader.add_argument("--url", required=True)
    downloader.add_argument("--sha256", required=True)
    downloader.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "build":
        build(args.output.resolve(), args.python)
    elif args.command == "check":
        check(args.bundle.resolve(), args.python, args.driver, args.evidence.resolve())
    else:
        fetch(args.url, args.sha256, args.output.resolve())


if __name__ == "__main__":
    main()
