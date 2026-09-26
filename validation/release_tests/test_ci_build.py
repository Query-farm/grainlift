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

"""Check architecture-specific packaging setup without contacting a registry."""

import os
import subprocess
from pathlib import Path

import pytest

HOOK = Path(__file__).resolve().parents[2] / "ci/scripts/pre-build.sh"


@pytest.mark.parametrize("architecture", ["amd64", "arm64"])
def test_release_compose_fallback_is_single_platform(tmp_path: Path, architecture: str) -> None:
    """Preserve configuration and generate a safely quoted single-platform override."""
    tools = tmp_path / "tools"
    tools.mkdir()
    dev = tmp_path / "dev with spaces"
    dev.mkdir()
    (dev / "compose.yaml").write_text("services: {}\n")
    pixi = tools / "pixi"
    pixi.write_text('#!/bin/sh\nprintf "%s\\n" "$GRAINLIFT_TEST_DEV_ROOT"\n')
    pixi.chmod(0o755)
    (tmp_path / ".env.build").write_text("export EXISTING_SETTING=preserved\n")
    env = {
        **os.environ,
        "GITHUB_ACTIONS": "true",
        "RUNNER_TEMP": str(tmp_path),
        "GRAINLIFT_TEST_DEV_ROOT": str(dev),
        "PATH": str(tools) + os.pathsep + os.environ["PATH"],
    }
    subprocess.run(["bash", str(HOOK), "release", "linux", architecture], cwd=tmp_path, env=env, check=True)
    result = subprocess.run(
        ["bash", "-c", 'source .env.build; printf "%s\\n" "$EXISTING_SETTING" "$RUST" "$COMPOSE_FILE"'],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    existing, rust, compose = result.stdout.splitlines()
    assert existing == "preserved"
    assert rust == "1.97.1"
    original, override = compose.split(":")
    assert original == str(dev / "compose.yaml")
    image_architecture = "x86_64" if architecture == "amd64" else "aarch64"
    assert Path(override).read_text() == (
        "services:\n  manylinux-rust:\n    build:\n      args:\n"
        f"        MANYLINUX: manylinux2014_{image_architecture}\n"
        f"      platforms: !override\n        - linux/{architecture}\n"
    )


@pytest.mark.parametrize(
    "configuration,platform,ci",
    [("test", "linux", "true"), ("release", "macos", "true"), ("release", "linux", "false")],
)
def test_non_container_builds_leave_configuration_alone(
    tmp_path: Path,
    configuration: str,
    platform: str,
    ci: str,
) -> None:
    """Avoid modifying user environment files or native/debug build paths."""
    subprocess.run(
        ["bash", str(HOOK), configuration, platform, "arm64"],
        cwd=tmp_path,
        env={**os.environ, "GITHUB_ACTIONS": ci},
        check=True,
    )
    assert not (tmp_path / ".env.build").exists()
