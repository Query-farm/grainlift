# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0

"""Generate isolated, authenticated transport identities for native E2E tests."""

from __future__ import annotations

import json
import socket
import subprocess
from pathlib import Path


def unused_port() -> int:
    """Select an ephemeral loopback listener port."""
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def openssl(root: Path, *arguments: str) -> None:
    """Run bounded certificate generation without exposing key material."""
    subprocess.run(["openssl", *arguments], cwd=root, check=True, capture_output=True, timeout=20)


def configure_mtls(root: Path, port: int) -> tuple[str, str, dict[str, dict[str, str]]]:
    """Create a private CA and independently authenticated server and client certificates."""
    openssl(
        root,
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-days",
        "1",
        "-subj",
        "/CN=Grainlift E2E CA",
        "-addext",
        "basicConstraints=critical,CA:TRUE",
        "-addext",
        "keyUsage=critical,keyCertSign,cRLSign",
        "-keyout",
        "ca.key",
        "-out",
        "ca.pem",
    )
    for name in ("server", "test", "other"):
        server = name == "server"
        openssl(
            root,
            "req",
            "-new",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-subj",
            f"/CN={name}",
            "-addext",
            "subjectAltName=" + ("DNS:localhost" if server else f"URI:spiffe://e2e.test/{name}"),
            "-addext",
            "basicConstraints=critical,CA:FALSE",
            "-addext",
            "keyUsage=critical,digitalSignature,keyEncipherment",
            "-addext",
            "extendedKeyUsage=" + ("serverAuth" if server else "clientAuth,serverAuth"),
            "-keyout",
            f"{name}.key",
            "-out",
            f"{name}.csr",
        )
        openssl(
            root,
            "x509",
            "-req",
            "-days",
            "1",
            "-sha256",
            "-copy_extensions",
            "copy",
            "-in",
            f"{name}.csr",
            "-CA",
            "ca.pem",
            "-CAkey",
            "ca.key",
            "-CAcreateserial",
            "-out",
            f"{name}.pem",
        )
        (root / f"{name}.key").chmod(0o600)
    (root / "ca.key").chmod(0o600)
    q = json.dumps
    config = (
        f'\n[tcp]\nlisten = "127.0.0.1:{port}"\n[tcp.tls]\n'
        f"server_certificate_chain = {q(str(root / 'server.pem'))}\n"
        f"server_private_key = {q(str(root / 'server.key'))}\n"
        f'client_ca = {q(str(root / "ca.pem"))}\ntrust_domains = ["e2e.test"]\n'
    )
    permissions = "".join(
        f"{q('peer/spiffe/spiffe%3A%2F%2Fe2e.test/spiffe%3A%2F%2Fe2e.test%2F' + name)} = [TARGET]\n"
        for name in ("test", "other")
    )
    options = {
        name: {
            "grainlift.tls.ca": str(root / "ca.pem"),
            "grainlift.tls.cert": str(root / f"{name}.pem"),
            "grainlift.tls.key": str(root / f"{name}.key"),
            "grainlift.tls.server_name": "localhost",
        }
        for name in ("test", "other")
    }
    return config, permissions, options


def configure_iroh(root: Path, server: Path) -> tuple[str, dict[str, dict[str, str]]]:
    """Use the server identity CLI to create private keys and an explicit peer allowlist."""
    identities = {}
    for name in ("server", "test", "other"):
        result = subprocess.run(
            [str(server), "identity", "create", str(root / f"{name}.key")],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        identities[name] = result.stdout.strip()
    q = json.dumps
    config = (
        '\n[iroh]\nissuer = "e2e"\ndisable_relays = true\n'
        # Drop a killed client's QUIC connection (and its sessions) after 2s
        # of silence instead of Iroh's default 30s.
        "connection_idle_timeout_seconds = 2\n"
        f"secret_key_file = {q(str(root / 'server.key'))}\n"
        f"endpoint_info_file = {q(str(root / 'endpoint.json'))}\n[iroh.principals]\n"
        + "".join(f"{q(identities[name])} = {q(name)}\n" for name in ("test", "other"))
    )
    options = {name: {"grainlift.iroh.secret_key_file": str(root / f"{name}.key")} for name in ("test", "other")}
    return config, options
