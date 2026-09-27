# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Ephemeral endpoint identities for native Grainlift raw-Iroh validation."""

from dataclasses import dataclass, field

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey


@dataclass(frozen=True, slots=True)
class IrohIdentity:
    """An Ed25519 endpoint identity generated for one test fixture.

    Attributes:
        public_id: Canonical lowercase hexadecimal public EndpointId.
        secret_key: Hexadecimal private seed, excluded from diagnostic repr.
    """

    public_id: str
    secret_key: str = field(repr=False)

    def native_options(self, direct_address: str) -> dict[str, str]:
        """Build native-driver options for a discovered direct QUIC address.

        Args:
            direct_address: A socket address from bridge discovery output.

        Returns:
            Native ADBC database options; this mapping contains the private seed.
        """
        return {
            "grainlift.iroh.secret_key": self.secret_key,
            "grainlift.iroh.direct_address": direct_address,
        }


def generate_identity() -> IrohIdentity:
    """Generate an independent client identity without persisting its key.

    Returns:
        A fresh identity suitable for an authorized or unauthorized test client.
    """
    private = Ed25519PrivateKey.generate()
    secret = private.private_bytes(
        serialization.Encoding.Raw,
        serialization.PrivateFormat.Raw,
        serialization.NoEncryption(),
    )
    public = private.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    return IrohIdentity(public.hex(), secret.hex())
