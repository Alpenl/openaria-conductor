"""Fast in-process TLS certificate and Ed25519 keypair generators via cryptography."""

from __future__ import annotations

import ipaddress
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, ed25519
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID


def write_self_signed_tls(
    cert_path: Path,
    key_path: Path,
    *,
    common_name: str = "localhost",
    dns_names: Sequence[str] = (),
    ip_addresses: Sequence[str] = ("127.0.0.1",),
    cert_mode: int = 0o644,
    key_mode: int = 0o600,
) -> tuple[Path, Path]:
    """Generate a self-signed ECDSA P-256 certificate and PKCS8 private key in PEM format."""
    cert_path.parent.mkdir(parents=True, exist_ok=True)
    key_path.parent.mkdir(parents=True, exist_ok=True)
    key = ec.generate_private_key(ec.SECP256R1())
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    now = datetime.now(UTC)
    san_entries: list[x509.GeneralName] = [x509.DNSName(name) for name in dns_names]
    san_entries.extend(x509.IPAddress(ipaddress.ip_address(addr)) for addr in ip_addresses)
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]),
            critical=False,
        )
    )
    if san_entries:
        builder = builder.add_extension(x509.SubjectAlternativeName(san_entries), critical=False)
    certificate = builder.sign(private_key=key, algorithm=hashes.SHA256())

    key_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    cert_pem = certificate.public_bytes(serialization.Encoding.PEM)
    key_path.write_bytes(key_pem)
    key_path.chmod(key_mode)
    cert_path.write_bytes(cert_pem)
    cert_path.chmod(cert_mode)
    return cert_path, key_path


def write_ed25519_keypair(
    private_key_path: Path,
    public_key_path: Path,
    *,
    private_mode: int = 0o600,
    public_mode: int = 0o644,
) -> tuple[Path, Path]:
    """Generate an Ed25519 keypair in PEM format without invoking openssl."""
    private_key_path.parent.mkdir(parents=True, exist_ok=True)
    public_key_path.parent.mkdir(parents=True, exist_ok=True)
    private_key = ed25519.Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    private_key_path.write_bytes(private_pem)
    private_key_path.chmod(private_mode)
    public_key_path.write_bytes(public_pem)
    public_key_path.chmod(public_mode)
    return private_key_path, public_key_path


__all__ = [
    "write_ed25519_keypair",
    "write_self_signed_tls",
]
