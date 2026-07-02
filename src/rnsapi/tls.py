"""Self-signed cert generation and SSLContext construction for rnsapid."""
from __future__ import annotations

import datetime
import ipaddress
import ssl
from dataclasses import dataclass
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


@dataclass
class CertPaths:
    cert: Path
    key: Path


def ensure_self_signed(certs_dir: Path, common_name: str) -> CertPaths:
    """Return (cert, key) paths under *certs_dir*, generating them if missing.

    Uses RSA-2048 (broad client compatibility including macOS LibreSSL) and
    10-year validity.
    """
    certs_dir.mkdir(parents=True, exist_ok=True)
    cert_path = certs_dir / "rnsapid.crt"
    key_path = certs_dir / "rnsapid.key"
    if cert_path.exists() and key_path.exists():
        return CertPaths(cert=cert_path, key=key_path)

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, common_name),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "ReticulumAPI"),
    ])
    now = datetime.datetime.now(datetime.timezone.utc)
    san = [x509.DNSName(common_name)]
    for candidate in ("localhost",):
        if candidate != common_name:
            san.append(x509.DNSName(candidate))
    san.extend([x509.IPAddress(ipaddress.ip_address("127.0.0.1")), x509.IPAddress(ipaddress.ip_address("::1"))])

    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=3650))
        .add_extension(x509.SubjectAlternativeName(san), critical=False)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .sign(private_key, algorithm=hashes.SHA256())
    )

    key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    key_path.write_bytes(key_bytes)
    key_path.chmod(0o600)
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    return CertPaths(cert=cert_path, key=key_path)


def build_ssl_context(cert_path: Path, key_path: Path) -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_cert_chain(certfile=str(cert_path), keyfile=str(key_path))
    return ctx


def cert_fingerprint_sha256(cert_path: Path) -> str:
    data = cert_path.read_bytes()
    cert = x509.load_pem_x509_certificate(data)
    fp = cert.fingerprint(hashes.SHA256())
    return ":".join(f"{b:02X}" for b in fp)
