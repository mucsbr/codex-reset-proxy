from __future__ import annotations

import ipaddress
import re
import ssl
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from codex_reset_proxy.config import Settings

CA_KEY_NAME = "ca.key"
CA_CERT_NAME = "ca.crt"


def ensure_ca_certificate(settings: Settings) -> tuple[Path, Path]:
    cert_dir = Path(settings.mitm_cert_dir)
    cert_dir.mkdir(parents=True, exist_ok=True)

    key_path = cert_dir / CA_KEY_NAME
    cert_path = cert_dir / CA_CERT_NAME
    if key_path.exists() and cert_path.exists():
        return key_path, cert_path

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name(
        [
            x509.NameAttribute(NameOID.COMMON_NAME, "codex-reset-proxy local MITM CA"),
        ]
    )
    now = datetime.now(timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_cert_sign=True,
                crl_sign=True,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                content_commitment=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
        .sign(key, hashes.SHA256())
    )

    key_path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    return key_path, cert_path


def server_ssl_context_for_host(settings: Settings, host: str) -> ssl.SSLContext:
    cert_path, key_path = ensure_leaf_certificate(settings, host)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.set_alpn_protocols(["http/1.1"])
    context.load_cert_chain(certfile=cert_path, keyfile=key_path)
    return context


def ensure_leaf_certificate(settings: Settings, host: str) -> tuple[Path, Path]:
    ca_key_path, ca_cert_path = ensure_ca_certificate(settings)
    safe_host = _safe_filename(host)
    leaf_dir = Path(settings.mitm_cert_dir) / "leaf"
    leaf_dir.mkdir(parents=True, exist_ok=True)

    cert_path = leaf_dir / f"{safe_host}.crt"
    key_path = leaf_dir / f"{safe_host}.key"
    if cert_path.exists() and key_path.exists():
        return cert_path, key_path

    ca_key = serialization.load_pem_private_key(ca_key_path.read_bytes(), password=None)
    ca_cert = x509.load_pem_x509_certificate(ca_cert_path.read_bytes())
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    subject = x509.Name(
        [
            x509.NameAttribute(NameOID.COMMON_NAME, host),
        ]
    )
    now = datetime.now(timezone.utc)
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=825))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_encipherment=True,
                key_cert_sign=False,
                crl_sign=False,
                data_encipherment=False,
                key_agreement=False,
                content_commitment=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .add_extension(_subject_alt_name(host), critical=False)
    )
    cert = builder.sign(ca_key, hashes.SHA256())

    key_path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    return cert_path, key_path


def _subject_alt_name(host: str) -> x509.SubjectAlternativeName:
    try:
        return x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address(host))])
    except ValueError:
        return x509.SubjectAlternativeName([x509.DNSName(host)])


def _safe_filename(host: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", host)[:180] or "unknown"
