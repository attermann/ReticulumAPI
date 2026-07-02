import ssl

from cryptography import x509

from rnsapi import tls


def test_ensure_self_signed_creates_files(tmp_path):
    paths = tls.ensure_self_signed(tmp_path / "certs", "localhost")
    assert paths.cert.exists()
    assert paths.key.exists()
    assert paths.cert.stat().st_size > 0
    assert paths.key.stat().st_size > 0
    # key must be owner-only readable
    assert oct(paths.key.stat().st_mode)[-3:] == "600"


def test_ensure_self_signed_reuses_existing(tmp_path):
    first = tls.ensure_self_signed(tmp_path / "certs", "localhost")
    cert_bytes = first.cert.read_bytes()
    second = tls.ensure_self_signed(tmp_path / "certs", "localhost")
    assert second.cert.read_bytes() == cert_bytes  # not regenerated


def test_cert_has_san_for_localhost_and_127001(tmp_path):
    paths = tls.ensure_self_signed(tmp_path / "certs", "localhost")
    cert = x509.load_pem_x509_certificate(paths.cert.read_bytes())
    san = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    dns_names = san.get_values_for_type(x509.DNSName)
    ip_names = [str(ip) for ip in san.get_values_for_type(x509.IPAddress)]
    assert "localhost" in dns_names
    assert "127.0.0.1" in ip_names


def test_build_ssl_context_loads_cert(tmp_path):
    paths = tls.ensure_self_signed(tmp_path / "certs", "localhost")
    ctx = tls.build_ssl_context(paths.cert, paths.key)
    assert isinstance(ctx, ssl.SSLContext)
    assert ctx.minimum_version == ssl.TLSVersion.TLSv1_2


def test_fingerprint_is_hex_pairs(tmp_path):
    paths = tls.ensure_self_signed(tmp_path / "certs", "localhost")
    fp = tls.cert_fingerprint_sha256(paths.cert)
    parts = fp.split(":")
    assert len(parts) == 32  # 32 bytes
    for p in parts:
        assert len(p) == 2
        int(p, 16)
