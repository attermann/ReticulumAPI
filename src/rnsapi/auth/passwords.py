"""Password hashing for the login credentials stored in config.

Uses scrypt from the stdlib so no extra dependencies are required. The encoded
form is `scrypt$N$r$p$salt_hex$hash_hex` and is safe to store in the INI file.
"""
from __future__ import annotations

import hashlib
import os
import secrets


_N = 2 ** 14  # ~16k iterations; reasonable for interactive login
_R = 8
_P = 1
_SALT_BYTES = 16
_KEY_LEN = 32


def hash_password(password: str) -> str:
    salt = os.urandom(_SALT_BYTES)
    key = hashlib.scrypt(password.encode("utf-8"), salt=salt, n=_N, r=_R, p=_P, dklen=_KEY_LEN)
    return f"scrypt${_N}${_R}${_P}${salt.hex()}${key.hex()}"


def verify_password(password: str, encoded: str) -> bool:
    if not encoded or not password:
        return False
    try:
        scheme, n_s, r_s, p_s, salt_hex, hash_hex = encoded.split("$")
    except ValueError:
        return False
    if scheme != "scrypt":
        return False
    try:
        n = int(n_s)
        r = int(r_s)
        p = int(p_s)
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(hash_hex)
    except ValueError:
        return False
    actual = hashlib.scrypt(password.encode("utf-8"), salt=salt, n=n, r=r, p=p, dklen=len(expected))
    return secrets.compare_digest(expected, actual)
