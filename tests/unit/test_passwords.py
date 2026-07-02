from rnsapi.auth.passwords import hash_password, verify_password


def test_hash_and_verify_roundtrip():
    h = hash_password("correct-horse-battery-staple")
    assert h.startswith("scrypt$")
    assert verify_password("correct-horse-battery-staple", h)
    assert not verify_password("wrong", h)


def test_hash_is_salted():
    a = hash_password("same")
    b = hash_password("same")
    assert a != b
    assert verify_password("same", a)
    assert verify_password("same", b)


def test_verify_rejects_malformed():
    assert not verify_password("x", "")
    assert not verify_password("x", "notascrypt")
    assert not verify_password("x", "scrypt$x$y$z$q$w")
    assert not verify_password("x", "argon2$1$1$1$aaaa$bbbb")


def test_verify_rejects_empty_password():
    h = hash_password("some")
    assert not verify_password("", h)
