import base64
import hashlib

from pgllens.oauth import crypto


def test_new_token_is_urlsafe_and_unique():
    a, b = crypto.new_token(), crypto.new_token()
    assert a != b
    assert len(a) >= 32
    assert all(c.isalnum() or c in "-_" for c in a)


def test_constant_time_equals_matches_and_rejects():
    assert crypto.constant_time_equals("secret", "secret") is True
    assert crypto.constant_time_equals("secret", "Secret") is False
    assert crypto.constant_time_equals("secret", "") is False
    assert crypto.constant_time_equals("", "") is True


def test_verify_pkce_s256_roundtrip():
    verifier = "a" * 64
    digest = hashlib.sha256(verifier.encode()).digest()
    challenge = base64.urlsafe_b64encode(digest).decode().rstrip("=")
    assert crypto.verify_pkce(verifier, challenge, "S256") is True
    assert crypto.verify_pkce("wrong", challenge, "S256") is False


def test_verify_pkce_rejects_unknown_method():
    assert crypto.verify_pkce("v", "v", "MD5") is False
    assert crypto.verify_pkce("v", "v", "") is False


def test_verify_pkce_plain_roundtrip():
    assert crypto.verify_pkce("same-value", "same-value", "plain") is True
    assert crypto.verify_pkce("a", "b", "plain") is False


def test_verify_pkce_empty_verifier_or_challenge_never_true():
    assert crypto.verify_pkce("", "", "plain") is False
    assert crypto.verify_pkce("", "", "S256") is False
    assert crypto.verify_pkce("v", "", "S256") is False
    assert crypto.verify_pkce("", "c", "S256") is False


def test_verify_pkce_non_ascii_verifier_returns_false_not_raises():
    # RFC 7636 verifiers are ASCII unreserved chars only; a non-ASCII verifier
    # is invalid and must return False, never raise (would be a 500 on the
    # token endpoint otherwise).
    assert crypto.verify_pkce("ü" * 64, "anything", "S256") is False
