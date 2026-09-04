import httpx
import pytest

from pgllens.oauth.okta import JwksCache, JwksError
from tests.jwt_helpers import rsa_keypair


class Serve:
    """A JWKS endpoint that counts requests and can be made to fail or change."""

    def __init__(self, document, cache_control="max-age=300"):
        self.document = document
        self.cache_control = cache_control
        self.status = 200
        self.calls = 0

    def transport(self):
        def handler(request):
            self.calls += 1
            if self.status != 200:
                return httpx.Response(self.status, json={"error": "boom"})
            headers = {"cache-control": self.cache_control} if self.cache_control else {}
            return httpx.Response(200, json=self.document, headers=headers)

        return httpx.MockTransport(handler)

    def cache(self, **kw):
        client = httpx.AsyncClient(transport=self.transport())
        return JwksCache("https://test.okta.com/oauth2/aus1/v1/keys", client=client, **kw)


async def test_prime_fetches_the_document_once():
    _key, doc = rsa_keypair()
    serve = Serve(doc)
    cache = serve.cache()
    await cache.prime()
    assert serve.calls == 1
    assert await cache.key_for("test-key-1") is not None
    assert serve.calls == 1  # served from cache, no second fetch


async def test_prime_raises_when_jwks_is_unreachable():
    # Fail closed at boot: a server that cannot verify anything must not start
    # and pretend to be protected.
    _key, doc = rsa_keypair()
    serve = Serve(doc)
    serve.status = 503
    with pytest.raises(JwksError):
        await serve.cache().prime()


async def test_unknown_kid_triggers_exactly_one_refresh():
    _key, doc = rsa_keypair()
    serve = Serve(doc)
    cache = serve.cache()
    await cache.prime()
    with pytest.raises(JwksError):
        await cache.key_for("rotated-key")
    assert serve.calls == 2  # primed once, refreshed once for the unknown kid


async def test_refresh_is_rate_limited_against_forged_kids():
    # Without this, an attacker sends 10k tokens with random kid values and
    # PgLLens turns into a DoS amplifier pointed at the client's Okta tenant.
    _key, doc = rsa_keypair()
    serve = Serve(doc)
    clock = [1000.0]
    cache = serve.cache(min_refresh_interval_s=60.0, now=lambda: clock[0])
    await cache.prime()
    for _ in range(50):
        with pytest.raises(JwksError):
            await cache.key_for("forged-kid")
    assert serve.calls == 2  # prime + one refresh, not 51

    clock[0] += 61.0
    with pytest.raises(JwksError):
        await cache.key_for("forged-kid")
    assert serve.calls == 3  # the window elapsed, one more refresh is allowed


async def test_a_rotated_key_is_picked_up_by_the_refresh():
    _key, doc = rsa_keypair("old-key")
    serve = Serve(doc)
    cache = serve.cache()
    await cache.prime()
    _key2, doc2 = rsa_keypair("new-key")
    serve.document = doc2
    assert await cache.key_for("new-key") is not None


async def test_cache_control_max_age_drives_expiry():
    _key, doc = rsa_keypair()
    serve = Serve(doc, cache_control="max-age=120")
    clock = [1000.0]
    cache = serve.cache(min_refresh_interval_s=0.0, now=lambda: clock[0])
    await cache.prime()
    clock[0] += 119.0
    await cache.key_for("test-key-1")
    assert serve.calls == 1  # still inside max-age
    clock[0] += 2.0
    await cache.key_for("test-key-1")
    assert serve.calls == 2  # max-age elapsed, refetched


async def test_missing_cache_control_falls_back_to_the_default_ttl():
    _key, doc = rsa_keypair()
    serve = Serve(doc, cache_control=None)
    clock = [1000.0]
    cache = serve.cache(min_refresh_interval_s=0.0, now=lambda: clock[0])
    await cache.prime()
    clock[0] += 301.0
    await cache.key_for("test-key-1")
    assert serve.calls == 2


async def test_empty_key_set_is_an_error_not_an_empty_cache():
    # A 200 with {"keys": []} must not silently install a cache that rejects
    # every token with "unknown kid" -- that reads as an attack, not an outage.
    serve = Serve({"keys": []})
    with pytest.raises(JwksError):
        await serve.cache().prime()


async def test_corrupt_rsa_key_material_raises_jwks_error():
    # Valid JSON, kty=RSA, kid present -- but n/e are not valid base64url RSA
    # material, so RSAAlgorithm.from_jwk raises. That must surface as
    # JwksError (-> TokenError -> 401), never escape as an unhandled 500.
    serve = Serve({"keys": [
        {"kty": "RSA", "kid": "bad", "n": "!!!not-base64!!!", "e": "AQAB"},
    ]})
    cache = serve.cache()
    with pytest.raises(JwksError):
        await cache.prime()
