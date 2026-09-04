import time

import httpx
import jwt
import pytest

from pgllens.oauth.okta import JwksCache, JwtVerifier, TokenError
from tests.jwt_helpers import AUDIENCE, ISSUER, KID, mint, rsa_keypair


@pytest.fixture
def setup():
    private_key, doc = rsa_keypair()

    def handler(request):
        return httpx.Response(200, json=doc, headers={"cache-control": "max-age=300"})

    cache = JwksCache(
        "https://test.okta.com/oauth2/aus1/v1/keys",
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    verifier = JwtVerifier(cache, issuer=ISSUER, audience=AUDIENCE)
    return private_key, verifier


async def test_a_valid_token_verifies_and_yields_its_identity(setup):
    private_key, verifier = setup
    claims = await verifier.verify(mint(private_key))
    assert claims.sub == "00u1testuser"
    assert claims.client_id == "0oa1testclient"
    assert claims.scopes == frozenset({"pgllens.read"})


async def test_a_token_signed_by_another_key_is_rejected(setup):
    # SIGNATURE. Mutation check: remove `key` from the jwt.decode call (or pass
    # options={"verify_signature": False}) and this test must fail.
    _private_key, verifier = setup
    attacker_key, _doc = rsa_keypair()
    with pytest.raises(TokenError):
        await verifier.verify(mint(attacker_key))


async def test_a_token_from_another_issuer_is_rejected(setup):
    # ISS. Mutation check: delete `issuer=self._issuer` from jwt.decode and this
    # test must fail.
    private_key, verifier = setup
    token = mint(private_key, iss="https://evil.okta.com/oauth2/aus9")
    with pytest.raises(TokenError):
        await verifier.verify(token)


async def test_a_token_for_another_audience_in_the_same_tenant_is_rejected(setup):
    # AUD -- the confused-deputy case the whole mode exists for: same tenant,
    # same issuer, same signing key, different resource. Mutation check: delete
    # `audience=self._audience` from jwt.decode and this test must fail.
    private_key, verifier = setup
    token = mint(private_key, aud="api://some-other-service")
    with pytest.raises(TokenError):
        await verifier.verify(token)


async def test_an_expired_token_is_rejected(setup):
    # EXP. Mutation check: add options={"verify_exp": False} to jwt.decode and
    # this test must fail.
    private_key, verifier = setup
    now = int(time.time())
    token = mint(private_key, iat=now - 4000, nbf=now - 4000, exp=now - 3600)
    with pytest.raises(TokenError):
        await verifier.verify(token)


async def test_a_not_yet_valid_token_is_rejected(setup):
    # NBF. Mutation check: add options={"verify_nbf": False} to jwt.decode and
    # this test must fail. 600s is well past the 60s leeway.
    private_key, verifier = setup
    now = int(time.time())
    token = mint(private_key, nbf=now + 600, exp=now + 1200)
    with pytest.raises(TokenError):
        await verifier.verify(token)


async def test_a_token_missing_the_audience_claim_entirely_is_rejected(setup):
    # An Okta *Org* authorization server mints tokens with no custom audience.
    # Those must be refused loudly, not treated as unaudienced-and-fine.
    private_key, verifier = setup
    with pytest.raises(TokenError):
        await verifier.verify(mint(private_key, aud=None))


async def test_an_unsigned_alg_none_token_is_rejected(setup):
    # Classic algorithm-confusion probe.
    _private_key, verifier = setup
    token = jwt.encode({"iss": ISSUER, "aud": AUDIENCE, "sub": "x"}, key=None,
                       algorithm=None, headers={"kid": KID})
    with pytest.raises(TokenError):
        await verifier.verify(token)


async def test_an_hs256_token_signed_with_the_public_key_is_rejected(setup):
    # Algorithm confusion: RSA public keys are public, so an HS256 token signed
    # with the modulus must never be accepted.
    _private_key, verifier = setup
    token = jwt.encode({"iss": ISSUER, "aud": AUDIENCE, "sub": "x",
                        "exp": int(time.time()) + 300},
                       key="secret", algorithm="HS256", headers={"kid": KID})
    with pytest.raises(TokenError):
        await verifier.verify(token)


async def test_a_token_with_no_kid_is_rejected(setup):
    private_key, verifier = setup
    token = jwt.encode({"iss": ISSUER, "aud": AUDIENCE, "sub": "x",
                        "exp": int(time.time()) + 300},
                       private_key, algorithm="RS256")
    with pytest.raises(TokenError):
        await verifier.verify(token)


async def test_garbage_is_rejected_without_raising_anything_but_token_error(setup):
    _private_key, verifier = setup
    with pytest.raises(TokenError):
        await verifier.verify("not-a-jwt")


async def test_scopes_are_read_from_scp_or_scope(setup):
    # Okta uses `scp` (a list); the RFC name is `scope` (a space-delimited
    # string). Accept both so the mode works against either shape.
    private_key, verifier = setup
    claims = await verifier.verify(mint(private_key, scp=None,
                                        scope="pgllens.read pgllens.admin"))
    assert claims.scopes == frozenset({"pgllens.read", "pgllens.admin"})


async def test_client_id_falls_back_to_sub_when_cid_is_absent(setup):
    private_key, verifier = setup
    claims = await verifier.verify(mint(private_key, cid=None))
    assert claims.client_id == "00u1testuser"
