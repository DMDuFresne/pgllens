import time

from pgllens.oauth.store import ClientStore, CodeStore, TokenStore


def test_registered_client_gets_id_and_secret_and_is_retrievable():
    store = ClientStore()
    reg = store.register({"client_name": "Claude", "redirect_uris": ["http://x/cb"]})
    assert reg["client_id"] and reg["client_secret"]
    assert store.get(reg["client_id"])["client_name"] == "Claude"
    assert store.get("nope") is None


def test_authorization_code_is_single_use():
    store = CodeStore()
    code = store.issue("cid", "http://x/cb", "chal", "S256", "mcp")
    assert store.consume(code)["client_id"] == "cid"
    assert store.consume(code) is None  # replay must fail


def test_expired_code_is_not_consumable(monkeypatch):
    store = CodeStore(ttl_seconds=1)
    code = store.issue("cid", "http://x/cb", "chal", "S256", "mcp")
    # Offset relative to the real clock (not an absolute constant): time.monotonic()
    # has no fixed zero point (e.g. it's system uptime on some platforms), so a
    # hardcoded small absolute replacement value would be spuriously "in the past"
    # on any long-uptime host. +5s relative to "now" reliably simulates 5 real
    # seconds elapsing regardless of the host's monotonic baseline.
    later = time.monotonic() + 5
    monkeypatch.setattr("pgllens.oauth.store.time.monotonic", lambda: later)
    assert store.consume(code) is None


async def test_token_validates_until_revoked():
    store = TokenStore(ttl_seconds=3600)
    token, expires_in = await store.issue("cid", "mcp")
    assert expires_in == 3600
    assert (await store.validate(token))["client_id"] == "cid"
    await store.revoke(token)
    assert await store.validate(token) is None


async def test_unknown_token_is_invalid():
    assert await TokenStore().validate("made-up") is None


# --- adversarial cases ---


def test_code_consume_from_different_client_still_single_use_globally():
    store = CodeStore()
    code_a = store.issue("cid-a", "http://x/cb", "chal", "S256", "mcp")
    code_b = store.issue("cid-b", "http://x/cb", "chal", "S256", "mcp")
    assert store.consume(code_a)["client_id"] == "cid-a"
    assert store.consume(code_a) is None
    # a different code from a different client is unaffected and still single-use
    assert store.consume(code_b)["client_id"] == "cid-b"
    assert store.consume(code_b) is None


async def test_token_ttl_zero_or_negative_is_immediately_invalid():
    store = TokenStore(ttl_seconds=0)
    token, _ = await store.issue("cid", "mcp")
    assert await store.validate(token) is None

    store_neg = TokenStore(ttl_seconds=-1)
    token_neg, _ = await store_neg.issue("cid", "mcp")
    assert await store_neg.validate(token_neg) is None


def test_register_cannot_override_client_id_or_secret():
    store = ClientStore()
    reg = store.register(
        {
            "client_name": "evil",
            "client_id": "victim",
            "client_secret": "",
        }
    )
    # server-generated values win, not the attacker-supplied ones
    assert reg["client_id"] != "victim"
    assert reg["client_secret"] != ""
    assert len(reg["client_secret"]) >= 32
    # the caller cannot smuggle themselves in under a different client's id
    assert store.get("victim") is None
    assert store.get(reg["client_id"])["client_id"] == reg["client_id"]
    assert store.get(reg["client_id"])["client_secret"] == reg["client_secret"]


async def test_token_store_never_holds_raw_token_as_dict_key():
    store = TokenStore()
    token, _ = await store.issue("cid", "mcp")
    # whitebox: the raw token must not appear as a key in internal storage
    assert token not in store._tokens
