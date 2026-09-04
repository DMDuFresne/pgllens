from types import SimpleNamespace

from pgllens.oauth import clientip


class _FakeSettings:
    def __init__(self, trust: bool):
        self.trust_proxy_headers = trust


def _fake_request(xff=None, peer="9.9.9.9"):
    headers = {"x-forwarded-for": xff} if xff else {}
    return SimpleNamespace(headers=headers, client=SimpleNamespace(host=peer))


def test_uses_peer_when_proxy_headers_not_trusted(monkeypatch):
    monkeypatch.setattr(clientip, "get_settings", lambda: _FakeSettings(False))
    req = _fake_request(xff="1.2.3.4", peer="9.9.9.9")
    assert clientip.client_ip(req) == "9.9.9.9"


def test_uses_forwarded_for_when_trusted(monkeypatch):
    monkeypatch.setattr(clientip, "get_settings", lambda: _FakeSettings(True))
    req = _fake_request(xff="1.2.3.4, 5.6.7.8", peer="9.9.9.9")
    # The trusted (last-hop) proxy appended 5.6.7.8; the leftmost 1.2.3.4 is
    # merely what the client itself claimed and must NOT be trusted.
    assert clientip.client_ip(req) == "5.6.7.8"


def test_forwarded_for_uses_rightmost_hop_not_leftmost_even_with_multiple_hops(
    monkeypatch,
):
    # A client sending a forged multi-hop header cannot make itself look like
    # the trusted proxy's own appended address: only the rightmost hop (the
    # one the adjacent trusted proxy appended) is ever used.
    monkeypatch.setattr(clientip, "get_settings", lambda: _FakeSettings(True))
    req = _fake_request(xff="attacker-forged, 10.0.0.1, 203.0.113.9", peer="9.9.9.9")
    assert clientip.client_ip(req) == "203.0.113.9"


def test_falls_back_to_unknown(monkeypatch):
    monkeypatch.setattr(clientip, "get_settings", lambda: _FakeSettings(False))
    req = SimpleNamespace(headers={}, client=None)
    assert clientip.client_ip(req) == "unknown"
