from tests.conftest_docker import ALLOY_IMAGE, OPS, docker, run_in_image

ALLOY = OPS / "alloy"
TEXT = (ALLOY / "config.alloy").read_text(encoding="utf-8")


def test_audit_writer_retries_forever():
    # Zero loss: positions only advance on an acknowledged write.
    assert "max_backoff_retries = 0" in TEXT
    assert "max_backoff_period" in TEXT


def test_audit_stream_has_only_the_event_label():
    audit_block = TEXT.split('loki.process "audit"')[1].split("}\n}")[0]
    assert 'event = ""' in audit_block
    for forbidden in ("client_id", "sub", "ip", "tool", "outcome"):
        assert f"{forbidden} = " not in audit_block


def test_docker_logs_carry_only_container_and_level_labels():
    assert 'loki.source.docker' in TEXT
    docker_block = TEXT.split('loki.process "docker"')[1].split("}\n}")[0]
    assert 'container = ""' in docker_block and 'level = ""' in docker_block


@docker
def test_config_validates():
    result = run_in_image(ALLOY_IMAGE, None, ["validate", "/etc/alloy/config.alloy"],
                          {ALLOY: "/etc/alloy"})
    assert result.returncode == 0, result.stdout + result.stderr
