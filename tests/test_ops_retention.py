import re

from tests.conftest_docker import (
    LOKI_IMAGE,
    OPS,
    TEMPO_IMAGE,
    docker,
    run_in_image,
)

LOKI = (OPS / "loki" / "loki-config.yaml").read_text(encoding="utf-8")
TEMPO = (OPS / "tempo" / "tempo.yaml").read_text(encoding="utf-8")


def test_loki_retention_is_a_lever_defaulting_to_90_days():
    assert "retention_period: ${LOKI_RETENTION:-2160h}" in LOKI


def test_loki_docker_logs_keep_two_weeks():
    assert re.search(r"selector: '\{job=\"pgllens-docker\"\}'\s+priority: 1\s+period: 336h", LOKI)


def test_loki_rejects_samples_older_than_retention():
    assert "reject_old_samples: true" in LOKI
    assert "reject_old_samples_max_age: ${LOKI_RETENTION:-2160h}" in LOKI


def test_tempo_retention_is_a_lever_defaulting_to_7_days():
    assert "block_retention: ${TEMPO_RETENTION:-168h}" in TEMPO


# The assertions above pin the text; these pin that the text is a config the real
# binary accepts -- including that ${LOKI_RETENTION}/${TEMPO_RETENTION} expand.
@docker
def test_loki_config_is_valid():
    result = run_in_image(
        LOKI_IMAGE, None,
        ["-config.file=/etc/loki/loki-config.yaml", "-config.expand-env=true", "-verify-config"],
        {OPS / "loki": "/etc/loki"}, env={"LOKI_RETENTION": "2160h"})
    assert result.returncode == 0, result.stdout + result.stderr


@docker
def test_tempo_config_is_valid():
    result = run_in_image(
        TEMPO_IMAGE, None,
        ["-config.file=/etc/tempo/tempo.yaml", "-config.expand-env=true", "-config.verify=true"],
        {OPS / "tempo": "/etc/tempo"}, env={"TEMPO_RETENTION": "168h"})
    assert result.returncode == 0, result.stdout + result.stderr
