from pgllens.config import Settings
from pgllens.instructions import build_instructions

DSN = "postgresql://u:p@localhost:5432/flux"


def test_instructions_name_the_exposed_schemas():
    s = Settings(database_url=DSN, exposed_schemas="wms,task")
    text = build_instructions(s)
    assert "wms" in text and "task" in text


def test_instructions_include_domain_context_when_configured():
    s = Settings(database_url=DSN, exposed_schemas="public",
                 domain_context="Lots expire per FEFO.")
    assert "FEFO" in build_instructions(s)


def test_instructions_omit_template_boilerplate():
    s = Settings(database_url=DSN, exposed_schemas="public",
                 domain_context="<!-- pgllens:template --> Replace me.")
    assert "Replace me" not in build_instructions(s)
