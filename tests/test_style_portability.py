import ast
from pathlib import Path

import pgllens.llens_style as pkg

PKG = Path(pkg.__file__).parent


def test_no_host_imports():
    """No import may name the `pgllens` package at all -- absolute or
    `from pgllens...` -- so this directory is copyable verbatim into another
    host. A relative import (`from .foo import bar`, node.level > 0) is the
    only way to reach a sibling module in this package."""
    for path in PKG.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "pgllens" or alias.name.startswith("pgllens."):
                        raise AssertionError(f"{path.name} imports host package: {alias.name}")
            elif (isinstance(node, ast.ImportFrom) and node.level == 0 and node.module
                  and (node.module == "pgllens" or node.module.startswith("pgllens."))):
                raise AssertionError(f"{path.name} imports host package: {node.module}")


def test_public_names():
    for name in ("Response", "Error", "Section", "Table", "Bullets", "Bullet", "Code", "Caveat",
                 "Call", "ErrorCode", "hint_for", "render", "render_error", "render_call", "lint",
                 "estimate", "count", "size", "duration", "iso", "ident"):
        assert hasattr(pkg, name), name
