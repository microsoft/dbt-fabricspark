from __future__ import annotations

from pathlib import Path

import pytest
from dbt_common.clients._jinja_blocks import (
    BlockIterator,
    ExtractWarning,
    TagIterator,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
MACROS_DIR = REPO_ROOT / "src" / "dbt" / "include" / "fabricspark" / "macros"
ALLOWED_BLOCKS = {"macro", "materialization", "test", "data_test"}


def _macro_files() -> list[Path]:
    return sorted(MACROS_DIR.rglob("*.sql"))


@pytest.mark.parametrize(
    "macro_path",
    _macro_files(),
    ids=lambda p: str(p.relative_to(REPO_ROOT)),
)
def test_macro_file_has_no_unexpected_block_warnings(macro_path: Path) -> None:
    warnings: list[ExtractWarning] = []
    BlockIterator(
        TagIterator(macro_path.read_text()),
        warning_callback=warnings.append,
    ).lex_for_blocks(allowed_blocks=ALLOWED_BLOCKS, collect_raw_data=False)

    formatted = "\n".join(f"  {w.warning_type}: {w.msg}" for w in warnings)
    assert not warnings, (
        f"{macro_path.relative_to(REPO_ROOT)} emitted unexpected-block warnings "
        f"(would trigger UnexpectedJinjaBlockDeprecation in dbt-core):\n{formatted}"
    )
