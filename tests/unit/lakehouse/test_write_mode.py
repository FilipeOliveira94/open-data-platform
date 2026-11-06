"""Write-mode defaults and incremental placeholders."""

from __future__ import annotations

import pytest

from open_data_platform.lakehouse.write_mode import (
    CURRENT_WRITE_MODE,
    IMPLEMENTED_WRITE_MODES,
    WriteMode,
    WriteModeError,
    render_append_only_sql,
    render_incremental_merge_sql,
    require_implemented,
)


def test_current_mode_is_full_overwrite() -> None:
    assert CURRENT_WRITE_MODE is WriteMode.FULL_OVERWRITE
    assert WriteMode.FULL_OVERWRITE in IMPLEMENTED_WRITE_MODES
    assert WriteMode.INCREMENTAL_MERGE not in IMPLEMENTED_WRITE_MODES
    assert WriteMode.APPEND_ONLY not in IMPLEMENTED_WRITE_MODES


def test_require_implemented_accepts_full_overwrite() -> None:
    require_implemented(WriteMode.FULL_OVERWRITE, context="demo.table")


def test_incremental_placeholders_fail_loudly() -> None:
    with pytest.raises(WriteModeError, match="placeholder"):
        require_implemented(WriteMode.INCREMENTAL_MERGE, context="demo.table")
    with pytest.raises(WriteModeError, match="full load \\+ overwrite"):
        render_incremental_merge_sql(context="demo.table")
    with pytest.raises(WriteModeError, match="placeholder"):
        render_append_only_sql(context="demo.table")
