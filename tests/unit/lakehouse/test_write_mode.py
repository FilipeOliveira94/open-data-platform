"""Write-mode defaults per layer and incremental placeholders (ADR-0024)."""

from __future__ import annotations

import pytest

from open_data_platform.lakehouse.write_mode import (
    BRONZE_WRITE_MODE,
    IMPLEMENTED_WRITE_MODES,
    REBUILD_WRITE_MODE,
    SILVER_WRITE_MODE,
    WriteMode,
    WriteModeError,
    render_append_only_sql,
    render_incremental_merge_sql,
    require_implemented,
)


def test_layer_defaults() -> None:
    assert BRONZE_WRITE_MODE is WriteMode.INCREMENTAL_APPEND
    assert SILVER_WRITE_MODE is WriteMode.FULL_OVERWRITE
    assert REBUILD_WRITE_MODE is WriteMode.FULL_OVERWRITE
    assert IMPLEMENTED_WRITE_MODES == {
        WriteMode.FULL_OVERWRITE,
        WriteMode.INCREMENTAL_APPEND,
    }


def test_require_implemented_accepts_the_implemented_modes() -> None:
    require_implemented(WriteMode.FULL_OVERWRITE, context="demo.table")
    require_implemented(WriteMode.INCREMENTAL_APPEND, context="demo.table")


def test_incremental_placeholders_fail_loudly() -> None:
    with pytest.raises(WriteModeError, match="placeholder"):
        require_implemented(WriteMode.INCREMENTAL_MERGE, context="demo.table")
    with pytest.raises(WriteModeError, match="placeholder"):
        render_incremental_merge_sql(context="demo.table")
    with pytest.raises(WriteModeError, match="placeholder"):
        render_append_only_sql(context="demo.table")
