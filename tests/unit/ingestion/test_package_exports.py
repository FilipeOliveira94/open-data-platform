"""The lazy ``open_data_platform.ingestion`` exports resolve."""

from __future__ import annotations

import pytest

import open_data_platform.ingestion as ingestion


@pytest.mark.parametrize("name", ingestion.__all__)
def test_every_listed_export_resolves(name: str) -> None:
    assert getattr(ingestion, name) is not None


def test_unknown_name_raises_attribute_error() -> None:
    with pytest.raises(AttributeError):
        _ = ingestion.not_an_export
