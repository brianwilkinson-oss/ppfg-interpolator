from datetime import datetime

import pytest

from corva_cli.timewindow import AutoTimeParseError, parse_auto_time, resolve_auto_window


REFERENCE = datetime(2024, 1, 10, 12, 0, 0)


def test_parse_auto_time_zero_delta():
    result = parse_auto_time("auto_0d", reference=REFERENCE)
    assert result == REFERENCE


def test_parse_auto_time_combined_units():
    result = parse_auto_time("auto_1d2h30m", reference=REFERENCE)
    assert result == datetime(2024, 1, 9, 9, 30, 0)


def test_parse_auto_time_invalid_prefix():
    with pytest.raises(AutoTimeParseError):
        parse_auto_time("1d")


def test_resolve_auto_window_order_validation():
    with pytest.raises(AutoTimeParseError):
        resolve_auto_window("auto_0d", "auto_1d", reference=REFERENCE)


def test_resolve_auto_window():
    start, end = resolve_auto_window("auto_2h", "auto_0d", reference=REFERENCE)
    assert start == datetime(2024, 1, 10, 10, 0, 0)
    assert end == REFERENCE
