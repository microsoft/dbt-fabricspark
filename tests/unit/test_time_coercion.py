"""Tests for coerce_time_columns — normalizing Livy string timestamps (#237)."""

import datetime as dt

from dbt.adapters.fabricspark.livy_backend import coerce_time_columns


def _schema(*types):
    return [{"name": f"c{i}", "type": t, "nullable": True} for i, t in enumerate(types)]


class TestCoerceTimeColumns:
    def test_timestamp_with_microseconds(self):
        rows = [["2024-01-01 12:00:00.123456"]]
        coerce_time_columns(rows, _schema("timestamp"))
        assert rows[0][0] == dt.datetime(2024, 1, 1, 12, 0, 0, 123456)

    def test_timestamp_without_microseconds(self):
        rows = [["2024-01-01 12:00:00"]]
        coerce_time_columns(rows, _schema("timestamp"))
        assert rows[0][0] == dt.datetime(2024, 1, 1, 12, 0, 0)

    def test_timestamp_iso_t_separator_and_z_suffix(self):
        rows = [["2024-01-01T05:06:07Z"]]
        coerce_time_columns(rows, _schema("timestamp"))
        assert rows[0][0] == dt.datetime(2024, 1, 1, 5, 6, 7)

    def test_timestamp_ntz(self):
        rows = [["2024-05-06 07:08:09"]]
        coerce_time_columns(rows, _schema("timestamp_ntz"))
        assert rows[0][0] == dt.datetime(2024, 5, 6, 7, 8, 9)

    def test_date(self):
        rows = [["2024-02-29"]]
        coerce_time_columns(rows, _schema("date"))
        assert rows[0][0] == dt.date(2024, 2, 29)

    def test_non_time_columns_untouched(self):
        rows = [["hello", "42", "2024-01-01 00:00:00"]]
        coerce_time_columns(rows, _schema("string", "integer", "string"))
        assert rows == [["hello", "42", "2024-01-01 00:00:00"]]

    def test_null_values_pass_through(self):
        rows = [[None, None]]
        coerce_time_columns(rows, _schema("timestamp", "date"))
        assert rows == [[None, None]]

    def test_unparseable_string_left_untouched(self):
        rows = [["not-a-timestamp", "not-a-date"]]
        coerce_time_columns(rows, _schema("timestamp", "date"))
        assert rows == [["not-a-timestamp", "not-a-date"]]

    def test_already_native_value_untouched(self):
        native = dt.datetime(2024, 1, 1)
        rows = [[native]]
        coerce_time_columns(rows, _schema("timestamp"))
        assert rows[0][0] is native

    def test_mixed_columns_and_multiple_rows(self):
        rows = [
            ["2024-01-01 00:00:00", "keep", "2024-01-02"],
            ["2024-06-01 06:06:06.000001", "me", "2024-06-02"],
        ]
        coerce_time_columns(rows, _schema("timestamp", "string", "date"))
        assert rows[0][0] == dt.datetime(2024, 1, 1, 0, 0, 0)
        assert rows[0][1] == "keep"
        assert rows[0][2] == dt.date(2024, 1, 2)
        assert rows[1][0] == dt.datetime(2024, 6, 1, 6, 6, 6, 1)
        assert rows[1][2] == dt.date(2024, 6, 2)

    def test_empty_rows(self):
        assert coerce_time_columns([], _schema("timestamp")) == []

    def test_none_rows(self):
        assert coerce_time_columns(None, _schema("timestamp")) is None

    def test_empty_schema(self):
        rows = [["2024-01-01 00:00:00"]]
        coerce_time_columns(rows, [])
        assert rows == [["2024-01-01 00:00:00"]]

    def test_ragged_row_shorter_than_schema(self):
        rows = [["2024-01-01 00:00:00"]]
        coerce_time_columns(rows, _schema("timestamp", "date"))
        assert rows[0][0] == dt.datetime(2024, 1, 1, 0, 0, 0)
