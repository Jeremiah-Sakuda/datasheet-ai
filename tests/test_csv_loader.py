"""Tests for the CSV Loader module."""

import os
import sqlite3
import tempfile

import pytest

from src.csv_loader import load_csv, _table_name_from_path


@pytest.fixture
def tmp_dir():
    """Provide a temporary directory that is cleaned up after the test."""
    with tempfile.TemporaryDirectory() as d:
        yield d


def _write_csv(directory, filename, content):
    """Helper to write a CSV file and return its path."""
    path = os.path.join(directory, filename)
    with open(path, "w", newline="") as f:
        f.write(content)
    return path


def _db_path(directory):
    return os.path.join(directory, "test.db")


# ---------------------------------------------------------------------------
# _table_name_from_path
# ---------------------------------------------------------------------------

class TestTableNameFromPath:
    def test_simple_name(self):
        assert _table_name_from_path("data/sample.csv") == "sample"

    def test_spaces_replaced(self):
        assert _table_name_from_path("My Sales Data.csv") == "my_sales_data"

    def test_leading_digit(self):
        assert _table_name_from_path("2024-revenue.csv") == "_2024_revenue"

    def test_special_characters(self):
        assert _table_name_from_path("data@#2024!!.csv") == "data_2024"

    def test_empty_after_sanitize(self):
        assert _table_name_from_path("@@@.csv") == "imported_data"


# ---------------------------------------------------------------------------
# load_csv — happy paths
# ---------------------------------------------------------------------------

class TestLoadCsvHappyPath:
    def test_load_valid_csv_creates_table(self, tmp_dir):
        csv_path = _write_csv(tmp_dir, "people.csv",
                              "name,age,salary\nAlice,30,75000\nBob,25,85000\n")
        db = _db_path(tmp_dir)

        result = load_csv(csv_path, db)

        assert result["table_name"] == "people"
        assert result["rows_inserted"] == 2
        assert result["action"] == "created"

        # Verify data actually in DB
        conn = sqlite3.connect(db)
        rows = conn.execute("SELECT name, age, salary FROM people").fetchall()
        conn.close()
        assert len(rows) == 2
        assert rows[0] == ("Alice", 30, 75000)

    def test_append_on_matching_schema(self, tmp_dir):
        csv_path = _write_csv(tmp_dir, "people.csv",
                              "name,age\nAlice,30\n")
        db = _db_path(tmp_dir)

        load_csv(csv_path, db)
        result = load_csv(csv_path, db)

        assert result["action"] == "appended"
        assert result["rows_inserted"] == 1

        conn = sqlite3.connect(db)
        rows = conn.execute("SELECT name FROM people").fetchall()
        conn.close()
        assert len(rows) == 2

    def test_mismatched_schema_creates_new_table(self, tmp_dir):
        csv1 = _write_csv(tmp_dir, "data.csv", "name,age\nAlice,30\n")
        db = _db_path(tmp_dir)
        load_csv(csv1, db)

        # Overwrite the CSV with different columns
        csv2 = _write_csv(tmp_dir, "data.csv", "city,population\nNY,8000000\n")
        result = load_csv(csv2, db)

        assert result["table_name"] == "data_2"
        assert result["action"] == "created"

    def test_load_sample_csv(self, tmp_dir):
        """Load the project's sample.csv to make sure it works end-to-end."""
        sample = os.path.join(os.path.dirname(__file__), "..", "data", "sample.csv")
        if not os.path.exists(sample):
            pytest.skip("sample.csv not found")

        db = _db_path(tmp_dir)
        result = load_csv(sample, db)

        assert result["rows_inserted"] == 5
        assert result["action"] == "created"


# ---------------------------------------------------------------------------
# load_csv — error cases
# ---------------------------------------------------------------------------

class TestLoadCsvErrors:
    def test_file_not_found(self, tmp_dir):
        with pytest.raises(FileNotFoundError):
            load_csv(os.path.join(tmp_dir, "nope.csv"), _db_path(tmp_dir))

    def test_empty_csv_no_data_rows(self, tmp_dir):
        csv_path = _write_csv(tmp_dir, "empty.csv", "name,age\n")
        with pytest.raises(ValueError, match="no data rows"):
            load_csv(csv_path, _db_path(tmp_dir))


# ---------------------------------------------------------------------------
# load_csv — type handling
# ---------------------------------------------------------------------------

class TestLoadCsvTypes:
    def test_integer_columns_stored_correctly(self, tmp_dir):
        csv_path = _write_csv(tmp_dir, "nums.csv", "val\n1\n2\n3\n")
        db = _db_path(tmp_dir)
        load_csv(csv_path, db)

        conn = sqlite3.connect(db)
        rows = conn.execute("SELECT val FROM nums").fetchall()
        conn.close()
        assert rows == [(1,), (2,), (3,)]

    def test_float_columns_stored_correctly(self, tmp_dir):
        csv_path = _write_csv(tmp_dir, "floats.csv", "price\n1.5\n2.99\n")
        db = _db_path(tmp_dir)
        load_csv(csv_path, db)

        conn = sqlite3.connect(db)
        rows = conn.execute("SELECT price FROM floats").fetchall()
        conn.close()
        assert rows[0][0] == pytest.approx(1.5)

    def test_mixed_types_default_to_text(self, tmp_dir):
        csv_path = _write_csv(tmp_dir, "mix.csv", "info\nhello\n42\n3.14\n")
        db = _db_path(tmp_dir)
        load_csv(csv_path, db)

        conn = sqlite3.connect(db)
        rows = conn.execute("SELECT info FROM mix").fetchall()
        conn.close()
        # pandas reads as object/TEXT when mixed
        assert len(rows) == 3
