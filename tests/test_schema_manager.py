"""Tests for the Schema Manager module."""

import os
import sqlite3
import tempfile

import pytest

from src.schema_manager import (
    infer_schema,
    get_table_schema,
    get_all_tables,
    check_schema_compatibility,
    generate_create_table_sql,
    get_schema_context,
)


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


def _write_csv(directory, filename, content):
    path = os.path.join(directory, filename)
    with open(path, "w", newline="") as f:
        f.write(content)
    return path


def _db_path(directory):
    return os.path.join(directory, "test.db")


def _create_table(db_path, sql):
    conn = sqlite3.connect(db_path)
    conn.execute(sql)
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# infer_schema
# ---------------------------------------------------------------------------

class TestInferSchema:
    def test_integer_column(self, tmp_dir):
        csv = _write_csv(tmp_dir, "t.csv", "count\n1\n2\n3\n")
        schema = infer_schema(csv)
        assert schema == [{"name": "count", "type": "INTEGER"}]

    def test_real_column(self, tmp_dir):
        csv = _write_csv(tmp_dir, "t.csv", "price\n1.5\n2.5\n")
        schema = infer_schema(csv)
        assert schema == [{"name": "price", "type": "REAL"}]

    def test_text_column(self, tmp_dir):
        csv = _write_csv(tmp_dir, "t.csv", "name\nAlice\nBob\n")
        schema = infer_schema(csv)
        assert schema == [{"name": "name", "type": "TEXT"}]

    def test_multiple_columns(self, tmp_dir):
        csv = _write_csv(tmp_dir, "t.csv", "name,age,score\nAlice,30,95.5\n")
        schema = infer_schema(csv)
        assert len(schema) == 3
        assert schema[0] == {"name": "name", "type": "TEXT"}
        assert schema[1] == {"name": "age", "type": "INTEGER"}
        assert schema[2] == {"name": "score", "type": "REAL"}

    def test_column_name_normalization(self, tmp_dir):
        csv = _write_csv(tmp_dir, "t.csv", " First Name ,age\nAlice,30\n")
        schema = infer_schema(csv)
        assert schema[0]["name"] == "First_Name"

    def test_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            infer_schema("/nonexistent/file.csv")

    def test_nullable_integers_inferred_type(self, tmp_dir):
        # pandas may read int columns with NaN as float64 or nullable Int64
        # depending on version; either REAL or INTEGER is acceptable
        csv = _write_csv(tmp_dir, "t.csv", "val\n1\n\n3\n")
        schema = infer_schema(csv)
        assert schema[0]["type"] in ("REAL", "INTEGER")


# ---------------------------------------------------------------------------
# get_table_schema
# ---------------------------------------------------------------------------

class TestGetTableSchema:
    def test_existing_table(self, tmp_dir):
        db = _db_path(tmp_dir)
        _create_table(db, "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        schema = get_table_schema(db, "people")
        assert schema is not None
        names = [c["name"] for c in schema]
        assert "id" in names
        assert "name" in names
        assert "age" in names

    def test_nonexistent_table_returns_none(self, tmp_dir):
        db = _db_path(tmp_dir)
        # Touch the DB so the file exists
        sqlite3.connect(db).close()
        assert get_table_schema(db, "nope") is None


# ---------------------------------------------------------------------------
# get_all_tables
# ---------------------------------------------------------------------------

class TestGetAllTables:
    def test_no_tables(self, tmp_dir):
        db = _db_path(tmp_dir)
        sqlite3.connect(db).close()
        assert get_all_tables(db) == []

    def test_multiple_tables(self, tmp_dir):
        db = _db_path(tmp_dir)
        _create_table(db, "CREATE TABLE a (id INTEGER PRIMARY KEY)")
        _create_table(db, "CREATE TABLE b (id INTEGER PRIMARY KEY)")
        tables = get_all_tables(db)
        assert set(tables) == {"a", "b"}


# ---------------------------------------------------------------------------
# check_schema_compatibility
# ---------------------------------------------------------------------------

class TestCheckSchemaCompatibility:
    def test_matching_schemas(self):
        existing = [
            {"name": "id", "type": "INTEGER"},
            {"name": "name", "type": "TEXT"},
            {"name": "age", "type": "INTEGER"},
        ]
        incoming = [
            {"name": "name", "type": "TEXT"},
            {"name": "age", "type": "INTEGER"},
        ]
        assert check_schema_compatibility(existing, incoming) == "match"

    def test_mismatched_column_count(self):
        existing = [
            {"name": "id", "type": "INTEGER"},
            {"name": "name", "type": "TEXT"},
        ]
        incoming = [
            {"name": "name", "type": "TEXT"},
            {"name": "age", "type": "INTEGER"},
        ]
        assert check_schema_compatibility(existing, incoming) == "mismatch"

    def test_mismatched_column_type(self):
        existing = [
            {"name": "id", "type": "INTEGER"},
            {"name": "val", "type": "TEXT"},
        ]
        incoming = [
            {"name": "val", "type": "INTEGER"},
        ]
        assert check_schema_compatibility(existing, incoming) == "mismatch"

    def test_case_insensitive_comparison(self):
        existing = [
            {"name": "id", "type": "INTEGER"},
            {"name": "Name", "type": "TEXT"},
        ]
        incoming = [
            {"name": "name", "type": "text"},
        ]
        assert check_schema_compatibility(existing, incoming) == "match"


# ---------------------------------------------------------------------------
# generate_create_table_sql
# ---------------------------------------------------------------------------

class TestGenerateCreateTableSql:
    def test_basic_table(self):
        schema = [
            {"name": "name", "type": "TEXT"},
            {"name": "age", "type": "INTEGER"},
        ]
        sql = generate_create_table_sql("people", schema)
        assert "CREATE TABLE people" in sql
        assert "id INTEGER PRIMARY KEY AUTOINCREMENT" in sql
        assert "name TEXT" in sql
        assert "age INTEGER" in sql

    def test_single_column(self):
        schema = [{"name": "value", "type": "REAL"}]
        sql = generate_create_table_sql("data", schema)
        assert "value REAL" in sql


# ---------------------------------------------------------------------------
# get_schema_context
# ---------------------------------------------------------------------------

class TestGetSchemaContext:
    def test_empty_database(self, tmp_dir):
        db = _db_path(tmp_dir)
        sqlite3.connect(db).close()
        assert "no tables" in get_schema_context(db).lower()

    def test_schema_context_includes_table_and_columns(self, tmp_dir):
        db = _db_path(tmp_dir)
        _create_table(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        ctx = get_schema_context(db)
        assert "users" in ctx
        assert "name" in ctx
        assert "TEXT" in ctx

    def test_multiple_tables(self, tmp_dir):
        db = _db_path(tmp_dir)
        _create_table(db, "CREATE TABLE a (id INTEGER PRIMARY KEY, x TEXT)")
        _create_table(db, "CREATE TABLE b (id INTEGER PRIMARY KEY, y REAL)")
        ctx = get_schema_context(db)
        assert "Table: a" in ctx
        assert "Table: b" in ctx
