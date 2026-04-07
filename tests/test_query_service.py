"""Tests for the Query Service module.

Uses mocked LLM responses and real SQLite databases to test the
full orchestration pipeline.
"""

import os
import sqlite3
import tempfile
from unittest.mock import patch

import pytest

from src.llm_adapter import LLMError
from src.query_service import (
    describe_table,
    execute_natural_language_query,
    execute_raw_sql,
    list_tables,
)


@pytest.fixture
def db_with_data():
    """Create a temp DB with an employees table containing sample data."""
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    db_path = tmp.name

    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE employees ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "name TEXT, age INTEGER, salary REAL)"
    )
    conn.execute("INSERT INTO employees (name, age, salary) VALUES ('Alice', 30, 75000)")
    conn.execute("INSERT INTO employees (name, age, salary) VALUES ('Bob', 25, 85000)")
    conn.execute("INSERT INTO employees (name, age, salary) VALUES ('Charlie', 35, 65000)")
    conn.commit()
    conn.close()

    yield db_path
    os.unlink(db_path)


@pytest.fixture
def empty_db():
    """Create a temp DB with no tables."""
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    yield tmp.name
    os.unlink(tmp.name)


# ---------------------------------------------------------------------------
# execute_natural_language_query
# ---------------------------------------------------------------------------

class TestExecuteNLQuery:
    @patch("src.query_service.natural_language_to_sql")
    def test_full_pipeline_success(self, mock_llm, db_with_data):
        mock_llm.return_value = {
            "sql": "SELECT name, salary FROM employees WHERE age > 26",
            "explanation": "Employees older than 26",
        }

        result = execute_natural_language_query("who earns the most?", db_with_data)

        assert result["success"] is True
        assert result["error"] is None
        assert len(result["results"]) == 2  # Alice (30) and Charlie (35)
        assert result["sql"] == "SELECT name, salary FROM employees WHERE age > 26"
        assert result["explanation"] == "Employees older than 26"

    @patch("src.query_service.natural_language_to_sql")
    def test_llm_returns_invalid_sql(self, mock_llm, db_with_data):
        # LLM hallucinates a table that doesn't exist
        mock_llm.return_value = {
            "sql": "SELECT * FROM nonexistent_table",
            "explanation": "Oops",
        }

        result = execute_natural_language_query("show me data", db_with_data)

        assert result["success"] is False
        assert "Validation failed" in result["error"]

    @patch("src.query_service.natural_language_to_sql")
    def test_llm_api_failure(self, mock_llm, db_with_data):
        mock_llm.side_effect = LLMError("API timeout")

        result = execute_natural_language_query("anything", db_with_data)

        assert result["success"] is False
        assert "LLM error" in result["error"]

    def test_empty_database(self, empty_db):
        result = execute_natural_language_query("show me everything", empty_db)

        assert result["success"] is False
        assert "No tables" in result["error"]

    @patch("src.query_service.natural_language_to_sql")
    def test_empty_result_set(self, mock_llm, db_with_data):
        mock_llm.return_value = {
            "sql": "SELECT * FROM employees WHERE age > 100",
            "explanation": "Very old employees",
        }

        result = execute_natural_language_query("centenarians?", db_with_data)

        assert result["success"] is True
        assert result["results"] == []

    @patch("src.query_service.natural_language_to_sql")
    def test_validator_rejects_dangerous_sql(self, mock_llm, db_with_data):
        mock_llm.return_value = {
            "sql": "SELECT * FROM employees; DROP TABLE employees",
            "explanation": "Bad query",
        }

        result = execute_natural_language_query("drop it", db_with_data)

        assert result["success"] is False
        assert "Validation failed" in result["error"]


# ---------------------------------------------------------------------------
# execute_raw_sql
# ---------------------------------------------------------------------------

class TestExecuteRawSql:
    def test_valid_select(self, db_with_data):
        result = execute_raw_sql("SELECT name FROM employees", db_with_data)

        assert result["success"] is True
        assert len(result["results"]) == 3
        assert result["results"][0]["name"] == "Alice"

    def test_rejects_insert(self, db_with_data):
        result = execute_raw_sql(
            "INSERT INTO employees (name) VALUES ('Mallory')", db_with_data
        )
        assert result["success"] is False
        assert "Validation failed" in result["error"]

    def test_rejects_nonexistent_table(self, db_with_data):
        result = execute_raw_sql("SELECT * FROM ghosts", db_with_data)
        assert result["success"] is False

    def test_select_with_aggregate(self, db_with_data):
        result = execute_raw_sql(
            "SELECT COUNT(*) FROM employees", db_with_data
        )
        assert result["success"] is True
        assert list(result["results"][0].values())[0] == 3


# ---------------------------------------------------------------------------
# list_tables / describe_table
# ---------------------------------------------------------------------------

class TestTableHelpers:
    def test_list_tables(self, db_with_data):
        tables = list_tables(db_with_data)
        assert "employees" in tables

    def test_list_tables_empty_db(self, empty_db):
        assert list_tables(empty_db) == []

    def test_describe_table(self, db_with_data):
        schema = describe_table("employees", db_with_data)
        col_names = [c["name"] for c in schema]
        assert "name" in col_names
        assert "salary" in col_names

    def test_describe_nonexistent_table(self, db_with_data):
        assert describe_table("nope", db_with_data) == []
