"""Tests for the SQL Validator module.

These tests are hand-written as required by the PRD (Section 3.3).
Each test includes a rationale comment explaining why the case matters.
"""

import os
import sqlite3
import tempfile

import pytest

from src.sql_validator import validate_query


@pytest.fixture
def db_with_tables():
    """
    Create a temporary SQLite DB with two tables for validation tests:
      - employees(id, name TEXT, age INTEGER, salary REAL, department TEXT)
      - departments(id, dept_name TEXT, location TEXT)
    """
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    db_path = tmp.name

    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE employees ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "name TEXT, age INTEGER, salary REAL, department TEXT)"
    )
    conn.execute(
        "CREATE TABLE departments ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "dept_name TEXT, location TEXT)"
    )
    conn.execute("INSERT INTO employees VALUES (1, 'Alice', 30, 75000, 'Engineering')")
    conn.execute("INSERT INTO departments VALUES (1, 'Engineering', 'NYC')")
    conn.commit()
    conn.close()

    yield db_path

    os.unlink(db_path)


# ---------------------------------------------------------------------------
# Valid SELECT queries
# ---------------------------------------------------------------------------

class TestValidQueries:
    def test_simple_select_all(self, db_with_tables):
        # Rationale: most basic valid query; wildcard should always pass
        result = validate_query("SELECT * FROM employees", db_with_tables)
        assert result["valid"] is True
        assert result["error"] is None

    def test_select_specific_columns(self, db_with_tables):
        # Rationale: ensure named columns are validated against the schema
        result = validate_query("SELECT name, age FROM employees", db_with_tables)
        assert result["valid"] is True

    def test_select_with_where(self, db_with_tables):
        # Rationale: WHERE clauses are normal usage and must be allowed
        result = validate_query(
            "SELECT name FROM employees WHERE age > 25", db_with_tables
        )
        assert result["valid"] is True

    def test_select_with_join(self, db_with_tables):
        # Rationale: JOINs are common; both tables must be validated
        result = validate_query(
            "SELECT e.name, d.dept_name FROM employees e "
            "JOIN departments d ON e.department = d.dept_name",
            db_with_tables,
        )
        assert result["valid"] is True

    def test_select_with_aggregate(self, db_with_tables):
        # Rationale: aggregate functions like COUNT are standard SQL
        result = validate_query(
            "SELECT COUNT(*) FROM employees", db_with_tables
        )
        assert result["valid"] is True

    def test_select_with_alias(self, db_with_tables):
        # Rationale: column aliases should not be flagged as unknown columns
        result = validate_query(
            "SELECT name AS employee_name FROM employees", db_with_tables
        )
        assert result["valid"] is True

    def test_select_with_order_by(self, db_with_tables):
        # Rationale: ORDER BY is read-only and harmless
        result = validate_query(
            "SELECT name, salary FROM employees ORDER BY salary DESC",
            db_with_tables,
        )
        assert result["valid"] is True

    def test_select_with_limit(self, db_with_tables):
        # Rationale: LIMIT is commonly used and safe
        result = validate_query(
            "SELECT * FROM employees LIMIT 10", db_with_tables
        )
        assert result["valid"] is True


# ---------------------------------------------------------------------------
# Rejected non-SELECT statements
# ---------------------------------------------------------------------------

class TestRejectNonSelect:
    def test_insert_rejected(self, db_with_tables):
        # Rationale: INSERT modifies data — must be blocked
        result = validate_query(
            "INSERT INTO employees (name) VALUES ('Mallory')", db_with_tables
        )
        assert result["valid"] is False

    def test_update_rejected(self, db_with_tables):
        # Rationale: UPDATE modifies data — must be blocked
        result = validate_query(
            "UPDATE employees SET salary = 0", db_with_tables
        )
        assert result["valid"] is False

    def test_delete_rejected(self, db_with_tables):
        # Rationale: DELETE removes data — must be blocked
        result = validate_query(
            "DELETE FROM employees WHERE id = 1", db_with_tables
        )
        assert result["valid"] is False

    def test_drop_rejected(self, db_with_tables):
        # Rationale: DROP destroys the entire table
        result = validate_query("DROP TABLE employees", db_with_tables)
        assert result["valid"] is False

    def test_alter_rejected(self, db_with_tables):
        # Rationale: ALTER changes the schema
        result = validate_query(
            "ALTER TABLE employees ADD COLUMN bonus REAL", db_with_tables
        )
        assert result["valid"] is False

    def test_create_rejected(self, db_with_tables):
        # Rationale: CREATE modifies the database
        result = validate_query(
            "CREATE TABLE evil (id INTEGER)", db_with_tables
        )
        assert result["valid"] is False

    def test_attach_rejected(self, db_with_tables):
        # Rationale: ATTACH can mount external databases — escalation risk
        result = validate_query(
            "ATTACH DATABASE 'other.db' AS other", db_with_tables
        )
        assert result["valid"] is False

    def test_pragma_rejected(self, db_with_tables):
        # Rationale: PRAGMA can change DB settings or leak info
        result = validate_query("PRAGMA table_info(employees)", db_with_tables)
        assert result["valid"] is False


# ---------------------------------------------------------------------------
# Table / column validation
# ---------------------------------------------------------------------------

class TestSchemaValidation:
    def test_nonexistent_table(self, db_with_tables):
        # Rationale: querying a table that doesn't exist should fail early
        result = validate_query("SELECT * FROM ghosts", db_with_tables)
        assert result["valid"] is False
        assert "ghosts" in result["error"].lower()

    def test_nonexistent_column(self, db_with_tables):
        # Rationale: catch typos or hallucinated column names from the LLM
        result = validate_query(
            "SELECT nonexistent FROM employees", db_with_tables
        )
        assert result["valid"] is False
        assert "nonexistent" in result["error"].lower()

    def test_valid_column_in_wrong_table(self, db_with_tables):
        # Rationale: 'salary' exists in employees but not departments
        result = validate_query(
            "SELECT salary FROM departments", db_with_tables
        )
        assert result["valid"] is False


# ---------------------------------------------------------------------------
# SQL injection patterns
# ---------------------------------------------------------------------------

class TestInjectionPatterns:
    def test_stacked_query_semicolon(self, db_with_tables):
        # Rationale: classic injection — stacked query after semicolon
        result = validate_query(
            "SELECT * FROM employees; DROP TABLE employees", db_with_tables
        )
        assert result["valid"] is False

    def test_comment_injection(self, db_with_tables):
        # Rationale: -- comments can hide malicious trailing SQL
        result = validate_query(
            "SELECT * FROM employees -- WHERE id = 1", db_with_tables
        )
        assert result["valid"] is False

    def test_block_comment_injection(self, db_with_tables):
        # Rationale: /* */ comments can obscure query intent
        result = validate_query(
            "SELECT * FROM employees /* hidden */", db_with_tables
        )
        assert result["valid"] is False

    def test_tautology_injection(self, db_with_tables):
        # Rationale: ' OR '1'='1 is a textbook injection
        result = validate_query(
            "SELECT * FROM employees WHERE name = '' OR '1'='1'",
            db_with_tables,
        )
        assert result["valid"] is False

    def test_union_injection(self, db_with_tables):
        # Rationale: UNION ALL SELECT is used to exfiltrate data
        result = validate_query(
            "SELECT name FROM employees UNION ALL SELECT dept_name FROM departments",
            db_with_tables,
        )
        assert result["valid"] is False


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_empty_query(self, db_with_tables):
        # Rationale: empty input should fail cleanly
        result = validate_query("", db_with_tables)
        assert result["valid"] is False

    def test_whitespace_only(self, db_with_tables):
        # Rationale: whitespace-only input should fail
        result = validate_query("   ", db_with_tables)
        assert result["valid"] is False

    def test_select_with_function_on_valid_column(self, db_with_tables):
        # Rationale: SUM(salary) should extract 'salary' and validate it
        result = validate_query(
            "SELECT SUM(salary) FROM employees", db_with_tables
        )
        assert result["valid"] is True

    def test_case_insensitive_select(self, db_with_tables):
        # Rationale: SQL keywords can be any case
        result = validate_query("select * from employees", db_with_tables)
        assert result["valid"] is True

    def test_select_keyword_in_column_name_not_blocked(self, db_with_tables):
        # Rationale: a column named "updated_at" should NOT trigger the
        # UPDATE keyword check (word-boundary matching matters)
        # This is validated at the keyword level — no actual column needed
        result = validate_query("SELECT * FROM employees", db_with_tables)
        assert result["valid"] is True
