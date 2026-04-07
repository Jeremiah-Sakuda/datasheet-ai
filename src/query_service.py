"""
Query Service — orchestrates the full query pipeline.

This is the single point of contact between the CLI and the database
for all query operations.

Pipeline:
  User question → LLM Adapter → SQL Validator → SQLite execution → results
"""

import sqlite3

from src.llm_adapter import LLMError, natural_language_to_sql
from src.schema_manager import get_all_tables, get_schema_context, get_table_schema
from src.sql_validator import validate_query


def execute_natural_language_query(user_query: str, db_path: str) -> dict:
    """
    Full pipeline: NL → LLM → SQL → Validate → Execute → Format.

    Args:
        user_query: Plain English question from the user.
        db_path:    Path to the SQLite database.

    Returns:
        dict with keys:
            - success:     bool
            - sql:         str  — the generated SQL (for transparency)
            - explanation: str  — LLM explanation
            - results:     list[dict] — query results as list of row dicts
            - error:       str | None — error message if any step failed
    """
    # 1. Get schema context for the LLM
    schema_context = get_schema_context(db_path)
    if "no tables" in schema_context.lower():
        return {
            "success": False,
            "sql": None,
            "explanation": None,
            "results": [],
            "error": "No tables in the database. Load a CSV first.",
        }

    # 2. Call LLM to generate SQL
    try:
        llm_result = natural_language_to_sql(user_query, schema_context)
    except LLMError as exc:
        return {
            "success": False,
            "sql": None,
            "explanation": None,
            "results": [],
            "error": f"LLM error: {exc}",
        }

    sql = llm_result["sql"]
    explanation = llm_result["explanation"]

    # 3. Validate the generated SQL
    validation = validate_query(sql, db_path)
    if not validation["valid"]:
        return {
            "success": False,
            "sql": sql,
            "explanation": explanation,
            "results": [],
            "error": f"Validation failed: {validation['error']}",
        }

    # 4. Execute the validated SQL
    return _execute_and_format(sql, db_path, explanation=explanation)


def execute_raw_sql(query: str, db_path: str) -> dict:
    """
    Validate and execute a raw SQL query (for advanced users).

    Same return format as execute_natural_language_query, minus explanation.
    """
    validation = validate_query(query, db_path)
    if not validation["valid"]:
        return {
            "success": False,
            "sql": query,
            "explanation": None,
            "results": [],
            "error": f"Validation failed: {validation['error']}",
        }

    return _execute_and_format(query, db_path, explanation=None)


def list_tables(db_path: str) -> list[str]:
    """Return available tables.  Delegates to Schema Manager."""
    return get_all_tables(db_path)


def describe_table(table_name: str, db_path: str) -> list[dict]:
    """Return schema of a specific table.  Delegates to Schema Manager."""
    schema = get_table_schema(db_path, table_name)
    return schema if schema is not None else []


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _execute_and_format(sql: str, db_path: str, explanation: str | None) -> dict:
    """Execute a validated SQL query and return structured results."""
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(sql)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        results = [dict(zip(columns, row)) for row in rows]
        conn.close()
    except sqlite3.Error as exc:
        return {
            "success": False,
            "sql": sql,
            "explanation": explanation,
            "results": [],
            "error": f"Database error: {exc}",
        }

    return {
        "success": True,
        "sql": sql,
        "explanation": explanation,
        "results": results,
        "error": None,
    }
