"""
Schema Manager — owns all database structure operations.

Responsibilities:
  - Infer SQL column types from CSV data
  - Inspect existing table schemas via PRAGMA
  - Compare schemas for append compatibility
  - Generate CREATE TABLE DDL with auto-increment PK
  - Build human-readable schema context for the LLM prompt
"""

import os
import sqlite3

import pandas as pd


def infer_schema(file_path: str) -> list[dict]:
    """
    Inspect CSV columns and infer SQL types.

    Reads the CSV and maps pandas dtypes to SQLite types:
      int64   -> INTEGER
      float64 -> REAL
      object  -> TEXT  (default / fallback)

    Returns:
        List of dicts: [{"name": "col_name", "type": "TEXT|INTEGER|REAL"}]

    Raises:
        FileNotFoundError: if file_path does not exist
        ValueError: if the CSV has no columns
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    df = pd.read_csv(file_path)

    if df.columns.empty:
        raise ValueError("CSV file has no columns")

    schema = []
    for col in df.columns:
        # Normalize: strip whitespace, replace spaces with underscores
        col_name = str(col).strip().replace(" ", "_")

        dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            sql_type = "INTEGER"
        elif pd.api.types.is_float_dtype(dtype):
            sql_type = "REAL"
        else:
            sql_type = "TEXT"

        schema.append({"name": col_name, "type": sql_type})

    return schema


def get_table_schema(db_path: str, table_name: str) -> list[dict] | None:
    """
    Retrieve existing table schema using PRAGMA table_info().

    Returns:
        List of column dicts [{"name": ..., "type": ...}] if table exists,
        None otherwise.
    """
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(f"PRAGMA table_info({table_name})")
        rows = cursor.fetchall()
        if not rows:
            return None
        # PRAGMA table_info columns: cid, name, type, notnull, dflt_value, pk
        return [{"name": row[1], "type": row[2]} for row in rows]
    finally:
        conn.close()


def get_all_tables(db_path: str) -> list[str]:
    """
    List all user tables in the database.
    """
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%'"
        )
        return [row[0] for row in cursor.fetchall()]
    finally:
        conn.close()


def check_schema_compatibility(existing: list[dict], incoming: list[dict]) -> str:
    """
    Compare two schemas using case-insensitive, whitespace-trimmed column names.

    Filters out the auto-generated 'id' PK from the existing schema before
    comparing so that only data columns are checked.

    Returns:
        "match"    — column names and types align, safe to append
        "mismatch" — schemas differ, caller should create a new table
    """
    # Strip the auto-generated 'id' column from the existing schema
    existing_cols = [
        {"name": c["name"].strip().lower(), "type": c["type"].upper()}
        for c in existing
        if c["name"].lower() != "id"
    ]
    incoming_cols = [
        {"name": c["name"].strip().lower(), "type": c["type"].upper()}
        for c in incoming
    ]

    if len(existing_cols) != len(incoming_cols):
        return "mismatch"

    for e, i in zip(existing_cols, incoming_cols):
        if e["name"] != i["name"] or e["type"] != i["type"]:
            return "mismatch"

    return "match"


def generate_create_table_sql(table_name: str, schema: list[dict]) -> str:
    """
    Generate a CREATE TABLE statement with an auto-increment primary key.

    Output always starts with: id INTEGER PRIMARY KEY AUTOINCREMENT
    """
    columns = ["id INTEGER PRIMARY KEY AUTOINCREMENT"]
    for col in schema:
        columns.append(f"{col['name']} {col['type']}")
    columns_sql = ", ".join(columns)
    return f"CREATE TABLE {table_name} ({columns_sql})"


def get_schema_context(db_path: str) -> str:
    """
    Build a human-readable representation of the full database schema
    for use as LLM prompt context.

    Returns:
        Multi-line string describing every table and its columns.
    """
    tables = get_all_tables(db_path)
    if not tables:
        return "The database has no tables."

    parts = []
    for table in tables:
        schema = get_table_schema(db_path, table)
        col_lines = [f"  - {c['name']} ({c['type']})" for c in schema]
        parts.append(f"Table: {table}\n" + "\n".join(col_lines))

    return "\n\n".join(parts)
