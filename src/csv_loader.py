"""
CSV Loader — reads CSV files and inserts data into SQLite.

Responsibilities:
  - Validate that the CSV file exists and is well-formed
  - Delegate schema inference and table creation to Schema Manager
  - Construct and execute parameterized INSERT statements manually
    (df.to_sql() is explicitly prohibited by the PRD)
  - Return metadata about the completed operation
"""

import os
import re
import sqlite3

import pandas as pd

from src.schema_manager import (
    check_schema_compatibility,
    generate_create_table_sql,
    get_table_schema,
    infer_schema,
)


def _table_name_from_path(file_path: str) -> str:
    """
    Derive a SQLite-safe table name from the CSV filename.

    Examples:
        "data/sample.csv"       -> "sample"
        "My Sales Data.csv"     -> "my_sales_data"
        "2024-revenue.csv"      -> "_2024_revenue"  (leading digit fixed)
    """
    base = os.path.splitext(os.path.basename(file_path))[0]
    # Replace non-alphanumeric chars with underscores, collapse runs
    name = re.sub(r"[^a-zA-Z0-9]", "_", base)
    name = re.sub(r"_+", "_", name).strip("_").lower()
    # SQLite identifiers can't start with a digit
    if name and name[0].isdigit():
        name = f"_{name}"
    return name or "imported_data"


def load_csv(file_path: str, db_path: str) -> dict:
    """
    Load a CSV file into the SQLite database.

    Steps:
      1. Validate the file exists and is non-empty
      2. Infer schema via Schema Manager
      3. Check if a table with the same name already exists
         - If it exists and schemas match  -> append rows
         - If it exists and schemas differ -> create a numbered variant
         - If it does not exist            -> create the table
      4. Insert rows using parameterized queries (not df.to_sql)
      5. Return metadata about the operation

    Args:
        file_path: Path to the CSV file
        db_path:   Path to the SQLite database file

    Returns:
        dict with keys:
            - table_name:    str — name of the created/appended table
            - rows_inserted: int — number of rows inserted
            - action:        str — "created" | "appended"

    Raises:
        FileNotFoundError: CSV file does not exist
        ValueError:        CSV is empty or has no data rows
    """
    # --- 1. Validate file ---
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    df = pd.read_csv(file_path)

    if df.empty:
        raise ValueError("CSV file contains no data rows")

    # Normalize column names to match what infer_schema produces
    df.columns = [str(c).strip().replace(" ", "_") for c in df.columns]

    # --- 2. Infer schema ---
    schema = infer_schema(file_path)
    col_names = [col["name"] for col in schema]

    # --- 3. Resolve target table ---
    table_name = _table_name_from_path(file_path)
    action = "created"

    conn = sqlite3.connect(db_path)
    try:
        existing_schema = get_table_schema(db_path, table_name)

        if existing_schema is not None:
            compat = check_schema_compatibility(existing_schema, schema)
            if compat == "match":
                # Schemas align — append into existing table
                action = "appended"
            else:
                # Schemas differ — create a numbered variant (e.g. sample_2)
                counter = 2
                while get_table_schema(db_path, f"{table_name}_{counter}") is not None:
                    counter += 1
                table_name = f"{table_name}_{counter}"
                action = "created"

        # --- 3b. Create table if needed ---
        if action == "created":
            create_sql = generate_create_table_sql(table_name, schema)
            conn.execute(create_sql)
            conn.commit()

        # --- 4. Insert rows with parameterized queries ---
        placeholders = ", ".join(["?"] * len(col_names))
        insert_sql = f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({placeholders})"

        rows_inserted = 0
        for _, row in df.iterrows():
            # Convert each value to its Python-native type so sqlite3
            # receives clean scalars rather than numpy objects.
            values = [
                None if pd.isna(v) else int(v) if isinstance(v, (int,)) or (hasattr(v, "item") and pd.api.types.is_integer_dtype(type(v)))
                else float(v) if isinstance(v, float) else str(v)
                for v in row[col_names]
            ]
            conn.execute(insert_sql, values)
            rows_inserted += 1

        conn.commit()

    finally:
        conn.close()

    # --- 5. Return metadata ---
    return {
        "table_name": table_name,
        "rows_inserted": rows_inserted,
        "action": action,
    }
