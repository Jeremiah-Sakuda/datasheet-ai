"""
CLI Interface — user-facing interactive shell.

This is a thin layer that delegates all work to the Query Service
and CSV Loader.  It must NOT import sqlite3 or access the database
directly.

Commands:
    load <file.csv>   — Load a CSV file into the database
    tables            — List all available tables
    schema <table>    — Show schema for a specific table
    ask <question>    — Query the database in natural language
    sql <query>       — Execute a raw SQL SELECT query
    help              — Show available commands
    exit / quit       — Quit the application
"""

import os
import sys

from src.csv_loader import load_csv
from src.query_service import (
    describe_table,
    execute_natural_language_query,
    execute_raw_sql,
    list_tables,
)

DEFAULT_DB_PATH = "data/nlq_engine.db"


def _print_results(results: list[dict]) -> None:
    """Pretty-print query results as a simple table."""
    if not results:
        print("  (no results)")
        return

    columns = list(results[0].keys())
    # Calculate column widths
    widths = {col: len(col) for col in columns}
    for row in results:
        for col in columns:
            widths[col] = max(widths[col], len(str(row.get(col, ""))))

    # Header
    header = " | ".join(col.ljust(widths[col]) for col in columns)
    separator = "-+-".join("-" * widths[col] for col in columns)
    print(f"  {header}")
    print(f"  {separator}")

    # Rows
    for row in results:
        line = " | ".join(str(row.get(col, "")).ljust(widths[col]) for col in columns)
        print(f"  {line}")

    print(f"\n  ({len(results)} row{'s' if len(results) != 1 else ''})")


def _print_schema(schema: list[dict], table_name: str) -> None:
    """Print a table's schema."""
    if not schema:
        print(f"  Table '{table_name}' not found.")
        return

    print(f"  Table: {table_name}")
    for col in schema:
        print(f"    {col['name']:20s} {col['type']}")


def _cmd_load(args: str, db_path: str) -> None:
    """Handle the 'load' command."""
    file_path = args.strip()
    if not file_path:
        print("  Usage: load <file.csv>")
        return

    try:
        result = load_csv(file_path, db_path)
        action = result["action"]
        table = result["table_name"]
        rows = result["rows_inserted"]
        print(f"  {action.capitalize()} table '{table}' with {rows} rows.")
    except FileNotFoundError as exc:
        print(f"  Error: {exc}")
    except ValueError as exc:
        print(f"  Error: {exc}")


def _cmd_tables(db_path: str) -> None:
    """Handle the 'tables' command."""
    tables = list_tables(db_path)
    if not tables:
        print("  No tables loaded. Use 'load <file.csv>' to import data.")
        return
    print("  Available tables:")
    for t in tables:
        print(f"    - {t}")


def _cmd_schema(args: str, db_path: str) -> None:
    """Handle the 'schema' command."""
    table_name = args.strip()
    if not table_name:
        print("  Usage: schema <table_name>")
        return

    schema = describe_table(table_name, db_path)
    _print_schema(schema, table_name)


def _cmd_ask(args: str, db_path: str) -> None:
    """Handle the 'ask' command."""
    question = args.strip()
    if not question:
        print("  Usage: ask <your question>")
        return

    print("  Thinking...")
    result = execute_natural_language_query(question, db_path)

    if result["success"]:
        if result["sql"]:
            print(f"  SQL: {result['sql']}")
        if result["explanation"]:
            print(f"  Explanation: {result['explanation']}")
        print()
        _print_results(result["results"])
    else:
        print(f"  Error: {result['error']}")
        if result.get("sql"):
            print(f"  Generated SQL: {result['sql']}")


def _cmd_sql(args: str, db_path: str) -> None:
    """Handle the 'sql' command."""
    query = args.strip()
    if not query:
        print("  Usage: sql <SELECT query>")
        return

    result = execute_raw_sql(query, db_path)

    if result["success"]:
        _print_results(result["results"])
    else:
        print(f"  Error: {result['error']}")


def _cmd_help() -> None:
    """Print available commands."""
    print("""
  Available commands:
    load <file.csv>   Load a CSV file into the database
    tables            List all available tables
    schema <table>    Show schema for a specific table
    ask <question>    Query the database in natural language
    sql <query>       Execute a raw SQL SELECT query
    help              Show this help message
    exit              Quit the application
""")


def main(db_path: str | None = None) -> None:
    """Run the interactive CLI loop."""
    db_path = db_path or DEFAULT_DB_PATH

    # Ensure the data directory exists
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    print("NLQ Engine — Natural Language Query Engine")
    print("Type 'help' for available commands.\n")

    while True:
        try:
            user_input = input("nlq> ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nGoodbye!")
            break

        if not user_input:
            continue

        # Parse command and arguments
        parts = user_input.split(None, 1)
        command = parts[0].lower()
        args = parts[1] if len(parts) > 1 else ""

        if command in ("exit", "quit"):
            print("Goodbye!")
            break
        elif command == "load":
            _cmd_load(args, db_path)
        elif command == "tables":
            _cmd_tables(db_path)
        elif command == "schema":
            _cmd_schema(args, db_path)
        elif command == "ask":
            _cmd_ask(args, db_path)
        elif command == "sql":
            _cmd_sql(args, db_path)
        elif command == "help":
            _cmd_help()
        else:
            print(f"  Unknown command: '{command}'. Type 'help' for options.")


if __name__ == "__main__":
    main()
