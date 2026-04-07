"""
SQL/DB Validator — validates SQL queries before execution.

This is the security boundary of the system.  Every query produced by the
LLM Adapter (or typed by the user) must pass through validate_query()
before it touches the database.

Validation rules
----------------
1. Only SELECT statements are allowed.
2. Dangerous keywords (DROP, DELETE, INSERT, UPDATE, ALTER, CREATE, ATTACH,
   DETACH, PRAGMA, GRANT, REVOKE) are rejected regardless of position.
3. All referenced tables must exist in the database.
4. All referenced columns must exist in the referenced tables.
5. Common SQL-injection patterns (comment markers, stacked queries,
   tautologies) are flagged.
"""

import re
import sqlite3

from src.schema_manager import get_all_tables, get_table_schema


# Keywords that must never appear in a user/LLM query
_DANGEROUS_KEYWORDS = [
    "DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE",
    "ATTACH", "DETACH", "PRAGMA", "GRANT", "REVOKE", "EXEC",
    "EXECUTE", "MERGE", "TRUNCATE", "REPLACE",
]

# Regex patterns that suggest injection attempts
_INJECTION_PATTERNS = [
    r";\s*\w",            # stacked queries: "; DROP …"
    r"--",                # SQL line comment
    r"/\*",               # SQL block comment open
    r"\*/",               # SQL block comment close
    r"'\s*OR\s+'",        # tautology: ' OR '
    r"'\s*OR\s+1\s*=\s*1", # tautology: ' OR 1=1
    r"UNION\s+ALL\s+SELECT",  # UNION injection
]


def _is_select_only(query: str) -> bool:
    """Return True if the query starts with SELECT (ignoring whitespace)."""
    stripped = query.strip()
    return stripped.upper().startswith("SELECT")


def _contains_dangerous_keyword(query: str) -> str | None:
    """
    Return the first dangerous keyword found in *query*, or None.

    Uses word-boundary matching so that column names like "updated_at"
    don't trigger false positives.
    """
    upper = query.upper()
    for kw in _DANGEROUS_KEYWORDS:
        if re.search(rf"\b{kw}\b", upper):
            return kw
    return None


def _check_injection_patterns(query: str) -> str | None:
    """Return a description of the first injection pattern found, or None."""
    for pattern in _INJECTION_PATTERNS:
        if re.search(pattern, query, re.IGNORECASE):
            return f"Suspicious pattern detected: {pattern}"
    return None


def _extract_table_names(query: str) -> list[str]:
    """
    Extract table names referenced in a SELECT query.

    Handles:
      - FROM <table>
      - JOIN <table>
      - FROM <table> AS <alias>  /  FROM <table> <alias>
    """
    tables = []

    # Match FROM / JOIN followed by a table name (with optional alias)
    pattern = r"(?:FROM|JOIN)\s+([A-Za-z_]\w*)"
    for m in re.finditer(pattern, query, re.IGNORECASE):
        tables.append(m.group(1).lower())

    return tables


def _extract_column_names(query: str) -> list[str]:
    """
    Extract column names from the SELECT clause.

    Returns an empty list when the query uses SELECT * (wildcard),
    since all columns are implicitly valid.
    """
    # Grab the text between SELECT and FROM
    m = re.search(r"SELECT\s+(.*?)\s+FROM", query, re.IGNORECASE | re.DOTALL)
    if not m:
        return []

    select_clause = m.group(1).strip()

    # Wildcard — all columns valid
    if select_clause == "*":
        return []

    columns = []
    for part in select_clause.split(","):
        part = part.strip()
        # Skip aggregate functions like COUNT(*), SUM(x), etc.
        if "(" in part:
            # Try to extract column inside the function: SUM(price) -> price
            inner = re.search(r"\(([^*)]+)\)", part)
            if inner:
                col = inner.group(1).strip()
                # Handle table.column
                if "." in col:
                    col = col.split(".")[-1].strip()
                columns.append(col.lower())
            continue

        # Handle "column AS alias" — we want the column, not the alias
        if " as " in part.lower():
            part = re.split(r"\s+[Aa][Ss]\s+", part)[0].strip()

        # Handle "table.column"
        if "." in part:
            part = part.split(".")[-1].strip()

        columns.append(part.lower())

    return columns


def _get_valid_columns(db_path: str, table_names: list[str]) -> set[str]:
    """Return the union of all column names across the given tables."""
    valid = set()
    for table in table_names:
        schema = get_table_schema(db_path, table)
        if schema:
            for col in schema:
                valid.add(col["name"].lower())
    return valid


def validate_query(query: str, db_path: str) -> dict:
    """
    Validate a SQL query against the database schema.

    Args:
        query:   SQL string to validate.
        db_path: Path to the SQLite database.

    Returns:
        dict with keys:
            - valid: bool
            - error: str | None — human-readable reason if invalid
    """
    if not query or not query.strip():
        return {"valid": False, "error": "Query is empty"}

    # 1. SELECT only
    if not _is_select_only(query):
        return {"valid": False, "error": "Only SELECT queries are allowed"}

    # 2. Dangerous keywords
    bad_kw = _contains_dangerous_keyword(query)
    if bad_kw:
        return {"valid": False, "error": f"Forbidden keyword: {bad_kw}"}

    # 3. Injection patterns
    injection = _check_injection_patterns(query)
    if injection:
        return {"valid": False, "error": injection}

    # 4. Table existence
    tables_in_query = _extract_table_names(query)
    existing_tables = [t.lower() for t in get_all_tables(db_path)]

    for table in tables_in_query:
        if table not in existing_tables:
            return {"valid": False, "error": f"Table not found: {table}"}

    # 5. Column existence (skip if SELECT *)
    columns_in_query = _extract_column_names(query)
    if columns_in_query and tables_in_query:
        valid_columns = _get_valid_columns(db_path, tables_in_query)
        for col in columns_in_query:
            if col not in valid_columns:
                return {"valid": False, "error": f"Column not found: {col}"}

    return {"valid": True, "error": None}
