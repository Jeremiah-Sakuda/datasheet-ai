# LLM Companion Log

This document tracks how the LLM was used as a coding companion during development, including cases where LLM-generated code was incorrect and tests caught the errors.

---

## Entry 1: SQL Validator — Dangerous Keyword Detection

### What I asked the LLM

I asked the LLM to help implement the `_contains_dangerous_keyword()` function for the SQL Validator. The goal was to check if a SQL query contained any forbidden keywords like DROP, DELETE, INSERT, etc.

### What it generated

The LLM suggested using a simple substring check:

```python
def _contains_dangerous_keyword(query: str) -> str | None:
    upper = query.upper()
    for kw in _DANGEROUS_KEYWORDS:
        if kw in upper:
            return kw
    return None
```

### What was wrong

This approach produces **false positives** on column names that contain dangerous keywords as substrings. For example:

- A column named `updated_at` would trigger the `UPDATE` keyword check because `"UPDATE" in "UPDATED_AT"` is `True`.
- A column named `created_date` would trigger the `CREATE` keyword check.
- A table named `deleted_records` would trigger the `DELETE` keyword check.

These are all perfectly valid, read-only column/table names that should not be blocked.

### How my tests caught it

I wrote a test specifically for this edge case:

```python
def test_select_keyword_in_column_name_not_blocked(self, db_with_tables):
    # Rationale: a column named "updated_at" should NOT trigger the
    # UPDATE keyword check (word-boundary matching matters)
    result = validate_query("SELECT * FROM employees", db_with_tables)
    assert result["valid"] is True
```

While this specific test passes even with the substring approach (because the query itself doesn't contain "UPDATE"), I constructed additional manual tests during development with queries like `SELECT updated_at FROM ...` that would have failed with the naive substring check.

### How I fixed it

I replaced the simple `in` check with a **word-boundary regex** using `\b`:

```python
def _contains_dangerous_keyword(query: str) -> str | None:
    upper = query.upper()
    for kw in _DANGEROUS_KEYWORDS:
        if re.search(rf"\b{kw}\b", upper):
            return kw
    return None
```

The `\b` anchors ensure that `UPDATE` only matches as a standalone word, not as part of `UPDATED_AT`. This eliminates false positives while still catching actual dangerous statements.

---

## Entry 2: SQL Validator — Column Extraction with Aliases

### What I asked the LLM

I asked the LLM to help extract column names from the SELECT clause for validation against the schema.

### What it generated

The initial approach split on commas and returned each token as a column name, without accounting for `AS` aliases.

### What was wrong

When a query like `SELECT name AS employee_name FROM employees` was validated, the extracted column list would include `employee_name` instead of `name`. Since `employee_name` is the alias and doesn't exist in the schema, the validator would incorrectly reject a valid query.

### How my tests caught it

```python
def test_select_with_alias(self, db_with_tables):
    # Rationale: column aliases should not be flagged as unknown columns
    result = validate_query(
        "SELECT name AS employee_name FROM employees", db_with_tables
    )
    assert result["valid"] is True
```

This test failed until the alias-stripping logic was added.

### How I fixed it

I added logic to strip `AS alias` from column expressions before validation:

```python
# Handle "column AS alias" — we want the column, not the alias
if " as " in part.lower():
    part = re.split(r"\s+[Aa][Ss]\s+", part)[0].strip()
```
