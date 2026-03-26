# Product Requirements Document: EC530 NLQ Engine

**Author:** Jeremiah  
**Course:** EC530 — Building Data Systems with LLM Interfaces (Prof. Osama Alshaykh)  
**Date:** March 26, 2026  
**Due:** April 5, 2026  
**Repository:** `ec530-nlq-engine`

---

## 1. Overview

A modular Python CLI system that loads CSV data into SQLite and enables natural language querying via LLM-powered SQL generation, with built-in query validation and schema management.

### 1.1 Problem Statement

Users with structured data in CSV format need a way to query that data using plain English without writing SQL manually. The system must translate natural language into valid, safe SQL — and must remain correct even when the LLM produces bad output.

### 1.2 Core Principles

- **Separation of concerns** — each module owns one responsibility
- **LLM as component, not system** — the LLM is an untrusted translator, not the decision-maker
- **Defensive by default** — every input (user and LLM) is validated before execution
- **Testability first** — core logic must be testable independently with mocked dependencies

---

## 2. System Architecture

### 2.1 Component Diagram

```
┌─────────────────────────────────────────────────┐
│                    CLI Interface                 │
│            (thin layer, no DB access)            │
└────────────┬───────────────────┬────────────────┘
             │                   │
      ┌──────▼──────┐    ┌──────▼──────┐
      │  CSV Loader  │    │Query Service│
      └──────┬──────┘    └──┬───────┬──┘
             │              │       │
      ┌──────▼──────┐  ┌───▼───┐ ┌─▼──────────┐
      │   Schema    │  │  LLM  │ │  SQL/DB     │
      │   Manager   │  │Adapter│ │  Validator  │
      └──────┬──────┘  └───────┘ └──────┬──────┘
             │                          │
      ┌──────▼──────────────────────────▼──────┐
      │              SQLite Database            │
      └────────────────────────────────────────┘
```

### 2.2 Data Flows

**Ingestion Flow:** CLI → CSV Loader → Schema Manager → SQLite  
**Query Flow:** CLI → Query Service → LLM Adapter → SQL/DB Validator → SQLite → Query Service → CLI

### 2.3 Key Constraint

The CLI must **never** access the database directly. All database interaction flows through the Query Service (for queries) or the CSV Loader + Schema Manager (for ingestion).

---

## 3. Module Specifications

### 3.1 CSV Loader

**Responsibility:** Read CSV files and insert data into SQLite.

**Constraints:**
- MAY use `pandas.read_csv()` for reading
- MAY NOT use `df.to_sql()` — must implement schema creation and INSERT logic manually

**API:**

```python
# csv_loader.py

def load_csv(file_path: str, db_path: str) -> dict:
    """
    Load a CSV file into the SQLite database.
    
    Args:
        file_path: Path to the CSV file
        db_path: Path to the SQLite database
    
    Returns:
        dict with keys:
            - table_name: str — name of the created/appended table
            - rows_inserted: int — number of rows inserted
            - action: str — "created" | "appended"
    
    Raises:
        FileNotFoundError: CSV file does not exist
        ValueError: CSV is empty or malformed
    """
```

**Acceptance Criteria:**
- Reads any well-formed CSV with headers
- Delegates schema decisions to Schema Manager
- Constructs and executes INSERT statements manually (parameterized, not string-formatted)
- Returns metadata about the operation
- Handles empty files, missing headers, and malformed rows gracefully

---

### 3.2 Schema Manager

**Responsibility:** Understand and manage the structure of the database. Does NOT execute data queries or call the LLM.

**API:**

```python
# schema_manager.py

def infer_schema(file_path: str) -> list[dict]:
    """
    Inspect CSV columns and infer SQL types.
    
    Returns:
        List of dicts: [{"name": "col_name", "type": "TEXT|INTEGER|REAL"}]
    """

def get_table_schema(db_path: str, table_name: str) -> list[dict] | None:
    """
    Retrieve existing table schema using PRAGMA table_info().
    
    Returns:
        List of column dicts if table exists, None otherwise.
    """

def get_all_tables(db_path: str) -> list[str]:
    """
    List all user tables in the database.
    """

def check_schema_compatibility(existing: list[dict], incoming: list[dict]) -> str:
    """
    Compare two schemas.
    
    Returns:
        "match" — column names and types align → safe to append
        "mismatch" — schemas differ → prompt user or create new table
    """

def generate_create_table_sql(table_name: str, schema: list[dict]) -> str:
    """
    Generate CREATE TABLE statement with auto-increment primary key.
    
    Output includes: id INTEGER PRIMARY KEY AUTOINCREMENT
    """

def get_schema_context(db_path: str) -> str:
    """
    Build a string representation of the full database schema
    for use as LLM prompt context.
    
    Returns:
        Human-readable schema string (table names, columns, types)
    """
```

**Acceptance Criteria:**
- Correctly infers TEXT, INTEGER, and REAL from CSV column data
- Detects existing tables via `PRAGMA table_info()`
- Normalized column name comparison (case-insensitive, whitespace-trimmed)
- All generated tables include `id INTEGER PRIMARY KEY AUTOINCREMENT`
- Provides schema context string for the LLM Adapter
- Handles edge cases: empty CSV, single-column CSV, duplicate column names

---

### 3.3 SQL/DB Validator

**Responsibility:** Validate SQL queries before execution. This is the security boundary.

**Constraints:**
- Must be designed and API-documented by the student
- Unit tests must be written by the student (no LLM-generated tests)
- Implementation may use LLM as a coding companion
- Must document at least one case where LLM-generated code was incorrect and tests caught it

**API:**

```python
# sql_validator.py

def validate_query(query: str, db_path: str) -> dict:
    """
    Validate a SQL query against the database schema.
    
    Args:
        query: SQL string to validate
        db_path: Path to the SQLite database
    
    Returns:
        dict with keys:
            - valid: bool
            - error: str | None — human-readable reason if invalid
    
    Validation rules:
        - Only SELECT statements allowed
        - All referenced tables must exist in the database
        - All referenced columns must exist in referenced tables
        - No dangerous patterns (DROP, DELETE, INSERT, UPDATE, ALTER, etc.)
        - No special character injection attempts
    """
```

**Acceptance Criteria:**
- Rejects any non-SELECT query (INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, etc.)
- Rejects queries referencing tables not in the database
- Rejects queries referencing columns not in referenced tables
- Handles aliased tables and subqueries at a basic level
- Returns clear, specific error messages
- Protects against SQL injection patterns
- All tests written manually with clear rationale

---

### 3.4 LLM Adapter

**Responsibility:** Translate natural language queries into SQL using an LLM. Does NOT execute SQL.

**API:**

```python
# llm_adapter.py

def natural_language_to_sql(user_query: str, schema_context: str) -> dict:
    """
    Send a natural language query + schema context to the LLM
    and return the generated SQL.
    
    Args:
        user_query: Plain English question from the user
        schema_context: Database schema string from Schema Manager
    
    Returns:
        dict with keys:
            - sql: str — the generated SQL query
            - explanation: str — LLM's explanation of the query
    
    Raises:
        LLMError: API call failed or returned unusable response
    """
```

**Prompt Template (reference):**

```
You are an AI assistant that converts natural language to SQL.
The database uses SQLite and contains the following tables:

{schema_context}

User Query: "{user_query}"

Generate a SQL SELECT query that answers the user's question.
Ensure compatibility with SQLite syntax.

Respond with:
- SQL Query (only the query, no markdown fencing)
- Explanation (one sentence)
```

**Acceptance Criteria:**
- Constructs prompts with full schema context
- Parses LLM response to extract SQL and explanation
- Handles API failures gracefully (timeout, rate limit, malformed response)
- LLM output is treated as untrusted — passed to validator, never executed directly
- Unit tests use mocked LLM responses (no real API calls in tests)

---

### 3.5 Query Service

**Responsibility:** Orchestrate the full query pipeline. The single point of contact between the CLI and the database for queries.

**API:**

```python
# query_service.py

def execute_natural_language_query(user_query: str, db_path: str) -> dict:
    """
    Full pipeline: NL → LLM → SQL → Validate → Execute → Format.
    
    Returns:
        dict with keys:
            - success: bool
            - sql: str — the generated SQL (for transparency)
            - explanation: str — LLM explanation
            - results: list[dict] — query results as list of row dicts
            - error: str | None — error message if any step failed
    """

def execute_raw_sql(query: str, db_path: str) -> dict:
    """
    Validate and execute a raw SQL query (for advanced users).
    
    Same return format as above, minus explanation.
    """

def list_tables(db_path: str) -> list[str]:
    """
    Return available tables. Delegates to Schema Manager.
    """

def describe_table(table_name: str, db_path: str) -> list[dict]:
    """
    Return schema of a specific table. Delegates to Schema Manager.
    """
```

**Acceptance Criteria:**
- Orchestrates LLM Adapter → Validator → Execution → Formatting
- Never executes unvalidated SQL
- Returns structured results with metadata
- Handles failures at any pipeline stage and returns useful errors
- Can operate without LLM (raw SQL mode) for testing and fallback

---

### 3.6 CLI Interface

**Responsibility:** User-facing interface. Thin layer that delegates everything to the Query Service and CSV Loader.

**Commands:**

| Command | Description |
|---------|-------------|
| `load <file.csv>` | Load a CSV file into the database |
| `tables` | List all available tables |
| `schema <table>` | Show schema for a specific table |
| `ask <question>` | Query the database in natural language |
| `sql <query>` | Execute a raw SQL SELECT query |
| `help` | Show available commands |
| `exit` | Quit the application |

**Constraints:**
- CLI must NOT import sqlite3 or access the database directly
- All operations go through Query Service or CSV Loader
- Must handle invalid inputs gracefully with clear error messages

**Acceptance Criteria:**
- Interactive loop via `input()`
- Clean, readable output formatting for query results
- Helpful error messages for all failure modes
- Graceful handling of keyboard interrupts (Ctrl+C)

---

## 4. Project Structure

```
ec530-nlq-engine/
├── README.md
├── requirements.txt
├── .env.example              # Template for API keys (never commit .env)
├── .gitignore
├── .github/
│   └── workflows/
│       └── ci.yml            # GitHub Actions: run pytest on every push
├── src/
│   ├── __init__.py
│   ├── csv_loader.py
│   ├── schema_manager.py
│   ├── sql_validator.py
│   ├── query_service.py
│   ├── llm_adapter.py
│   └── cli.py
├── tests/
│   ├── __init__.py
│   ├── test_csv_loader.py
│   ├── test_schema_manager.py
│   ├── test_sql_validator.py
│   ├── test_query_service.py
│   └── test_llm_adapter.py
├── data/
│   └── sample.csv            # Sample data for testing/demo
├── docs/
│   └── llm_companion_log.md  # Document LLM usage, including error case
└── error_log.txt             # Runtime error logging
```

---

## 5. Testing Strategy

### 5.1 Principles

- Tests define correct behavior — implementation follows
- Each module is tested independently with mocked dependencies
- LLM Adapter tests use mocked responses (no real API calls)
- SQL Validator tests are entirely hand-written

### 5.2 Test Coverage Targets

| Module | Key Test Cases |
|--------|---------------|
| CSV Loader | Valid CSV, empty file, missing headers, type inference edge cases, large file |
| Schema Manager | New table creation, schema match (append), schema mismatch, normalized column names, empty DB |
| SQL Validator | SELECT allowed, INSERT/DELETE/DROP rejected, unknown table, unknown column, SQL injection patterns, aliased queries |
| Query Service | Full pipeline success, LLM returns invalid SQL, validator rejects, DB execution error, empty results |
| LLM Adapter | Successful parse, malformed LLM response, API timeout, missing SQL in response |

### 5.3 CI Pipeline

```yaml
# .github/workflows/ci.yml
name: CI
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install -r requirements.txt
      - run: pytest tests/ -v --tb=short
```

---

## 6. LLM Companion Documentation

### 6.1 Required Documentation (`docs/llm_companion_log.md`)

For the SQL/DB Validator module, maintain a log documenting:

1. **What you asked the LLM** — the prompt or question
2. **What it generated** — the code or logic
3. **What was wrong** — the specific error or gap
4. **How your tests caught it** — which test failed and why
5. **How you fixed it** — the correction you made

At least one complete cycle of this must be documented.

### 6.2 LLM Usage Boundaries

| Allowed | Not Allowed |
|---------|-------------|
| Code review | Generating full solutions |
| Explaining concepts | Copy/pasting full modules |
| Refining unit tests (not generating) | Using LLM output without understanding |
| Implementing functions you designed | LLM defining validator behavior |
| Chat UI per project instructions | Generating unit tests |

---

## 7. Sprint Plan

### Sprint 1: Foundation (March 26–29)

- [ ] Create GitHub repo with project structure
- [ ] Set up `.gitignore`, `requirements.txt`, `.env.example`
- [ ] Configure GitHub Actions CI pipeline
- [ ] Implement CSV Loader (read, validate, insert)
- [ ] Implement SQLite setup utilities
- [ ] Write unit tests for CSV Loader
- [ ] Add Prof. Alshaykh (OsamaBU) and TAs as collaborators

### Sprint 2: Schema + Validation (March 30–April 1)

- [ ] Implement Schema Manager (infer, compare, generate)
- [ ] Write unit tests for Schema Manager
- [ ] Design SQL Validator API and document it
- [ ] Write SQL Validator unit tests (hand-written)
- [ ] Implement SQL Validator (LLM as coding companion)
- [ ] Document LLM companion log with error case
- [ ] Write unit tests for Query Service

### Sprint 3: LLM + Query Pipeline (April 2–3)

- [ ] Implement LLM Adapter (prompt construction, response parsing)
- [ ] Write unit tests for LLM Adapter (mocked responses)
- [ ] Implement Query Service (orchestration layer)
- [ ] Implement CLI Interface
- [ ] End-to-end testing with sample data
- [ ] Test with varied natural language queries

### Sprint 4: Polish + Deliver (April 4–5)

- [ ] Harden edge case handling across all modules
- [ ] Finalize README (overview, setup, run instructions, design decisions)
- [ ] Review and clean commit history
- [ ] Record 5–7 minute system review video
- [ ] Submit GitHub link on Blackboard

---

## 8. Deliverables Checklist

- [ ] Public GitHub repository
- [ ] README with system overview, run instructions, test instructions, design decisions
- [ ] Modular codebase with separation of concerns
- [ ] Unit tests for all modules (pytest)
- [ ] GitHub Actions CI passing
- [ ] LLM companion documentation with error-catch example
- [ ] `.env.example` (no secrets in code)
- [ ] Sample CSV data for demo/testing
- [ ] System review video (5–7 min, not a demo — explain design, testing, LLM integration, one limitation)
- [ ] Blackboard submission with GitHub link

---

## 9. Risk Register

| Risk | Mitigation |
|------|------------|
| LLM generates invalid SQL | Validator catches it; system returns error, never executes |
| LLM API rate limits or downtime | Raw SQL mode as fallback; mocked tests don't depend on API |
| Schema inference misidentifies types | Conservative default to TEXT; tests cover edge cases |
| SQL injection via natural language | Validator enforces SELECT-only + known tables/columns |
| Scope creep (synonym matching, advanced SQL) | Defer to "future nice-to-have"; focus on correctness over features |
| Budget overrun on LLM API | Set hard spending limit ($5); use mocked responses in development |

---

## 10. Non-Goals (Explicit Exclusions)

- No web UI — CLI only
- No multi-database support — SQLite only
- No UPDATE/INSERT/DELETE via natural language — read-only queries
- No synonym-based column matching (noted as future nice-to-have)
- No authentication or multi-user support
- No streaming LLM responses