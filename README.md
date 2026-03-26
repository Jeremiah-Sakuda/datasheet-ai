# EC530 NLQ Engine

A modular Python CLI that loads CSV data into SQLite and enables natural language querying via LLM-powered SQL generation, with built-in query validation and schema management.

## Architecture

```
CLI Interface
├── CSV Loader → Schema Manager → SQLite
└── Query Service → LLM Adapter → SQL Validator → SQLite
```

**Core principle:** The LLM is an untrusted translator. Every generated query is validated before execution.

## Setup

```bash
# Clone the repo
git clone https://github.com/Jeremiah-Sakuda/datasheet-ai.git
cd datasheet-ai

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure API key
cp .env.example .env
# Edit .env with your OpenAI API key
```

## Usage

```bash
python -m src.cli
```

### Commands

| Command | Description |
|---------|-------------|
| `load <file.csv>` | Load a CSV file into the database |
| `tables` | List all available tables |
| `schema <table>` | Show schema for a specific table |
| `ask <question>` | Query the database in natural language |
| `sql <query>` | Execute a raw SQL SELECT query |
| `help` | Show available commands |
| `exit` | Quit the application |

## Running Tests

```bash
pytest tests/ -v
```

## Project Structure

```
├── src/
│   ├── csv_loader.py       # CSV ingestion into SQLite
│   ├── schema_manager.py   # Schema inference and management
│   ├── sql_validator.py    # Query validation (security boundary)
│   ├── query_service.py    # Query pipeline orchestration
│   ├── llm_adapter.py      # Natural language to SQL translation
│   └── cli.py              # User-facing CLI interface
├── tests/                  # Unit tests for each module
├── data/                   # Sample CSV data
└── docs/                   # LLM companion documentation
```

## Design Decisions

- **SELECT-only queries** — The system only permits read operations, enforced by the SQL Validator
- **Manual INSERT logic** — CSV loading uses parameterized INSERT statements, not `df.to_sql()`
- **Schema context for LLM** — The full database schema is passed to the LLM with every query for accurate SQL generation
- **Defensive validation** — All LLM output is validated against the actual database schema before execution
