"""
LLM Adapter — translates natural language queries into SQL.

Responsibilities:
  - Construct a prompt with schema context and user question
  - Call the OpenAI API (ChatCompletion)
  - Parse the response to extract the SQL query and explanation
  - Never execute SQL — that is the Query Service's job

The adapter treats LLM output as *untrusted*; it merely extracts
and returns strings.  Validation happens downstream.
"""

import os
import re

from dotenv import load_dotenv
from openai import OpenAI, OpenAIError

load_dotenv()


class LLMError(Exception):
    """Raised when the LLM API call fails or returns an unusable response."""


_PROMPT_TEMPLATE = """\
You are an AI assistant that converts natural language to SQL.
The database uses SQLite and contains the following tables:

{schema_context}

User Query: "{user_query}"

Generate a SQL SELECT query that answers the user's question.
Ensure compatibility with SQLite syntax.

Respond in EXACTLY this format (no markdown fencing):
SQL Query: <your SQL here>
Explanation: <one sentence explanation>
"""


def _build_prompt(user_query: str, schema_context: str) -> str:
    """Build the full prompt string from the template."""
    return _PROMPT_TEMPLATE.format(
        schema_context=schema_context,
        user_query=user_query,
    )


def _parse_response(text: str) -> dict:
    """
    Parse the LLM response text into sql and explanation fields.

    Expected format:
        SQL Query: SELECT …
        Explanation: This query …

    Falls back to treating the entire response as SQL if parsing fails.
    """
    sql = None
    explanation = None

    # Try structured format first
    sql_match = re.search(r"SQL Query:\s*(.+?)(?:\n|$)", text, re.IGNORECASE | re.DOTALL)
    expl_match = re.search(r"Explanation:\s*(.+?)(?:\n|$)", text, re.IGNORECASE | re.DOTALL)

    if sql_match:
        sql = sql_match.group(1).strip()
    if expl_match:
        explanation = expl_match.group(1).strip()

    # Fallback: if no structured match, look for a SELECT statement
    if not sql:
        select_match = re.search(r"(SELECT\s.+?)(?:;|$)", text, re.IGNORECASE | re.DOTALL)
        if select_match:
            sql = select_match.group(1).strip()

    if not sql:
        raise LLMError("Could not extract SQL from LLM response")

    # Strip trailing semicolons
    sql = sql.rstrip(";").strip()

    return {
        "sql": sql,
        "explanation": explanation or "No explanation provided",
    }


def natural_language_to_sql(
    user_query: str,
    schema_context: str,
    client: OpenAI | None = None,
    model: str | None = None,
) -> dict:
    """
    Send a natural language query + schema context to the LLM
    and return the generated SQL.

    Args:
        user_query:     Plain English question from the user.
        schema_context: Database schema string from Schema Manager.
        client:         Optional pre-configured OpenAI client (for testing).
        model:          Optional model override (default: gpt-4o-mini).

    Returns:
        dict with keys:
            - sql: str         — the generated SQL query
            - explanation: str — LLM's explanation of the query

    Raises:
        LLMError: API call failed or returned unusable response.
    """
    if client is None:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise LLMError(
                "OPENAI_API_KEY not set. Add it to your .env file."
            )
        client = OpenAI(api_key=api_key)

    prompt = _build_prompt(user_query, schema_context)
    model = model or os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You convert natural language to SQL."},
                {"role": "user", "content": prompt},
            ],
            temperature=0,
            max_tokens=512,
        )
    except OpenAIError as exc:
        raise LLMError(f"OpenAI API error: {exc}") from exc

    content = response.choices[0].message.content
    if not content:
        raise LLMError("LLM returned an empty response")

    return _parse_response(content)
