"""Tests for the LLM Adapter module.

All tests use mocked OpenAI responses — no real API calls are made.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.llm_adapter import (
    LLMError,
    _build_prompt,
    _parse_response,
    natural_language_to_sql,
)


# ---------------------------------------------------------------------------
# _build_prompt
# ---------------------------------------------------------------------------

class TestBuildPrompt:
    def test_prompt_contains_schema(self):
        prompt = _build_prompt("show all users", "Table: users\n  - name (TEXT)")
        assert "Table: users" in prompt
        assert "show all users" in prompt

    def test_prompt_contains_user_query(self):
        prompt = _build_prompt("how many rows?", "Table: data\n  - id (INTEGER)")
        assert "how many rows?" in prompt


# ---------------------------------------------------------------------------
# _parse_response
# ---------------------------------------------------------------------------

class TestParseResponse:
    def test_structured_format(self):
        text = (
            "SQL Query: SELECT * FROM employees WHERE age > 30\n"
            "Explanation: This retrieves all employees older than 30."
        )
        result = _parse_response(text)
        assert result["sql"] == "SELECT * FROM employees WHERE age > 30"
        assert "older than 30" in result["explanation"]

    def test_strips_trailing_semicolons(self):
        text = "SQL Query: SELECT name FROM users;\nExplanation: Gets names."
        result = _parse_response(text)
        assert result["sql"] == "SELECT name FROM users"

    def test_fallback_to_raw_select(self):
        # LLM returns just the SQL with no labels
        text = "SELECT COUNT(*) FROM orders;"
        result = _parse_response(text)
        assert result["sql"] == "SELECT COUNT(*) FROM orders"
        assert result["explanation"] == "No explanation provided"

    def test_no_sql_raises_error(self):
        with pytest.raises(LLMError, match="Could not extract SQL"):
            _parse_response("I don't know how to answer that.")

    def test_empty_response_raises_error(self):
        with pytest.raises(LLMError, match="Could not extract SQL"):
            _parse_response("")


# ---------------------------------------------------------------------------
# natural_language_to_sql (mocked API)
# ---------------------------------------------------------------------------

def _mock_client(content: str) -> MagicMock:
    """Create a mock OpenAI client that returns the given content."""
    mock = MagicMock()
    choice = MagicMock()
    choice.message.content = content
    mock.chat.completions.create.return_value = MagicMock(choices=[choice])
    return mock


class TestNaturalLanguageToSql:
    def test_successful_call(self):
        client = _mock_client(
            "SQL Query: SELECT * FROM users\n"
            "Explanation: Returns all users."
        )
        result = natural_language_to_sql(
            "show me all users",
            "Table: users\n  - id (INTEGER)\n  - name (TEXT)",
            client=client,
        )
        assert result["sql"] == "SELECT * FROM users"
        assert "users" in result["explanation"].lower()

    def test_api_error_raises_llm_error(self):
        from openai import OpenAIError

        client = MagicMock()
        client.chat.completions.create.side_effect = OpenAIError("timeout")

        with pytest.raises(LLMError, match="OpenAI API error"):
            natural_language_to_sql("test", "schema", client=client)

    def test_empty_response_raises_llm_error(self):
        client = _mock_client(None)
        # Override so .content returns None
        client.chat.completions.create.return_value.choices[0].message.content = None

        with pytest.raises(LLMError, match="empty response"):
            natural_language_to_sql("test", "schema", client=client)

    def test_malformed_response_raises_llm_error(self):
        client = _mock_client("Sorry, I can't help with that.")

        with pytest.raises(LLMError, match="Could not extract SQL"):
            natural_language_to_sql("test", "schema", client=client)

    def test_missing_api_key_raises_llm_error(self):
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(LLMError, match="OPENAI_API_KEY"):
                natural_language_to_sql("test", "schema", client=None)

    def test_model_override(self):
        client = _mock_client(
            "SQL Query: SELECT 1\nExplanation: test"
        )
        natural_language_to_sql(
            "test", "schema", client=client, model="gpt-3.5-turbo"
        )
        call_kwargs = client.chat.completions.create.call_args
        assert call_kwargs.kwargs["model"] == "gpt-3.5-turbo"
