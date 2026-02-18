"""
Claude API client with retry logic and error handling.
"""

import json
import logging
import os
import sys
import time
import re
from typing import List, Dict, Any, Optional

from anthropic import Anthropic, APIError, RateLimitError, APIConnectionError

# Add this directory to path for local imports
_this_dir = os.path.dirname(os.path.abspath(__file__))
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)

from bulk_config import (
    CLAUDE_MODEL,
    MAX_TOKENS,
    MAX_RETRIES,
    RETRY_BASE_DELAY,
)
from prompts import SYSTEM_PROMPT, build_categorization_prompt, EXAMPLES

logger = logging.getLogger(__name__)


class ClaudeCategorizer:
    """Claude API client for transaction categorization."""

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the client.

        Args:
            api_key: Anthropic API key. If None, uses ANTHROPIC_API_KEY env var.
        """
        self.client = Anthropic(api_key=api_key)
        self.model = CLAUDE_MODEL
        self.total_input_tokens = 0
        self.total_output_tokens = 0

    def categorize_batch(
        self,
        transactions: List[Dict[str, Any]],
        categories: List[Dict[str, Any]],
        include_examples: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Categorize a batch of transactions using Claude API.

        Args:
            transactions: List of transaction dicts with row_number, Description, Amount, etc.
            categories: List of category dicts with category_id, category_name, etc.
            include_examples: Whether to include few-shot examples in prompt

        Returns:
            List of categorization dicts: [{row, category, confidence}, ...]
        """
        # Build prompt
        prompt = build_categorization_prompt(transactions, categories)
        if include_examples:
            prompt = EXAMPLES + "\n\n" + prompt

        # Call API with retries
        response = self._call_with_retry(prompt)

        # Parse response
        categorizations = self._parse_response(response, transactions)

        return categorizations

    def _call_with_retry(self, prompt: str) -> str:
        """Call Claude API with exponential backoff retry."""
        last_error = None

        for attempt in range(MAX_RETRIES):
            try:
                response = self.client.messages.create(
                    model=self.model,
                    max_tokens=MAX_TOKENS,
                    system=SYSTEM_PROMPT,
                    messages=[{"role": "user", "content": prompt}]
                )

                # Track token usage
                self.total_input_tokens += response.usage.input_tokens
                self.total_output_tokens += response.usage.output_tokens

                # Extract text content
                content = response.content[0].text
                return content

            except RateLimitError as e:
                last_error = e
                delay = RETRY_BASE_DELAY * (2 ** attempt)
                logger.warning(f"Rate limited, waiting {delay}s (attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(delay)

            except APIConnectionError as e:
                last_error = e
                delay = RETRY_BASE_DELAY * (2 ** attempt)
                logger.warning(f"Connection error, waiting {delay}s (attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(delay)

            except APIError as e:
                last_error = e
                if e.status_code and e.status_code >= 500:
                    # Server error, retry
                    delay = RETRY_BASE_DELAY * (2 ** attempt)
                    logger.warning(f"Server error {e.status_code}, waiting {delay}s")
                    time.sleep(delay)
                else:
                    # Client error, don't retry
                    raise

        raise last_error or Exception("Max retries exceeded")

    def _parse_response(
        self,
        response: str,
        transactions: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Parse Claude's response into categorization dicts.

        Handles various response formats and edge cases.
        """
        # Try to extract JSON array from response
        json_match = re.search(r'\[[\s\S]*\]', response)
        if not json_match:
            logger.error(f"No JSON array found in response: {response[:500]}")
            return []

        try:
            parsed = json.loads(json_match.group())
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {e}")
            logger.error(f"Response: {response[:500]}")
            return []

        # Validate and normalize
        valid_rows = {t.get('row_number') or t.get('_row_number') for t in transactions}
        categorizations = []

        for item in parsed:
            row = item.get('row')
            category = item.get('category')
            confidence = item.get('confidence', 80)

            if row not in valid_rows:
                logger.warning(f"Unknown row number in response: {row}")
                continue

            if not category:
                logger.warning(f"Missing category for row {row}")
                continue

            categorizations.append({
                'row_number': row,
                'category_id': category,
                'confidence': min(100, max(0, int(confidence))),
                'source': 'claude',
                'needs_review': True,
                'review_reason': 'Bulk categorized by Claude API'
            })

        return categorizations

    def get_usage_stats(self) -> Dict[str, Any]:
        """Get token usage statistics."""
        # Approximate cost calculation (as of 2024)
        # Sonnet: $3/1M input, $15/1M output
        input_cost = (self.total_input_tokens / 1_000_000) * 3.0
        output_cost = (self.total_output_tokens / 1_000_000) * 15.0

        return {
            'input_tokens': self.total_input_tokens,
            'output_tokens': self.total_output_tokens,
            'total_tokens': self.total_input_tokens + self.total_output_tokens,
            'estimated_cost_usd': round(input_cost + output_cost, 4)
        }
