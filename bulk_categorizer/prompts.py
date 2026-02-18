"""
Prompt templates for Claude API categorization.
"""

SYSTEM_PROMPT = """You are a transaction categorizer for a family budget system.

Your job is to assign the correct category_id to each transaction based on the description.

RULES:
1. Use ONLY category_ids from the provided taxonomy
2. Be consistent - same merchant should always get same category
3. When uncertain, use your best judgment based on the description
4. For ambiguous merchants (Target, Walmart, Costco), categorize based on likely purchase type

OUTPUT FORMAT:
Respond with a JSON array of objects, one per transaction:
[
  {"row": 123, "category": "groceries", "confidence": 90},
  {"row": 124, "category": "restaurants", "confidence": 85},
  ...
]

- row: The row_number from the input
- category: The category_id (must be from taxonomy)
- confidence: 0-100, your confidence in the categorization
"""


def format_taxonomy(categories: list) -> str:
    """Format category taxonomy for the prompt."""
    lines = ["CATEGORY TAXONOMY:"]
    lines.append("=" * 50)

    # Group by parent
    by_parent = {}
    for cat in categories:
        parent = cat.get('parent_category', 'Other')
        if parent not in by_parent:
            by_parent[parent] = []
        by_parent[parent].append(cat)

    for parent, cats in sorted(by_parent.items()):
        lines.append(f"\n{parent}:")
        for cat in cats:
            desc = cat.get('description', '')
            lines.append(f"  - {cat['category_id']}: {cat['category_name']}")
            if desc:
                lines.append(f"    ({desc})")

    return '\n'.join(lines)


def format_transactions(transactions: list) -> str:
    """Format transactions for the prompt."""
    lines = ["TRANSACTIONS TO CATEGORIZE:"]
    lines.append("=" * 50)

    for t in transactions:
        row = t.get('row_number') or t.get('_row_number', '?')
        desc = t.get('Description', 'Unknown')
        amount = t.get('Amount', '')
        date = t.get('Date', '')
        hint = t.get('hint', '')

        line = f"[Row {row}] {date} | {desc} | {amount}"
        if hint:
            line += f" (Note: {hint})"
        lines.append(line)

    return '\n'.join(lines)


def build_categorization_prompt(transactions: list, categories: list) -> str:
    """Build the full categorization prompt."""
    parts = [
        format_taxonomy(categories),
        "",
        format_transactions(transactions),
        "",
        "Respond with JSON array only. No explanation needed.",
    ]
    return '\n'.join(parts)


# Example few-shot examples to improve consistency
EXAMPLES = """
EXAMPLES OF CORRECT CATEGORIZATIONS:

[Row 1] 01/15/2024 | WHOLE FOODS MARKET #123 | -$87.50
{"row": 1, "category": "groceries", "confidence": 100}

[Row 2] 01/16/2024 | STARBUCKS #4521 | -$6.75
{"row": 2, "category": "restaurants", "confidence": 95}

[Row 3] 01/17/2024 | TARGET 00012345 | -$156.00
{"row": 3, "category": "supplies", "confidence": 70}
(Target is ambiguous - could be groceries, supplies, clothing, etc. Use best guess.)

[Row 4] 01/18/2024 | SHELL OIL 123456789 | -$45.00
{"row": 4, "category": "fuel", "confidence": 100}

[Row 5] 01/19/2024 | AMAZON MARKETPLACE | -$29.99
{"row": 5, "category": "supplies", "confidence": 60}
(Amazon is ambiguous - categorize based on amount/context if possible)
"""
