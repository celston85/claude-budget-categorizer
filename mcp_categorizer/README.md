# MCP Transaction Categorizer

A Model Context Protocol (MCP) server that enables Claude to categorize budget transactions with deterministic rules and intelligent fallback.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    BUDGET CONFIG SHEET                       │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │ Categories  │  │ Merchant    │  │ Keywords    │         │
│  │             │  │ Rules       │  │             │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    MCP SERVER                                │
│                                                             │
│  Tools:                                                     │
│  • get_uncategorized_transactions(limit, offset)            │
│  • get_category_taxonomy()                                  │
│  • apply_merchant_rules(transactions)                       │
│  • write_categories(updates)                                │
│  • get_categorization_stats()                               │
│  • flag_for_review(transaction_id, reason)                  │
│  • undo_last_batch()                                        │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                 PROCESSED TRANSACTIONS SHEET                 │
│                                                             │
│  Columns added:                                             │
│  • claude_category      - The assigned category             │
│  • category_source      - 'merchant_rule' | 'keyword' |     │
│                           'claude' | 'manual'               │
│  • category_confidence  - 0-100 confidence score            │
│  • categorized_at       - Timestamp                         │
│  • categorized_by       - 'mcp_v1' | 'manual'               │
│  • needs_review         - TRUE if flagged                   │
│  • review_reason        - Why it needs review               │
│  • previous_category    - For undo capability               │
└─────────────────────────────────────────────────────────────┘
```

## Setup

### 1. Install Dependencies

```bash
cd mcp_categorizer
pip install -r requirements.txt
```

### 2. Create Config Sheet

Run the setup script to create your Budget Config sheet:

```bash
python setup_config_sheet.py
```

This creates a new Google Sheet with:
- **Categories** tab: Your budget categories
- **Merchant Rules** tab: Deterministic merchant → category mappings
- **Keywords** tab: Keyword-based category hints

### 3. Configure Environment

Set the following environment variables (or update `config.py`):

```bash
export BUDGET_CONFIG_SHEET_ID='your_config_sheet_id'
export PROCESSED_TRANSACTIONS_SHEET_ID='your_output_sheet_id'
```

### 4. Add to Cursor MCP Config

Add to your `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "budget-categorizer": {
      "command": "python",
      "args": ["/path/to/claude_budget/mcp_categorizer/server.py"],
      "env": {
        "BUDGET_CONFIG_SHEET_ID": "your_config_sheet_id",
        "PROCESSED_TRANSACTIONS_SHEET_ID": "your_output_sheet_id"
      }
    }
  }
}
```

### 5. Restart Cursor

Restart Cursor to load the new MCP server.

## Usage

Once configured, tell Claude:

> "Categorize my uncategorized transactions"

Claude will:
1. Fetch uncategorized transactions
2. Apply merchant rules (deterministic)
3. Apply keyword rules (deterministic)
4. Ask you to confirm Claude-categorized items
5. Write results back to the sheet

## Safeguards

### Deterministic First
- Merchant rules are checked first (100% consistent)
- Keyword rules are checked second (consistent)
- Claude only handles ambiguous transactions

### Audit Trail
- Every categorization records source, confidence, timestamp
- Previous category stored for undo capability

### Validation
- Claude's picks are validated against allowed category list
- Invalid categories are rejected with clear error

### Review Queue
- Low-confidence items flagged for human review
- Ambiguous merchants (Target, Costco) flagged automatically

### Idempotent Writes
- Safe to retry failed operations
- Uses transaction IDs, not row numbers

### Manual Override Protection
- `category_source = 'manual'` rows are never auto-updated

## Configuration

### Categories Tab Schema

| Column | Type | Description |
|--------|------|-------------|
| category_id | string | Unique identifier (e.g., "groceries") |
| category_name | string | Display name (e.g., "Groceries") |
| parent_category | string | Parent for hierarchy (optional) |
| description | string | What belongs in this category |

### Merchant Rules Tab Schema

| Column | Type | Description |
|--------|------|-------------|
| merchant_pattern | string | Pattern to match (case-insensitive) |
| category_id | string | Category to assign |
| confidence | int | 0-100, use 100 for definite matches |
| notes | string | Why this rule exists |

### Keywords Tab Schema

| Column | Type | Description |
|--------|------|-------------|
| keyword | string | Word to match in description |
| category_id | string | Suggested category |
| priority | int | Higher = stronger signal |

## Troubleshooting

### "No uncategorized transactions found"
- Check that `claude_category` column exists in output sheet
- Verify transactions exist with empty `claude_category`

### "Category not in allowed list"
- Verify category_id exists in Categories tab
- Check for typos in merchant rules

### "Authentication failed"
- Re-run OAuth flow: delete `token.json` and restart

## Development

### Running Tests

```bash
python -m pytest tests/
```

### Local Testing Without MCP

```bash
python -c "from categorizer import test_categorization; test_categorization()"
```
