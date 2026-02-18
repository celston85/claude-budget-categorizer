#!/usr/bin/env python3
"""
MCP Server for Transaction Categorization.

Exposes tools for Claude to:
- Read uncategorized transactions
- Get category taxonomy
- Apply merchant rules
- Write categories back to sheet
- Get stats and flag for review
"""

import json
import logging
import os
import subprocess
import sys
from typing import Any, Optional

# Add the mcp_categorizer directory to Python path
# This allows imports to work regardless of where the server is run from
_server_dir = os.path.dirname(os.path.abspath(__file__))
if _server_dir not in sys.path:
    sys.path.insert(0, _server_dir)

# MCP SDK imports
try:
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    from mcp.types import Tool, TextContent
except ImportError:
    print("MCP SDK not installed. Run: pip install mcp", file=sys.stderr)
    sys.exit(1)

from config import (
    MCP_SERVER_NAME,
    MCP_SERVER_VERSION,
    LOG_LEVEL,
    BATCH_SIZE,
    PROCESSED_TRANSACTIONS_SHEET_ID,
    PROCESSED_TRANSACTIONS_SHEET_NAME,
    validate_config,
)
from sheets_client import get_sheets_client
from categorizer import TransactionCategorizer

# ============================================================================
# KEYCHAIN HELPERS
# ============================================================================

def get_anthropic_api_key() -> Optional[str]:
    """
    Fetch Anthropic API key from macOS Keychain.

    Checks (in order):
    1. ANTHROPIC_API_KEY environment variable
    2. macOS Keychain entry (set KEYCHAIN_SERVICE_NAME env var, default: budget-categorizer-api-key)
    """
    # First check env var (for flexibility)
    env_key = os.environ.get('ANTHROPIC_API_KEY')
    if env_key:
        return env_key

    # Try macOS Keychain
    try:
        result = subprocess.run(
            ['security', 'find-generic-password',
             '-s', os.environ.get('KEYCHAIN_SERVICE_NAME', 'budget-categorizer-api-key'), '-w'],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass

    return None


# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

# Initialize MCP server
server = Server(MCP_SERVER_NAME)

# Global categorizer instance (lazy loaded)
_categorizer = None


def get_categorizer() -> TransactionCategorizer:
    """Get or create the categorizer instance."""
    global _categorizer
    
    if _categorizer is None:
        client = get_sheets_client()
        categories = client.get_categories()
        merchant_rules = client.get_merchant_rules()
        keyword_rules = client.get_keywords()
        
        _categorizer = TransactionCategorizer(
            categories=categories,
            merchant_rules=merchant_rules,
            keyword_rules=keyword_rules
        )
    
    return _categorizer


def reload_categorizer():
    """Force reload of categorizer (when config changes)."""
    global _categorizer
    _categorizer = None
    return get_categorizer()


# ============================================================================
# MCP TOOL DEFINITIONS
# ============================================================================

@server.list_tools()
async def list_tools() -> list[Tool]:
    """Return list of available tools."""
    return [
        Tool(
            name="batch_apply_all_rules",
            description="""PRIMARY TOOL: Apply all deterministic rules to ALL uncategorized transactions.

This is the RECOMMENDED first step for categorization. It:
1. Fetches ALL uncategorized transactions (up to 10,000)
2. Applies merchant rules (sorted by specificity - longest patterns first)
3. Applies keyword rules to remaining
4. Writes ALL deterministic results in one batch
5. Returns summary + remaining transactions that truly need Claude

Ambiguous merchants (Amazon, Target, Costco, Walmart) are SKIPPED - left uncategorized
for matching via other means (e.g., Amazon order emails).

After running this, only call Claude for the 'needs_claude' transactions if any remain.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "dry_run": {
                        "type": "boolean",
                        "description": "If true, analyze but don't write to sheet",
                        "default": False
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max transactions to process (default: 10000)",
                        "default": 10000
                    }
                }
            }
        ),
        Tool(
            name="get_uncategorized_transactions",
            description="""Get transactions that don't have a category yet.

Returns a batch of uncategorized transactions ready for categorization.
Each transaction includes: row_number, Date, Description, Amount, Account.

NOTE: Consider using batch_apply_all_rules instead - it processes everything at once.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": f"Max transactions to return (default: {BATCH_SIZE})",
                        "default": BATCH_SIZE
                    },
                    "offset": {
                        "type": "integer",
                        "description": "Number to skip for pagination (default: 0)",
                        "default": 0
                    }
                }
            }
        ),
        Tool(
            name="get_category_taxonomy",
            description="""Get the list of valid budget categories.

Returns all categories with their IDs, names, parent categories, and descriptions.
Use this to understand what categories are available before categorizing transactions.

IMPORTANT: Only use category_ids from this list when writing categories.""",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="apply_rules_to_transactions",
            description="""Apply merchant and keyword rules to transactions.

This applies deterministic rules first:
1. Merchant rules (e.g., "Whole Foods" → "groceries")
2. Keyword rules (e.g., "battery" → "electronics")

Returns:
- auto_categorized: Transactions handled by rules (ready to write)
- needs_claude: Transactions that need your judgment
- needs_review: Auto-categorized but should be reviewed
- skipped: Transactions filtered out (if filter applied)

Use filter_parent_categories or filter_categories to focus on specific categories.

Call this BEFORE making your own categorization decisions.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "transactions": {
                        "type": "array",
                        "description": "List of transactions from get_uncategorized_transactions",
                        "items": {"type": "object"}
                    },
                    "filter_parent_categories": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Only process transactions matching these parent categories (e.g., ['Food', 'Home'])"
                    },
                    "filter_categories": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Only process transactions matching these category_ids (e.g., ['groceries', 'restaurants'])"
                    }
                },
                "required": ["transactions"]
            }
        ),
        Tool(
            name="write_categories",
            description="""Write category assignments back to the sheet.

Each update must include:
- row_number: The row to update (from get_uncategorized_transactions)
- category_id: A valid category ID (from get_category_taxonomy)
- source: 'merchant_rule', 'keyword', or 'claude'
- confidence: 0-100 (your confidence in the categorization)
- needs_review: true if human should verify
- review_reason: optional explanation

This is IDEMPOTENT - safe to retry if there's an error.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "updates": {
                        "type": "array",
                        "description": "List of category updates",
                        "items": {
                            "type": "object",
                            "properties": {
                                "row_number": {"type": "integer"},
                                "category_id": {"type": "string"},
                                "source": {"type": "string", "enum": ["merchant_rule", "keyword", "claude"]},
                                "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
                                "needs_review": {"type": "boolean"},
                                "review_reason": {"type": "string"}
                            },
                            "required": ["row_number", "category_id", "source", "confidence"]
                        }
                    }
                },
                "required": ["updates"]
            }
        ),
        Tool(
            name="get_categorization_stats",
            description="""Get statistics about categorization progress.

Returns:
- total: Total transactions in sheet
- categorized: Number with categories
- uncategorized: Number without categories
- percent_complete: Progress percentage
- by_source: Breakdown by categorization source
- needs_review: Number flagged for review""",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="flag_for_review",
            description="""Flag a specific transaction for human review.

Use this when you're unsure about a categorization or when the
transaction is ambiguous.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "row_number": {
                        "type": "integer",
                        "description": "The row number to flag"
                    },
                    "reason": {
                        "type": "string",
                        "description": "Why this needs review"
                    }
                },
                "required": ["row_number", "reason"]
            }
        ),
        Tool(
            name="reload_config",
            description="""Reload categories, merchant rules, and keywords from config sheet.

Use this if you've updated the Budget Config sheet and want to
pick up the changes without restarting.""",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="validate_category",
            description="""Check if a category_id is valid.

Returns true if the category exists, false otherwise.
Use this to validate before writing if unsure.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "category_id": {
                        "type": "string",
                        "description": "The category ID to validate"
                    }
                },
                "required": ["category_id"]
            }
        ),
        Tool(
            name="add_merchant_rule",
            description="""Add a new merchant rule to the Budget Config sheet.

Use this when the user tells you how to categorize a merchant, and you want
to remember it for future transactions. For example, if they say "Local Grocery
is always groceries", add a rule so it's automatic next time.

The rule will be applied to future categorization runs.
After adding, call reload_config to pick up the new rule immediately.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "merchant_pattern": {
                        "type": "string",
                        "description": "Text pattern to match in transaction descriptions (case-insensitive)"
                    },
                    "category_id": {
                        "type": "string",
                        "description": "Category to assign when pattern matches"
                    },
                    "confidence": {
                        "type": "integer",
                        "description": "Confidence level 0-100 (default: 100 for user-provided rules)",
                        "default": 100
                    },
                    "notes": {
                        "type": "string",
                        "description": "Optional notes about why this rule exists",
                        "default": ""
                    }
                },
                "required": ["merchant_pattern", "category_id"]
            }
        ),
        Tool(
            name="add_keyword",
            description="""Add a new keyword rule to the Budget Config sheet.

Use this to add product-type keywords that help categorize transactions
across multiple merchants. For example, "battery" → electronics.

Keywords are checked after merchant rules and suggest (not guarantee) a category.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "keyword": {
                        "type": "string",
                        "description": "Word to match in transaction descriptions (case-insensitive)"
                    },
                    "category_id": {
                        "type": "string",
                        "description": "Category to suggest when keyword matches"
                    },
                    "priority": {
                        "type": "integer",
                        "description": "Priority 1-100, higher = stronger signal (default: 20)",
                        "default": 20
                    }
                },
                "required": ["keyword", "category_id"]
            }
        ),
        Tool(
            name="audit_merchant_rules",
            description="""Audit merchant rules for potential problems.

Checks for:
1. Patterns too short (<4 chars) - cause false positives
2. Overlapping patterns - one rule may shadow another
3. Invalid category references - category_id doesn't exist
4. Duplicate patterns - same pattern defined twice

Use this to debug unexpected categorization behavior (like CalDigit → groceries).""",
            inputSchema={
                "type": "object",
                "properties": {
                    "test_description": {
                        "type": "string",
                        "description": "Optional: test which rules would match this description"
                    }
                }
            }
        ),
        Tool(
            name="migrate_category",
            description="""Migrate all transactions from one category_id to another.

Use this when renaming or merging categories. Updates all transactions
in the Processed Transactions sheet that have the old category_id.

IMPORTANT: Update the Budget Config sheet (Categories, Merchant Rules, Keywords)
BEFORE calling this tool. This only updates the transaction history.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "old_category_id": {
                        "type": "string",
                        "description": "The category_id to migrate FROM"
                    },
                    "new_category_id": {
                        "type": "string",
                        "description": "The category_id to migrate TO (must exist in Categories)"
                    }
                },
                "required": ["old_category_id", "new_category_id"]
            }
        ),
        # ====================================================================
        # QUERY, UPDATE & REPORTING TOOLS
        # ====================================================================
        Tool(
            name="query_transactions",
            description="""Search and filter transactions with flexible criteria.
Examples:
- 'Show me all restaurant transactions' → query_transactions(category='restaurants')
- 'Find Uber charges over $50' → query_transactions(description_pattern='uber', amount_min=50)
- 'What's flagged for review?' → query_transactions(needs_review=true)
- 'Show uncategorized transactions' → query_transactions(uncategorized_only=true)

Returns paginated results with optional category_summary breakdown.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "category": {
                        "type": "string",
                        "description": "Filter by exact claude_category (e.g., 'groceries')"
                    },
                    "description_pattern": {
                        "type": "string",
                        "description": "Case-insensitive substring match on Description"
                    },
                    "date_from": {
                        "type": "string",
                        "description": "Start date (YYYY-MM-DD)"
                    },
                    "date_to": {
                        "type": "string",
                        "description": "End date (YYYY-MM-DD)"
                    },
                    "source": {
                        "type": "string",
                        "description": "Filter by category_source (merchant_rule, keyword, claude, manual)"
                    },
                    "needs_review": {
                        "type": "boolean",
                        "description": "Filter to only flagged-for-review transactions"
                    },
                    "amount_min": {
                        "type": "number",
                        "description": "Minimum amount (use negative for expenses, e.g., -100)"
                    },
                    "amount_max": {
                        "type": "number",
                        "description": "Maximum amount"
                    },
                    "uncategorized_only": {
                        "type": "boolean",
                        "description": "Only return transactions without a category"
                    },
                    "account": {
                        "type": "string",
                        "description": "Filter by account name (substring match)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results to return (default: 50)",
                        "default": 50
                    },
                    "offset": {
                        "type": "integer",
                        "description": "Skip N results for pagination (default: 0)",
                        "default": 0
                    }
                }
            }
        ),
        Tool(
            name="bulk_update_category",
            description="""Change the category for all transactions matching your criteria.
Examples:
- 'Move Uber Eats from transport to restaurants' → bulk_update_category(description_pattern='uber eats', category='transportation', new_category_id='restaurants')
- 'Recategorize all Costco as groceries' → bulk_update_category(description_pattern='costco', new_category_id='groceries')
IMPORTANT: Use dry_run=true first to preview changes. At least one filter is required.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "new_category_id": {
                        "type": "string",
                        "description": "The category to assign to matching transactions"
                    },
                    "category": {
                        "type": "string",
                        "description": "Filter: current claude_category"
                    },
                    "description_pattern": {
                        "type": "string",
                        "description": "Filter: case-insensitive substring on Description"
                    },
                    "date_from": {"type": "string", "description": "Filter: start date YYYY-MM-DD"},
                    "date_to": {"type": "string", "description": "Filter: end date YYYY-MM-DD"},
                    "source": {"type": "string", "description": "Filter: category_source"},
                    "amount_min": {"type": "number", "description": "Filter: minimum amount"},
                    "amount_max": {"type": "number", "description": "Filter: maximum amount"},
                    "account": {"type": "string", "description": "Filter: account name"},
                    "dry_run": {
                        "type": "boolean",
                        "description": "Preview changes without writing (default: true)",
                        "default": True
                    }
                },
                "required": ["new_category_id"]
            }
        ),
        Tool(
            name="reset_categories",
            description="""Clear categories so transactions can be re-categorized from scratch.
Examples:
- 'Re-categorize everything from keyword rules' → reset_categories(source='keyword')
- 'Clear all miscellaneous' → reset_categories(category='other')
After reset, run batch_apply_all_rules to re-process.
IMPORTANT: Use dry_run=true first. At least one filter is required.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "category": {"type": "string", "description": "Filter: claude_category to reset"},
                    "description_pattern": {"type": "string", "description": "Filter: substring on Description"},
                    "source": {"type": "string", "description": "Filter: category_source to reset"},
                    "date_from": {"type": "string", "description": "Filter: start date YYYY-MM-DD"},
                    "date_to": {"type": "string", "description": "Filter: end date YYYY-MM-DD"},
                    "needs_review": {"type": "boolean", "description": "Filter: only flagged transactions"},
                    "dry_run": {
                        "type": "boolean",
                        "description": "Preview changes without writing (default: true)",
                        "default": True
                    }
                }
            }
        ),
        Tool(
            name="get_spending_summary",
            description="""Get spending totals by category for a time period, with budget comparison.
Examples:
- 'How much did we spend in January?' → get_spending_summary(date_from='2025-01-01', date_to='2025-01-31')
- 'Show me food spending this year' → get_spending_summary(date_from='2025-01-01', date_to='2025-12-31')
Returns hierarchical breakdown: parent categories → categories → totals + budgets.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "date_from": {
                        "type": "string",
                        "description": "Start date YYYY-MM-DD"
                    },
                    "date_to": {
                        "type": "string",
                        "description": "End date YYYY-MM-DD"
                    }
                }
            }
        ),
        Tool(
            name="generate_dashboard",
            description="""Generate a beautiful interactive HTML budget dashboard.
Examples:
- 'Generate my budget dashboard for January' → generate_dashboard(date_from='2025-01-01', date_to='2025-01-31')
- 'Create a dashboard for all of 2025' → generate_dashboard(date_from='2025-01-01', date_to='2025-12-31')
Opens as ~/claude_budget/dashboard.html in your browser.
Includes summary cards, collapsible category groups with budget bars, and click-to-expand transaction tables.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "date_from": {
                        "type": "string",
                        "description": "Start date YYYY-MM-DD"
                    },
                    "date_to": {
                        "type": "string",
                        "description": "End date YYYY-MM-DD"
                    }
                }
            }
        ),
        # ====================================================================
        # EMAIL BACKFILL TOOLS
        # ====================================================================
        Tool(
            name="auto_categorize_batch",
            description="""AUTO-CATEGORIZATION LOOP: Process transactions in batches with merchant rule learning.

WORKFLOW - Call this tool repeatedly:
1. First call: Returns batch of uncategorized transactions for you to categorize
2. You analyze and decide categories for each transaction
3. Next call: Pass your categorizations, tool writes them and returns next batch
4. Repeat until 'continue' is false

MERCHANT RULE LEARNING:
When you categorize the same merchant pattern 2+ times with the same category,
the tool automatically creates a merchant rule so future transactions are deterministic.

INPUT (optional on first call):
- categorizations: Your category decisions from the previous batch
  Format: [{row_number, category_id, confidence}, ...]
- batch_size: Transactions per batch (default 50)

OUTPUT:
- written: Count of categories written
- rules_added: New merchant rules created from patterns
- next_batch: Transactions to categorize (analyze these!)
- stats: Progress summary
- continue: Whether to call again (true = more transactions)

IMPORTANT: Examine 'next_batch' and respond with your categorizations.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "categorizations": {
                        "type": "array",
                        "description": "Your category decisions from the previous batch",
                        "items": {
                            "type": "object",
                            "properties": {
                                "row_number": {"type": "integer"},
                                "category_id": {"type": "string"},
                                "confidence": {"type": "integer", "default": 80}
                            },
                            "required": ["row_number", "category_id"]
                        }
                    },
                    "batch_size": {
                        "type": "integer",
                        "description": "Transactions per batch (default 50)",
                        "default": 50
                    }
                }
            }
        ),
        Tool(
            name="bulk_categorize_api",
            description="""BULK CATEGORIZATION: Process large batches of transactions using Claude API directly.

This tool bypasses Claude Desktop's UI and calls the Claude API directly for efficient bulk processing.
Best for: Initial categorization of 100+ transactions.

API KEY: Retrieved from ANTHROPIC_API_KEY env var or macOS Keychain (see KEYCHAIN_SERVICE_NAME).

WORKFLOW:
1. Fetches uncategorized transactions
2. Applies deterministic rules first (merchant/keyword)
3. Sends remaining to Claude API in batches
4. Auto-learns merchant rules from repeated patterns
5. Writes all results to sheet

OPTIONS:
- max_transactions: Total to process (default: 1000)
- batch_size: Per API call (default: 100)
- dry_run: Test without writing (default: false)
- learn_rules: Auto-create merchant rules (default: true)
- filter_parent: Only process categories under this parent (e.g., "Food")

COST: ~$0.04 per 100 transactions using claude-sonnet-4-20250514""",
            inputSchema={
                "type": "object",
                "properties": {
                    "max_transactions": {
                        "type": "integer",
                        "description": "Max transactions to process (default: 1000)",
                        "default": 1000
                    },
                    "batch_size": {
                        "type": "integer",
                        "description": "Transactions per API call (default: 100)",
                        "default": 100
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": "Test without writing to sheet",
                        "default": False
                    },
                    "learn_rules": {
                        "type": "boolean",
                        "description": "Auto-create merchant rules from patterns",
                        "default": True
                    },
                    "filter_parent": {
                        "type": "string",
                        "description": "Only process categories under this parent (e.g., 'Food', 'Home')"
                    }
                }
            }
        ),
        Tool(
            name="run_email_backfill",
            description="""Run the email backfill script to fetch Amazon order/return emails from Gmail.

IMPORTANT: Before calling this, ASK THE USER which mode they want:

1. **optimized_incremental** (recommended for regular use)
   - Only fetches NEW emails since last run
   - Fast, uses batch API
   - Best for: daily/weekly syncs

2. **optimized** 
   - Fetches ALL matching emails (full backfill)
   - Uses batch API + parallel requests (3-5x faster than legacy)
   - Best for: initial setup, re-syncing everything

3. **legacy**
   - Sequential fetching (slower but simpler)
   - Best for: debugging, testing with small limits

The script writes to the Parsed Orders sheet, which feeds into transaction matching.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "mode": {
                        "type": "string",
                        "enum": ["optimized_incremental", "optimized", "legacy"],
                        "description": "Fetching mode - ASK USER before choosing"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max emails to process (optional, for testing)"
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Start date YYYY-MM-DD (optional)"
                    },
                    "end_date": {
                        "type": "string",
                        "description": "End date YYYY-MM-DD (optional)"
                    },
                    "email_type": {
                        "type": "string",
                        "enum": ["all", "orders", "shipments", "returns"],
                        "description": "Which email types to process (default: all)",
                        "default": "all"
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": "If true, parse but don't write to sheet",
                        "default": False
                    }
                },
                "required": ["mode"]
            }
        ),
        Tool(
            name="run_transaction_matcher",
            description="""Run the transaction matcher to match bank transactions with parsed Amazon orders.

IMPORTANT: Before calling this, ASK THE USER which mode they want:

1. **production** (recommended for regular use)
   - Appends new rows, skips already-processed transactions
   - Preserves existing data
   - Best for: daily/regular runs

2. **development**
   - Clears output sheet, reprocesses ALL transactions
   - Best for: testing, debugging, full refresh

The matcher reads from:
- Source Transactions sheet (bank data)
- Parsed Orders sheet (from email backfill)

And writes to:
- Processed Transactions sheet (matched + expanded data)""",
            inputSchema={
                "type": "object",
                "properties": {
                    "mode": {
                        "type": "string",
                        "enum": ["production", "development"],
                        "description": "Processing mode - ASK USER before choosing"
                    }
                },
                "required": ["mode"]
            }
        ),
    ]


# ============================================================================
# MCP TOOL IMPLEMENTATIONS
# ============================================================================

@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    """Handle tool calls."""
    
    try:
        if name == "get_uncategorized_transactions":
            client = get_sheets_client()
            limit = arguments.get('limit', BATCH_SIZE)
            offset = arguments.get('offset', 0)
            
            transactions = client.get_uncategorized_transactions(limit=limit, offset=offset)
            
            # Simplify output for Claude
            simplified = []
            for t in transactions:
                simplified.append({
                    'row_number': t.get('_row_number'),
                    'Date': t.get('Date', ''),
                    'Description': t.get('Description', ''),
                    'Amount': t.get('Amount', ''),
                    'Account': t.get('Account', ''),
                })
            
            return [TextContent(
                type="text",
                text=json.dumps({
                    'count': len(simplified),
                    'transactions': simplified
                }, indent=2)
            )]

        elif name == "batch_apply_all_rules":
            client = get_sheets_client()
            categorizer = get_categorizer()
            dry_run = arguments.get('dry_run', False)
            limit = arguments.get('limit', 10000)

            # Step 1: Fetch ALL uncategorized transactions
            logger.info(f"Fetching up to {limit} uncategorized transactions...")
            transactions = client.get_uncategorized_transactions(limit=limit, offset=0)

            if not transactions:
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        'success': True,
                        'message': 'No uncategorized transactions found',
                        'summary': {
                            'total_processed': 0,
                            'auto_categorized': 0,
                            'written': 0,
                            'needs_claude': 0,
                            'skipped_ambiguous': 0
                        }
                    }, indent=2)
                )]

            # Step 2: Apply all rules in one batch
            logger.info(f"Applying rules to {len(transactions)} transactions...")
            results = categorizer.categorize_batch(transactions)

            # Step 3: Prepare updates for auto-categorized transactions
            updates = []
            for t in results['auto_categorized']:
                cat_info = t['_categorization']
                updates.append({
                    'row_number': t.get('_row_number'),
                    'category_id': cat_info['category_id'],
                    'source': cat_info['source'],
                    'confidence': cat_info['confidence'],
                    'needs_review': cat_info.get('needs_review', False),
                    'review_reason': cat_info.get('review_reason', '')
                })

            # Step 4: Write all results (unless dry run)
            write_result = {'success_count': 0, 'error_count': 0}
            if updates and not dry_run:
                logger.info(f"Writing {len(updates)} categorizations...")
                write_result = client.write_categories(updates)

            # Step 5: Build response
            output = {
                'success': True,
                'dry_run': dry_run,
                'summary': {
                    'total_processed': len(transactions),
                    'auto_categorized': len(results['auto_categorized']),
                    'written': write_result.get('success_count', 0) if not dry_run else 0,
                    'write_errors': write_result.get('error_count', 0),
                    'needs_claude': len(results['needs_claude']),
                    'needs_review': len(results['needs_review']),
                },
                'by_source': {},
                'needs_claude': [
                    {
                        'row_number': t.get('_row_number'),
                        'Description': t.get('Description', ''),
                        'Amount': t.get('Amount', ''),
                        'hint': t['_categorization'].get('review_reason', 'No rules matched'),
                    }
                    for t in results['needs_claude'][:50]  # Limit to first 50
                ],
            }

            # Count by source
            for t in results['auto_categorized']:
                source = t['_categorization'].get('source', 'unknown')
                output['by_source'][source] = output['by_source'].get(source, 0) + 1

            # Add note if there are more needs_claude than shown
            if len(results['needs_claude']) > 50:
                output['needs_claude_note'] = f"Showing 50 of {len(results['needs_claude'])} - use get_uncategorized_transactions for full list"

            return [TextContent(
                type="text",
                text=json.dumps(output, indent=2)
            )]

        elif name == "get_category_taxonomy":
            categorizer = get_categorizer()
            taxonomy = categorizer.get_category_taxonomy()
            
            return [TextContent(
                type="text",
                text=json.dumps(taxonomy, indent=2)
            )]
        
        elif name == "apply_rules_to_transactions":
            categorizer = get_categorizer()
            client = get_sheets_client()
            transactions = arguments.get('transactions', [])
            filter_parent_categories = arguments.get('filter_parent_categories', [])
            filter_categories = arguments.get('filter_categories', [])
            
            # Re-add _row_number as it may have been stripped
            for t in transactions:
                if 'row_number' in t and '_row_number' not in t:
                    t['_row_number'] = t['row_number']
            
            results = categorizer.categorize_batch(transactions)
            
            # Build filter set if filters specified
            allowed_category_ids = set()
            if filter_parent_categories or filter_categories:
                # Get categories to build parent->category mapping
                categories = client.get_categories()
                
                # Add categories matching parent filter
                if filter_parent_categories:
                    for cat in categories:
                        if cat.get('parent_category') in filter_parent_categories:
                            allowed_category_ids.add(cat['category_id'])
                
                # Add explicitly filtered categories
                if filter_categories:
                    allowed_category_ids.update(filter_categories)
                
                logger.info(f"Filtering to categories: {allowed_category_ids}")
            
            # Filter function
            def matches_filter(transaction):
                if not allowed_category_ids:
                    return True  # No filter, include all
                cat_info = transaction.get('_categorization', {})
                cat_id = cat_info.get('category_id', '')
                return cat_id in allowed_category_ids
            
            # Apply filters
            if allowed_category_ids:
                filtered_auto = [t for t in results['auto_categorized'] if matches_filter(t)]
                filtered_needs_claude = [t for t in results['needs_claude'] if matches_filter(t)]
                skipped_count = (
                    len(results['auto_categorized']) - len(filtered_auto) +
                    len(results['needs_claude']) - len(filtered_needs_claude)
                )
            else:
                filtered_auto = results['auto_categorized']
                filtered_needs_claude = results['needs_claude']
                skipped_count = 0
            
            # Format output
            output = {
                'summary': {
                    'auto_categorized': len(filtered_auto),
                    'needs_claude': len(filtered_needs_claude),
                    'needs_review': len(results['needs_review']),
                    'skipped_by_filter': skipped_count,
                },
                'filter_applied': {
                    'parent_categories': filter_parent_categories,
                    'categories': filter_categories,
                    'allowed_category_ids': list(allowed_category_ids) if allowed_category_ids else None,
                } if (filter_parent_categories or filter_categories) else None,
                'auto_categorized': [
                    {
                        'row_number': t.get('_row_number'),
                        'Description': t.get('Description', ''),
                        'category_id': t['_categorization']['category_id'],
                        'category_name': t['_categorization'].get('category_name', ''),
                        'source': t['_categorization']['source'],
                        'confidence': t['_categorization']['confidence'],
                        'needs_review': t['_categorization'].get('needs_review', False),
                        'review_reason': t['_categorization'].get('review_reason'),
                    }
                    for t in filtered_auto
                ],
                'needs_claude': [
                    {
                        'row_number': t.get('_row_number'),
                        'Description': t.get('Description', ''),
                        'Amount': t.get('Amount', ''),
                        'hint': t['_categorization'].get('review_reason', 'No rules matched'),
                    }
                    for t in filtered_needs_claude
                ],
            }
            
            return [TextContent(
                type="text",
                text=json.dumps(output, indent=2)
            )]
        
        elif name == "write_categories":
            client = get_sheets_client()
            categorizer = get_categorizer()
            updates = arguments.get('updates', [])
            
            # Validate all category_ids before writing
            invalid = []
            for update in updates:
                if not categorizer.validate_category(update.get('category_id', '')):
                    invalid.append({
                        'row_number': update.get('row_number'),
                        'invalid_category': update.get('category_id')
                    })
            
            if invalid:
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        'error': 'Invalid category IDs',
                        'invalid': invalid,
                        'valid_categories': list(categorizer.category_ids)
                    }, indent=2)
                )]
            
            result = client.write_categories(updates)
            
            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]
        
        elif name == "get_categorization_stats":
            client = get_sheets_client()
            stats = client.get_categorization_stats()
            
            return [TextContent(
                type="text",
                text=json.dumps(stats, indent=2)
            )]
        
        elif name == "flag_for_review":
            client = get_sheets_client()
            row_number = arguments.get('row_number')
            reason = arguments.get('reason', 'Flagged for review')
            
            success = client.flag_for_review(row_number, reason)
            
            return [TextContent(
                type="text",
                text=json.dumps({
                    'success': success,
                    'row_number': row_number,
                    'reason': reason
                })
            )]
        
        elif name == "reload_config":
            categorizer = reload_categorizer()
            
            return [TextContent(
                type="text",
                text=json.dumps({
                    'success': True,
                    'categories_loaded': len(categorizer.categories),
                    'merchant_rules_loaded': len(categorizer.merchant_rules),
                    'keyword_rules_loaded': len(categorizer.keyword_rules),
                })
            )]
        
        elif name == "validate_category":
            categorizer = get_categorizer()
            category_id = arguments.get('category_id', '')
            is_valid = categorizer.validate_category(category_id)
            
            return [TextContent(
                type="text",
                text=json.dumps({
                    'category_id': category_id,
                    'is_valid': is_valid,
                    'valid_categories': list(categorizer.category_ids) if not is_valid else None
                })
            )]
        
        elif name == "audit_merchant_rules":
            categorizer = get_categorizer()
            test_description = arguments.get('test_description', '')

            issues = []
            rules = categorizer.merchant_rules

            # Check 1: Patterns too short
            MIN_LENGTH = 4
            short_patterns = [r for r in rules if len(r['merchant_pattern']) < MIN_LENGTH]
            for r in short_patterns:
                issues.append({
                    'type': 'short_pattern',
                    'severity': 'high',
                    'pattern': r['merchant_pattern'],
                    'category': r['category_id'],
                    'message': f"Pattern '{r['merchant_pattern']}' is only {len(r['merchant_pattern'])} chars - may cause false positives"
                })

            # Check 2: Overlapping patterns
            for i, r1 in enumerate(rules):
                p1 = r1['merchant_pattern']
                for r2 in rules[i+1:]:
                    p2 = r2['merchant_pattern']
                    if p1 != p2:
                        if p1 in p2:
                            issues.append({
                                'type': 'overlap',
                                'severity': 'medium',
                                'pattern1': p1,
                                'category1': r1['category_id'],
                                'pattern2': p2,
                                'category2': r2['category_id'],
                                'message': f"'{p1}' is substring of '{p2}' - may shadow matches"
                            })
                        elif p2 in p1:
                            issues.append({
                                'type': 'overlap',
                                'severity': 'medium',
                                'pattern1': p2,
                                'category1': r2['category_id'],
                                'pattern2': p1,
                                'category2': r1['category_id'],
                                'message': f"'{p2}' is substring of '{p1}' - may shadow matches"
                            })

            # Check 3: Invalid category references
            for r in rules:
                if not categorizer.validate_category(r['category_id']):
                    issues.append({
                        'type': 'invalid_category',
                        'severity': 'high',
                        'pattern': r['merchant_pattern'],
                        'category': r['category_id'],
                        'message': f"Category '{r['category_id']}' does not exist"
                    })

            # Check 4: Duplicate patterns
            seen = {}
            for r in rules:
                p = r['merchant_pattern']
                if p in seen:
                    issues.append({
                        'type': 'duplicate',
                        'severity': 'medium',
                        'pattern': p,
                        'category1': seen[p],
                        'category2': r['category_id'],
                        'message': f"Pattern '{p}' defined twice"
                    })
                else:
                    seen[p] = r['category_id']

            # Optional: Test which rules match a description
            matching_rules = []
            if test_description:
                desc_lower = test_description.lower()
                for r in rules:
                    if r['merchant_pattern'] in desc_lower:
                        matching_rules.append({
                            'pattern': r['merchant_pattern'],
                            'category': r['category_id'],
                            'confidence': r['confidence'],
                            'would_match': True
                        })

            output = {
                'total_rules': len(rules),
                'issues_found': len(issues),
                'issues': issues,
                'rules_sorted_by': 'length (longest first for determinism)',
            }

            if test_description:
                output['test_description'] = test_description
                output['matching_rules'] = matching_rules
                output['first_match'] = matching_rules[0] if matching_rules else None

            return [TextContent(
                type="text",
                text=json.dumps(output, indent=2)
            )]

        elif name == "add_merchant_rule":
            client = get_sheets_client()
            categorizer = get_categorizer()
            
            merchant_pattern = arguments.get('merchant_pattern', '')
            category_id = arguments.get('category_id', '')
            confidence = arguments.get('confidence', 100)
            notes = arguments.get('notes', '')
            
            # Validate category exists
            if not categorizer.validate_category(category_id):
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        'success': False,
                        'error': f"Invalid category_id: {category_id}",
                        'valid_categories': list(categorizer.category_ids)
                    })
                )]
            
            result = client.add_merchant_rule(
                merchant_pattern=merchant_pattern,
                category_id=category_id,
                confidence=confidence,
                notes=notes
            )
            
            # Auto-reload config if successful
            if result.get('success'):
                reload_categorizer()
                result['config_reloaded'] = True
            
            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]
        
        elif name == "add_keyword":
            client = get_sheets_client()
            categorizer = get_categorizer()
            
            keyword = arguments.get('keyword', '')
            category_id = arguments.get('category_id', '')
            priority = arguments.get('priority', 20)
            
            # Validate category exists
            if not categorizer.validate_category(category_id):
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        'success': False,
                        'error': f"Invalid category_id: {category_id}",
                        'valid_categories': list(categorizer.category_ids)
                    })
                )]
            
            result = client.add_keyword(
                keyword=keyword,
                category_id=category_id,
                priority=priority
            )
            
            # Auto-reload config if successful
            if result.get('success'):
                reload_categorizer()
                result['config_reloaded'] = True
            
            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]
        
        elif name == "migrate_category":
            client = get_sheets_client()
            categorizer = get_categorizer()
            
            old_category_id = arguments.get('old_category_id', '')
            new_category_id = arguments.get('new_category_id', '')
            
            # Validate new category exists
            if not categorizer.validate_category(new_category_id):
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        'success': False,
                        'error': f"Target category '{new_category_id}' does not exist",
                        'valid_categories': list(categorizer.category_ids),
                        'suggestion': 'Update the Categories tab first, then reload config'
                    })
                )]
            
            result = client.migrate_category(
                old_category_id=old_category_id,
                new_category_id=new_category_id
            )
            
            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]
        
        elif name == "query_transactions":
            client = get_sheets_client()
            # Build filters from arguments
            filter_keys = ['category', 'description_pattern', 'date_from', 'date_to',
                           'source', 'needs_review', 'amount_min', 'amount_max',
                           'uncategorized_only', 'account']
            filters = {k: arguments[k] for k in filter_keys if k in arguments and arguments[k] is not None}
            limit = arguments.get('limit', 50)
            offset = arguments.get('offset', 0)

            result = client.query_transactions(filters=filters, limit=limit, offset=offset)

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]

        elif name == "bulk_update_category":
            client = get_sheets_client()
            categorizer = get_categorizer()

            new_category_id = arguments.get('new_category_id', '')
            dry_run = arguments.get('dry_run', True)

            # Validate category
            if not categorizer.validate_category(new_category_id):
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        'success': False,
                        'error': f"Invalid category_id: {new_category_id}",
                        'valid_categories': list(categorizer.category_ids)
                    })
                )]

            # Build filters (require at least one)
            filter_keys = ['category', 'description_pattern', 'date_from', 'date_to',
                           'source', 'amount_min', 'amount_max', 'account']
            filters = {k: arguments[k] for k in filter_keys if k in arguments and arguments[k] is not None}

            if not filters:
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        'success': False,
                        'error': 'At least one filter is required to prevent accidental bulk updates',
                        'available_filters': filter_keys
                    })
                )]

            result = client.bulk_update_category(
                filters=filters,
                new_category_id=new_category_id,
                dry_run=dry_run
            )

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]

        elif name == "reset_categories":
            client = get_sheets_client()
            dry_run = arguments.get('dry_run', True)

            # Build filters (require at least one)
            filter_keys = ['category', 'description_pattern', 'source', 'date_from',
                           'date_to', 'needs_review']
            filters = {k: arguments[k] for k in filter_keys if k in arguments and arguments[k] is not None}

            if not filters:
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        'success': False,
                        'error': 'At least one filter is required to prevent accidental mass reset',
                        'available_filters': filter_keys
                    })
                )]

            result = client.reset_categories(filters=filters, dry_run=dry_run)

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]

        elif name == "get_spending_summary":
            client = get_sheets_client()
            date_from = arguments.get('date_from')
            date_to = arguments.get('date_to')

            # Get category taxonomy for parent/budget mapping
            categories = client.get_categories()

            result = client.get_spending_summary(
                date_from=date_from,
                date_to=date_to,
                category_taxonomy=categories
            )

            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2)
            )]

        elif name == "generate_dashboard":
            from dashboard_generator import generate_dashboard_html

            client = get_sheets_client()
            date_from = arguments.get('date_from')
            date_to = arguments.get('date_to')

            # Get summary data
            categories = client.get_categories()
            summary = client.get_spending_summary(
                date_from=date_from,
                date_to=date_to,
                category_taxonomy=categories
            )

            # Get all transactions for drill-down (high limit)
            filters = {}
            if date_from:
                filters['date_from'] = date_from
            if date_to:
                filters['date_to'] = date_to
            query_result = client.query_transactions(filters=filters, limit=10000, offset=0)
            transactions = query_result.get('transactions', [])

            # Generate HTML
            output_path = generate_dashboard_html(summary, transactions)

            return [TextContent(
                type="text",
                text=json.dumps({
                    'success': True,
                    'file': output_path,
                    'period': summary.get('period', {}),
                    'total_transactions': len(transactions),
                    'total_income': summary.get('total_income', 0),
                    'total_expenses': summary.get('total_expenses', 0),
                    'net': summary.get('net', 0),
                    'message': f'Dashboard saved to {output_path} — open in browser to view'
                }, indent=2)
            )]

        elif name == "auto_categorize_batch":
            client = get_sheets_client()
            categorizer = get_categorizer()
            categorizations = arguments.get('categorizations', [])
            batch_size = arguments.get('batch_size', 50)

            results = {
                'written': 0,
                'write_errors': [],
                'rules_added': [],
                'next_batch': [],
                'stats': {},
                'continue': False
            }

            # Step 1: Write previous categorizations if provided
            if categorizations:
                # Validate and prepare updates
                updates = []
                for cat in categorizations:
                    row_num = cat.get('row_number')
                    category_id = cat.get('category_id')
                    confidence = cat.get('confidence', 80)

                    if not row_num or not category_id:
                        results['write_errors'].append({'error': 'Missing row_number or category_id', 'input': cat})
                        continue

                    if not categorizer.validate_category(category_id):
                        results['write_errors'].append({'error': f'Invalid category: {category_id}', 'row': row_num})
                        continue

                    updates.append({
                        'row_number': row_num,
                        'category_id': category_id,
                        'source': 'claude',
                        'confidence': confidence,
                        'needs_review': True,  # Always flag Claude decisions for review
                        'review_reason': 'Auto-categorized by Claude'
                    })

                if updates:
                    write_result = client.write_categories(updates)
                    results['written'] = write_result.get('success_count', 0)

            # Step 2: Analyze patterns for rule learning
            # Get recent Claude categorizations to find repeated patterns
            try:
                service = client._get_service()
                all_data = service.spreadsheets().values().get(
                    spreadsheetId=PROCESSED_TRANSACTIONS_SHEET_ID,
                    range=f"'{PROCESSED_TRANSACTIONS_SHEET_NAME}'!A:Z"
                ).execute()

                headers, col_indices = client._get_headers()
                values = all_data.get('values', [])

                if len(values) > 1:
                    desc_idx = col_indices.get('Description', col_indices.get('description'))
                    cat_idx = col_indices.get('claude_category')
                    source_idx = col_indices.get('category_source')

                    # Count merchant patterns from Claude categorizations
                    pattern_counts = {}  # pattern -> {category_id: count}

                    for row in values[1:]:
                        if len(row) <= max(desc_idx or 0, cat_idx or 0, source_idx or 0):
                            continue

                        source = row[source_idx].strip().lower() if source_idx and source_idx < len(row) else ''
                        if source != 'claude':
                            continue

                        desc = row[desc_idx].strip().lower() if desc_idx and desc_idx < len(row) else ''
                        category = row[cat_idx].strip() if cat_idx and cat_idx < len(row) else ''

                        if not desc or not category:
                            continue

                        # Extract potential merchant pattern (first 2-3 words, normalized)
                        words = desc.split()[:3]
                        pattern = ' '.join(words)

                        # Only consider patterns 4+ chars
                        if len(pattern) < 4:
                            continue

                        if pattern not in pattern_counts:
                            pattern_counts[pattern] = {}
                        pattern_counts[pattern][category] = pattern_counts[pattern].get(category, 0) + 1

                    # Find patterns that appear 2+ times with same category and don't have rules yet
                    existing_rules = {r['merchant_pattern'] for r in categorizer.merchant_rules}

                    for pattern, cat_counts in pattern_counts.items():
                        # Get the most common category for this pattern
                        top_category = max(cat_counts, key=cat_counts.get)
                        count = cat_counts[top_category]

                        # If 2+ occurrences and no existing rule, create one
                        if count >= 2 and pattern not in existing_rules:
                            # Check if pattern is substring of existing rule
                            is_covered = any(pattern in r or r in pattern for r in existing_rules)
                            if not is_covered:
                                add_result = client.add_merchant_rule(
                                    merchant_pattern=pattern,
                                    category_id=top_category,
                                    confidence=90,
                                    notes=f"Auto-learned from {count} Claude categorizations"
                                )
                                if add_result.get('success'):
                                    results['rules_added'].append({
                                        'pattern': pattern,
                                        'category': top_category,
                                        'occurrences': count
                                    })

                    # Reload categorizer if rules were added
                    if results['rules_added']:
                        reload_categorizer()

            except Exception as e:
                logger.warning(f"Error in pattern learning: {e}")

            # Step 3: Get next batch of uncategorized
            transactions = client.get_uncategorized_transactions(limit=batch_size, offset=0)

            # Apply rules first
            if transactions:
                batch_results = categorizer.categorize_batch(transactions)

                # Auto-write rule matches
                auto_updates = []
                for t in batch_results['auto_categorized']:
                    cat_info = t['_categorization']
                    auto_updates.append({
                        'row_number': t.get('_row_number'),
                        'category_id': cat_info['category_id'],
                        'source': cat_info['source'],
                        'confidence': cat_info['confidence'],
                        'needs_review': cat_info.get('needs_review', False),
                        'review_reason': cat_info.get('review_reason', '')
                    })

                if auto_updates:
                    client.write_categories(auto_updates)
                    results['auto_categorized'] = len(auto_updates)

                # Return needs_claude for Claude to categorize
                results['next_batch'] = [
                    {
                        'row_number': t.get('_row_number'),
                        'Date': t.get('Date', ''),
                        'Description': t.get('Description', ''),
                        'Amount': t.get('Amount', ''),
                        'Account': t.get('Account', ''),
                        'hint': t['_categorization'].get('review_reason', '')
                    }
                    for t in batch_results['needs_claude']
                ]

            # Step 4: Get stats
            stats = client.get_categorization_stats()
            results['stats'] = {
                'total': stats.get('total', 0),
                'categorized': stats.get('categorized', 0),
                'remaining': stats.get('uncategorized', 0),
                'percent_complete': stats.get('percent_complete', 0),
            }

            # Step 5: Determine if should continue
            results['continue'] = len(results['next_batch']) > 0

            # Add guidance message
            if results['continue']:
                results['message'] = f"Categorize the {len(results['next_batch'])} transactions in next_batch, then call this tool again with your categorizations."
            else:
                results['message'] = "All transactions categorized! No more batches to process."

            return [TextContent(
                type="text",
                text=json.dumps(results, indent=2)
            )]

        elif name == "bulk_categorize_api":
            # Run the external bulk_categorize.py script
            # This ensures we use the same code with pagination fixes

            max_batches = arguments.get('max_transactions', 1000) // arguments.get('batch_size', 100)
            batch_size = arguments.get('batch_size', 100)
            dry_run = arguments.get('dry_run', False)
            learn_rules = arguments.get('learn_rules', True)
            filter_parent = arguments.get('filter_parent')

            script_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                'bulk_categorizer',
                'bulk_categorize.py'
            )

            # Use the venv Python (script has shebang but be explicit)
            venv_python = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                'venv', 'bin', 'python'
            )

            cmd = [venv_python, script_path]
            cmd.extend(['--batch-size', str(batch_size)])
            cmd.extend(['--max-batches', str(max_batches)])

            if dry_run:
                cmd.append('--dry-run')
            if not learn_rules:
                cmd.append('--no-learn-rules')
            if filter_parent:
                cmd.extend(['--filter-parent', filter_parent])

            # Ensure API key is available in subprocess env
            env = os.environ.copy()
            if 'ANTHROPIC_API_KEY' not in env:
                api_key = get_anthropic_api_key()
                if api_key:
                    env['ANTHROPIC_API_KEY'] = api_key
                else:
                    return [TextContent(
                        type="text",
                        text=json.dumps({
                            'success': False,
                            'error': 'No Anthropic API key found. Set ANTHROPIC_API_KEY env var or store in macOS Keychain (see KEYCHAIN_SERVICE_NAME env var).'
                        })
                    )]

            logger.info(f"Running bulk categorizer: {' '.join(cmd)}")

            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=1800,  # 30 minute timeout for large batches
                    cwd=os.path.dirname(script_path),
                    env=env
                )

                # Try to read the stats file for structured output
                stats_file = os.path.join(os.path.dirname(script_path), 'last_run_stats.json')
                stats = {}
                if os.path.exists(stats_file):
                    try:
                        with open(stats_file, 'r') as f:
                            stats = json.load(f)
                    except Exception:
                        pass

                output = {
                    'success': result.returncode == 0,
                    'dry_run': dry_run,
                    'stats': stats,
                    'stdout': result.stdout[-3000:] if len(result.stdout) > 3000 else result.stdout,
                    'stderr': result.stderr[-1000:] if len(result.stderr) > 1000 else result.stderr,
                }

                if result.returncode != 0:
                    output['error'] = f"Script exited with code {result.returncode}"
                elif stats:
                    output['message'] = f"Processed {stats.get('total_processed', 0)} transactions: {stats.get('rule_categorized', 0)} by rules, {stats.get('claude_categorized', 0)} by Claude API. Cost: ${stats.get('api_usage', {}).get('estimated_cost_usd', 0):.2f}"

                return [TextContent(
                    type="text",
                    text=json.dumps(output, indent=2)
                )]

            except subprocess.TimeoutExpired:
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        'success': False,
                        'error': 'Bulk categorization timed out after 30 minutes',
                        'suggestion': 'Try with smaller max_transactions'
                    })
                )]
            except Exception as e:
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        'success': False,
                        'error': str(e)
                    })
                )]

        elif name == "run_email_backfill":
            mode = arguments.get('mode', 'optimized_incremental')
            limit = arguments.get('limit')
            start_date = arguments.get('start_date')
            end_date = arguments.get('end_date')
            email_type = arguments.get('email_type', 'all')
            dry_run = arguments.get('dry_run', False)

            script_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                'backfill_emails.py'
            )
            
            cmd = [sys.executable, script_path]

            # Mode flags (these bypass the interactive prompt)
            if mode == 'optimized_incremental':
                cmd.extend(['--optimized', '--incremental'])
            elif mode == 'optimized':
                cmd.append('--optimized')
            # Legacy mode = no flags
            
            # Optional parameters
            if limit:
                cmd.extend(['--limit', str(limit)])
            if start_date:
                cmd.extend(['--start-date', start_date])
            if end_date:
                cmd.extend(['--end-date', end_date])
            if dry_run:
                cmd.append('--dry-run')
            
            # Email type filters
            if email_type == 'orders':
                cmd.append('--orders-only')
            elif email_type == 'shipments':
                cmd.append('--shipments-only')
            elif email_type == 'returns':
                cmd.append('--returns-only')
            
            logger.info(f"Running backfill: {' '.join(cmd)}")
            
            try:
                # Run the script and capture output
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=600,  # 10 minute timeout
                    cwd=os.path.dirname(script_path)
                )
                
                output = {
                    'success': result.returncode == 0,
                    'mode': mode,
                    'command': ' '.join(cmd),
                    'stdout': result.stdout[-5000:] if len(result.stdout) > 5000 else result.stdout,  # Last 5KB
                    'stderr': result.stderr[-2000:] if len(result.stderr) > 2000 else result.stderr,
                }
                
                if result.returncode != 0:
                    output['error'] = f"Script exited with code {result.returncode}"
                
                return [TextContent(
                    type="text",
                    text=json.dumps(output, indent=2)
                )]
                
            except subprocess.TimeoutExpired:
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        'success': False,
                        'error': 'Backfill timed out after 10 minutes',
                        'mode': mode,
                        'suggestion': 'Try running with --limit to process fewer emails'
                    })
                )]
            except Exception as e:
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        'success': False,
                        'error': str(e),
                        'mode': mode
                    })
                )]
        
        elif name == "run_transaction_matcher":
            mode = arguments.get('mode', 'production')
            
            # Build command
            script_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                'transaction_matcher.py'
            )
            
            cmd = [sys.executable, script_path]

            # Set environment variable to bypass interactive prompt
            env = os.environ.copy()
            if mode == 'development':
                env['DEV_MODE'] = 'true'
            else:
                env['DEV_MODE'] = 'false'
            
            logger.info(f"Running transaction matcher in {mode} mode")
            
            try:
                # Run the script and capture output
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=600,  # 10 minute timeout
                    cwd=os.path.dirname(script_path),
                    env=env
                )
                
                output = {
                    'success': result.returncode == 0,
                    'mode': mode,
                    'stdout': result.stdout[-5000:] if len(result.stdout) > 5000 else result.stdout,
                    'stderr': result.stderr[-2000:] if len(result.stderr) > 2000 else result.stderr,
                }
                
                if result.returncode != 0:
                    output['error'] = f"Script exited with code {result.returncode}"
                
                return [TextContent(
                    type="text",
                    text=json.dumps(output, indent=2)
                )]
                
            except subprocess.TimeoutExpired:
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        'success': False,
                        'error': 'Transaction matcher timed out after 10 minutes',
                        'mode': mode
                    })
                )]
            except Exception as e:
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        'success': False,
                        'error': str(e),
                        'mode': mode
                    })
                )]
        
        else:
            return [TextContent(
                type="text",
                text=json.dumps({'error': f'Unknown tool: {name}'})
            )]
            
    except Exception as e:
        logger.exception(f"Error in tool {name}")
        return [TextContent(
            type="text",
            text=json.dumps({
                'error': str(e),
                'tool': name,
                'arguments': arguments
            })
        )]


# ============================================================================
# MAIN
# ============================================================================

async def main():
    """Run the MCP server."""
    validate_config()
    logger.info(f"Starting {MCP_SERVER_NAME} v{MCP_SERVER_VERSION}")

    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
