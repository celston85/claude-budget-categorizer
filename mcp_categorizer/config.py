#!/usr/bin/env python3
"""
Configuration for MCP Transaction Categorizer.
"""

import os

# ============================================================================
# GOOGLE SHEETS CONFIGURATION
# ============================================================================

# Budget Config Sheet - contains categories, merchant rules, keywords
BUDGET_CONFIG_SHEET_ID = os.environ.get('BUDGET_CONFIG_SHEET_ID', '')

# Processed Transactions Sheet - the output from transaction_matcher.py
PROCESSED_TRANSACTIONS_SHEET_ID = os.environ.get('PROCESSED_TRANSACTIONS_SHEET_ID', '')
PROCESSED_TRANSACTIONS_SHEET_NAME = 'Processed Transactions'

# Tab names in config sheet
CONFIG_CATEGORIES_TAB = 'Categories'
CONFIG_MERCHANT_RULES_TAB = 'Merchant Rules'
CONFIG_KEYWORDS_TAB = 'Keywords'

# ============================================================================
# CATEGORIZATION SETTINGS
# ============================================================================

# Batch size for processing transactions
BATCH_SIZE = 50

# Confidence thresholds
CONFIDENCE_HIGH = 80      # Auto-apply without review
CONFIDENCE_MEDIUM = 50    # Apply but flag for review
CONFIDENCE_LOW = 30       # Flag for review, don't auto-apply

# Merchants that are inherently ambiguous (always flag for review)
AMBIGUOUS_MERCHANTS = [
    'target',
    'walmart',
    'costco',
    'amazon',
    'amzn',
]

# ============================================================================
# COLUMN NAMES (in processed transactions sheet)
# ============================================================================

# Existing columns from transaction_matcher.py
COL_DATE = 'Date'
COL_DESCRIPTION = 'Description'
COL_CATEGORY = 'Category'  # Original Tiller category
COL_AMOUNT = 'Amount'
COL_ACCOUNT = 'Account'
COL_AMAZON_ORDER_ID = 'amazon_order_id'
COL_MATCH_CONFIDENCE = 'match_confidence'
COL_MATCH_STATUS = 'match_status'

# New columns for categorization
COL_CLAUDE_CATEGORY = 'claude_category'
COL_CATEGORY_SOURCE = 'category_source'  # merchant_rule | keyword | claude | manual
COL_CATEGORY_CONFIDENCE = 'category_confidence'
COL_CATEGORIZED_AT = 'categorized_at'
COL_CATEGORIZED_BY = 'categorized_by'  # mcp_v1 | manual
COL_NEEDS_REVIEW = 'needs_review'
COL_REVIEW_REASON = 'review_reason'
COL_PREVIOUS_CATEGORY = 'previous_category'

# All categorization columns (for ensuring they exist)
CATEGORIZATION_COLUMNS = [
    COL_CLAUDE_CATEGORY,
    COL_CATEGORY_SOURCE,
    COL_CATEGORY_CONFIDENCE,
    COL_CATEGORIZED_AT,
    COL_CATEGORIZED_BY,
    COL_NEEDS_REVIEW,
    COL_REVIEW_REASON,
    COL_PREVIOUS_CATEGORY,
]

# ============================================================================
# MCP SERVER SETTINGS
# ============================================================================

MCP_SERVER_NAME = 'budget-categorizer'
MCP_SERVER_VERSION = '1.4.0'  # Added query, bulk_update, reset, spending_summary, dashboard tools

# ============================================================================
# LOGGING
# ============================================================================

LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')


def validate_config():
    """Validate required configuration. Raises RuntimeError if sheet IDs are missing."""
    missing = []
    if not BUDGET_CONFIG_SHEET_ID:
        missing.append('BUDGET_CONFIG_SHEET_ID')
    if not PROCESSED_TRANSACTIONS_SHEET_ID:
        missing.append('PROCESSED_TRANSACTIONS_SHEET_ID')
    if missing:
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing)}\n"
            "Run ./setup.sh or set them in your Claude Desktop MCP config env block."
        )
