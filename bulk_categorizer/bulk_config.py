"""
Configuration for Bulk Categorizer.
"""

import os

# =============================================================================
# CLAUDE API SETTINGS
# =============================================================================

# Model to use for categorization
# claude-sonnet-4-20250514 is fast and cheap, good for bulk work
# claude-opus-4-20250514 for higher accuracy on ambiguous transactions
CLAUDE_MODEL = os.environ.get('CLAUDE_MODEL', 'claude-sonnet-4-20250514')

# Max tokens for response (categories are small, don't need much)
MAX_TOKENS = 4096

# =============================================================================
# BATCH SETTINGS
# =============================================================================

# Transactions per API call (balance between efficiency and context size)
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '100'))

# Max batches before stopping (safety limit)
MAX_BATCHES = int(os.environ.get('MAX_BATCHES', '100'))

# Delay between batches (seconds) to avoid rate limits
# Google Sheets allows 60 reads/minute, so 2 seconds between batches is safer
BATCH_DELAY = float(os.environ.get('BATCH_DELAY', '2.0'))

# =============================================================================
# RULE LEARNING
# =============================================================================

# Minimum occurrences before creating a merchant rule
RULE_LEARNING_THRESHOLD = 2

# Minimum pattern length for learned rules
MIN_PATTERN_LENGTH = 4

# =============================================================================
# ERROR HANDLING
# =============================================================================

# Max retries per API call
MAX_RETRIES = 3

# Base delay for exponential backoff (seconds)
RETRY_BASE_DELAY = 2.0

# Stop after this many consecutive errors
MAX_CONSECUTIVE_ERRORS = 5

# =============================================================================
# PATHS (relative to project root)
# =============================================================================

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MCP_CATEGORIZER_PATH = os.path.join(PROJECT_ROOT, 'mcp_categorizer')

# Progress file for resume capability
PROGRESS_FILE = os.path.join(PROJECT_ROOT, 'bulk_categorizer', '.progress.json')
