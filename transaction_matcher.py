#!/usr/bin/env python3
"""
Transaction Matcher

Local script that matches bank transactions against the pre-parsed orders
from the Parsed Orders Google Sheet.

Features:
- Reads unprocessed transactions from source sheet
- Reads parsed orders AND returns from Parsed Orders sheet
- Matches by date (+-7 days) and amount (+-$3 tolerance)
- Calculates confidence score for each match
- Writes matched/expanded rows to output sheet
- Flags low-confidence and unmatched for review
"""

import os
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Tuple
from collections import defaultdict

from utils import (
    get_credentials_oauth,
    get_sheets_service,
    parse_amount,
    parse_date,
    column_index_to_letter,
    validate_sheet_access,
    check_sheet_cell_usage,
    check_gmail_watch_status
)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Google Sheets Configuration
SOURCE_SHEET_ID = os.environ.get('SOURCE_SHEET_ID', '')  # Set your source sheet ID
SOURCE_SHEET_NAME = 'Transactions'
SOURCE_RANGE = f'{SOURCE_SHEET_NAME}!A:Z'

PARSED_ORDERS_SHEET_ID = os.environ.get('PARSED_ORDERS_SHEET_ID', '')
PARSED_ORDERS_SHEET_NAME = 'Parsed Orders'

OUTPUT_SHEET_ID = os.environ.get('PROCESSED_TRANSACTIONS_SHEET_ID', '')  # Set your output sheet ID
OUTPUT_SHEET_NAME = 'Processed Transactions'

# Column names in source sheet
COL_DATE = 'Date'
COL_DESCRIPTION = 'Description'
COL_CATEGORY = 'Category'
COL_AMOUNT = 'Amount'
COL_ACCOUNT = 'Account'
COL_PROCESSED_FLAG = 'Processed_Flag'
COL_CATEGORY_HINT = 'Category Hint'
COL_DATE_ADDED = 'Date Added'

REQUIRED_COLUMNS = [COL_DATE, COL_DESCRIPTION, COL_CATEGORY, COL_AMOUNT, COL_ACCOUNT]

# Dedup: Plaid/Tiller creates multiple rows for the same bank transaction
# (pending, posted, enriched stages with different Transaction IDs).
# We group by (Date, Amount, Account, description prefix) and keep the best row.
DEDUP_DESC_PREFIX_LEN = 20

# Amazon Transaction Filtering
# Skip transactions with blank Category Hint (Tiller duplicates filter)
# Set False to include ALL transactions (checking account txns often lack hints)
SKIP_BLANK_CATEGORY_HINT = os.environ.get('SKIP_BLANK_CATEGORY_HINT', 'false').lower() == 'true'

AMAZON_ACCOUNT = os.environ.get('AMAZON_ACCOUNT', 'Chase Sapphire')  # Your credit card account name
AMAZON_DESCRIPTION_PATTERNS = ['Amazon', 'AMZN', 'Amzn']

# Exclude subscriptions/digital that don't have order emails
AMAZON_EXCLUDE_PATTERNS = ['Digital', 'Kids', 'Prime', 'Tips']

# Known Amazon subscription/digital transaction patterns
# These won't have emails to match, so we assign clean names directly
# Format: (pattern, exact_match, clean_name)
# - pattern: substring to look for in description
# - exact_match: if True, description must equal pattern exactly
# - clean_name: readable name to use in output
# NOTE: Categories are NOT auto-assigned - left for manual/MCP categorization
# Customize this list for your Amazon usage patterns
AMAZON_KNOWN_PATTERNS = [
    # Order matters! More specific patterns first
    ('Amazon Prime*', False, 'Amazon Prime - Movie Rental'),  # Prime with special chars
    ('Amazon Prime', True, 'Amazon Prime - Subscription'),    # Exact "Amazon Prime"
    ('Amazon Kids', False, 'Amazon Kids - Movie Rental'),
    ('Amazon Tips', False, 'Amazon - Grocery Tips'),
    ('Amzn Digital', False, 'Amazon Digital - Movie Rental'),
    ('AMZN Digital', False, 'Amazon Digital - Movie Rental'),
]


def get_known_amazon_pattern(description: str) -> Optional[str]:
    """
    Check if description matches a known Amazon subscription/digital pattern.

    Returns:
        clean_name if matched, None otherwise
    """
    desc = description.strip()
    desc_upper = desc.upper()

    for pattern, exact_match, clean_name in AMAZON_KNOWN_PATTERNS:
        pattern_upper = pattern.upper()

        if exact_match:
            if desc_upper == pattern_upper:
                return clean_name
        else:
            if pattern_upper in desc_upper:
                return clean_name

    return None

# Matching Configuration
DATE_MATCHING_WINDOW_DAYS = 30  # +-30 days (Amazon can charge up to 30 days after order)
AMOUNT_MATCHING_TOLERANCE = 3.0  # +-$3

# Confidence Scoring
CONFIDENCE_THRESHOLD_HIGH = 60  # Above this = high confidence match (exact amount + 3 days = 61)
CONFIDENCE_THRESHOLD_LOW = 40   # Below this = flagged for review

# Date Filtering (optional)
FILTER_MONTH = None  # Set to 1-12 to filter by month
FILTER_YEAR = None   # Set to year to filter

# Development Mode - clears Processed_Flag and output sheet before each run
# Set via environment variable: export DEV_MODE=true (or false for production)
# Defaults to True for safety during development
DEV_MODE = os.environ.get('DEV_MODE', 'true').lower() == 'true'

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# DATA LOADING
# ============================================================================

def load_transactions(creds) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Load unprocessed transactions from source sheet.
    Returns (list of transactions, column index mapping).
    """
    service = get_sheets_service(creds)

    logger.info(f"Reading transactions from {SOURCE_SHEET_ID}")
    result = service.spreadsheets().values().get(
        spreadsheetId=SOURCE_SHEET_ID,
        range=SOURCE_RANGE
    ).execute()

    values = result.get('values', [])

    if not values:
        logger.warning("No data found in source sheet")
        return [], {}

    # Parse headers
    headers = values[0]
    col_indices = {}
    for col in REQUIRED_COLUMNS + [COL_PROCESSED_FLAG, COL_CATEGORY_HINT, COL_DATE_ADDED]:
        try:
            col_indices[col] = headers.index(col)
        except ValueError:
            logger.warning(f"Column '{col}' not found in sheet headers")
            col_indices[col] = None

    # Parse rows
    transactions = []
    skipped_no_hint = 0
    for i, row in enumerate(values[1:], start=2):
        padded_row = row + [''] * (len(headers) - len(row))

        # Check processed flag
        processed_idx = col_indices.get(COL_PROCESSED_FLAG)
        if processed_idx is not None and processed_idx < len(padded_row):
            flag = str(padded_row[processed_idx]).strip().upper()
            if flag == 'TRUE' or flag == '1':
                continue

        # Optionally skip rows where Category Hint is null/blank (Tiller duplicates)
        if SKIP_BLANK_CATEGORY_HINT:
            hint_idx = col_indices.get(COL_CATEGORY_HINT)
            if hint_idx is not None and hint_idx < len(padded_row):
                hint_value = str(padded_row[hint_idx]).strip()
                if not hint_value:
                    skipped_no_hint += 1
                    continue

        # Extract required columns
        trans = {'_row_number': i}
        for col in REQUIRED_COLUMNS:
            idx = col_indices.get(col)
            if idx is not None and idx < len(padded_row):
                trans[col] = padded_row[idx]
            else:
                trans[col] = ''

        # Extract dedup metadata (not written to output)
        hint_idx = col_indices.get(COL_CATEGORY_HINT)
        trans['_has_hint'] = bool(
            hint_idx is not None and hint_idx < len(padded_row)
            and padded_row[hint_idx].strip()
        )
        added_idx = col_indices.get(COL_DATE_ADDED)
        trans['_date_added'] = (
            padded_row[added_idx].strip()
            if added_idx is not None and added_idx < len(padded_row)
            else ''
        )

        transactions.append(trans)

    if skipped_no_hint > 0:
        logger.info(f"Skipped {skipped_no_hint} rows with blank Category Hint (Tiller duplicates)")
    logger.info(f"Loaded {len(transactions)} transactions (before dedup)")

    # Dedup Plaid/Tiller duplicates
    transactions = _dedup_plaid_transactions(transactions)

    logger.info(f"After dedup: {len(transactions)} transactions")
    return transactions, col_indices


def _dedup_plaid_transactions(transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Remove Plaid/Tiller duplicate rows.

    Plaid creates multiple rows per real bank transaction (pending, posted,
    enriched stages) with different Transaction IDs. We group by
    (Date, Amount, Account, first N chars of Description) and keep the
    best row: prefer has Category Hint, then latest Date Added.
    """
    groups = defaultdict(list)
    for trans in transactions:
        desc_prefix = trans.get(COL_DESCRIPTION, '')[:DEDUP_DESC_PREFIX_LEN].strip().upper()
        key = (
            trans.get(COL_DATE, '').strip(),
            trans.get(COL_AMOUNT, '').strip(),
            trans.get(COL_ACCOUNT, '').strip(),
            desc_prefix,
        )
        groups[key].append(trans)

    deduped = []
    removed = 0
    for key, rows in groups.items():
        if len(rows) == 1:
            deduped.append(rows[0])
            continue

        # Pick the best row: prefer hint, then latest date_added
        rows.sort(key=lambda t: (t.get('_has_hint', False), t.get('_date_added', '')))
        best = rows[-1]
        deduped.append(best)
        removed += len(rows) - 1

    if removed > 0:
        logger.info(f"Dedup: removed {removed} Plaid/Tiller duplicate rows")

    # Sort by original row number to preserve order
    deduped.sort(key=lambda t: t['_row_number'])
    return deduped


def load_parsed_orders(creds, min_date: datetime = None, max_date: datetime = None) -> List[Dict[str, Any]]:
    """
    Load parsed orders from Parsed Orders sheet.

    Args:
        creds: Google API credentials
        min_date: Optional minimum date filter (orders before this are skipped)
        max_date: Optional maximum date filter (orders after this are skipped)

    Returns list of parsed order records within the date range.
    Date filtering reduces memory usage for large datasets.
    """
    service = get_sheets_service(creds)

    logger.info(f"Reading parsed orders from {PARSED_ORDERS_SHEET_ID}")
    if min_date or max_date:
        logger.info(f"  Date filter: {min_date.strftime('%Y-%m-%d') if min_date else 'none'} to {max_date.strftime('%Y-%m-%d') if max_date else 'none'}")

    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=PARSED_ORDERS_SHEET_ID,
            range=f'{PARSED_ORDERS_SHEET_NAME}!A:J'
        ).execute()
    except Exception as e:
        logger.error(f"Error reading parsed orders sheet: {e}")
        logger.error("Make sure PARSED_ORDERS_SHEET_ID is set correctly")
        return []

    values = result.get('values', [])

    if not values:
        logger.warning("No data in Parsed Orders sheet")
        return []

    # Expected columns (10 total, with shipment_total):
    # email_id, email_date, email_type, order_number, shipment_total,
    # item_name, item_price, item_qty, parse_status, processed_at
    headers = values[0] if values else []

    orders = []
    skipped_date_filter = 0
    total_rows = len(values) - 1

    for row in values[1:]:
        if not row:
            continue

        padded_row = row + [''] * (10 - len(row))

        # Parse email date early for filtering
        email_date_str = padded_row[1]
        parsed_date = _parse_order_date(email_date_str)

        # Apply date filter to reduce memory usage
        if parsed_date:
            if min_date and parsed_date < min_date:
                skipped_date_filter += 1
                continue
            if max_date and parsed_date > max_date:
                skipped_date_filter += 1
                continue

        order = {
            'email_id': padded_row[0],
            'email_date': email_date_str,
            'email_type': padded_row[2],
            'order_number': padded_row[3],
            'shipment_total': _safe_float(padded_row[4]),  # The actual bank charge
            'item_name': padded_row[5],
            'item_price': _safe_float(padded_row[6]),
            'item_qty': _safe_int(padded_row[7]),
            'parse_status': padded_row[8],
            'processed_at': padded_row[9],
            '_parsed_date': parsed_date
        }

        orders.append(order)

    logger.info(f"Loaded {len(orders)} parsed order records (filtered from {total_rows} total)")
    if skipped_date_filter > 0:
        logger.info(f"  Skipped {skipped_date_filter} orders outside date range")

    # Log breakdown by type
    order_count = sum(1 for o in orders if o['email_type'] == 'order')
    shipment_count = sum(1 for o in orders if o['email_type'] == 'shipment')
    return_count = sum(1 for o in orders if o['email_type'] == 'return')
    logger.info(f"  - Orders: {order_count}, Shipments: {shipment_count}, Returns: {return_count}")

    return orders


def _safe_float(value) -> float:
    """Safely convert to float."""
    if not value:
        return 0.0
    try:
        return float(str(value).replace('$', '').replace(',', ''))
    except ValueError:
        return 0.0


def _safe_int(value) -> int:
    """Safely convert to int."""
    if not value:
        return 1
    try:
        return int(float(str(value)))
    except ValueError:
        return 1


def _parse_order_date(date_str: str) -> Optional[datetime]:
    """
    Parse order date from various formats.

    Returns datetime normalized to midnight (00:00:00) to avoid timezone-related
    off-by-one errors when comparing with transaction dates. Email dates may include
    time components that could push the date to the next day depending on timezone.
    """
    if not date_str:
        return None

    formats = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d',
        '%m/%d/%Y %H:%M:%S',
        '%m/%d/%Y',
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            # Normalize to midnight to avoid time-of-day affecting date comparisons
            return dt.replace(hour=0, minute=0, second=0, microsecond=0)
        except ValueError:
            continue

    return None

# ============================================================================
# TRANSACTION FILTERING
# ============================================================================

def filter_amazon_transactions(transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter for Amazon transactions from the configured account."""
    amazon_transactions = []

    for trans in transactions:
        account = str(trans.get(COL_ACCOUNT, '')).strip()
        description = str(trans.get(COL_DESCRIPTION, '')).strip()

        # Check account
        if account.upper() != AMAZON_ACCOUNT.upper():
            continue

        # Check description patterns
        desc_upper = description.upper()
        is_amazon = any(
            desc_upper == p.upper() or desc_upper.startswith(p.upper())
            for p in AMAZON_DESCRIPTION_PATTERNS
        )

        if is_amazon:
            amazon_transactions.append(trans)

    logger.info(f"Found {len(amazon_transactions)} Amazon transactions")
    return amazon_transactions


def filter_by_date(transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter transactions by month/year if configured."""
    if FILTER_MONTH is None and FILTER_YEAR is None:
        return transactions

    filtered = []
    for trans in transactions:
        trans_date = parse_date(trans.get(COL_DATE))
        if not trans_date:
            continue

        if FILTER_MONTH is not None and trans_date.month != FILTER_MONTH:
            continue
        if FILTER_YEAR is not None and trans_date.year != FILTER_YEAR:
            continue

        filtered.append(trans)

    logger.info(f"Filtered to {len(filtered)} transactions for month={FILTER_MONTH}, year={FILTER_YEAR}")
    return filtered


def deduplicate_transactions(transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove duplicate transactions based on Date + Description."""
    seen = set()
    unique = []

    for trans in transactions:
        key = (
            str(trans.get(COL_DATE, '')).strip(),
            str(trans.get(COL_DESCRIPTION, '')).strip().upper()
        )

        if key not in seen:
            seen.add(key)
            unique.append(trans)

    if len(unique) < len(transactions):
        logger.info(f"Removed {len(transactions) - len(unique)} duplicate transactions")

    return unique

# ============================================================================
# MATCHING LOGIC
# ============================================================================

def match_transaction(
    transaction: Dict[str, Any],
    parsed_orders: List[Dict[str, Any]],
    used_email_ids: set = None
) -> Tuple[Optional[List[Dict[str, Any]]], int, str, Optional[str]]:
    """
    Match a transaction to parsed orders using shipment_total.

    Each email represents one shipment with one bank charge, but a shipment
    can contain multiple items. We match on shipment_total (from email) to
    transaction amount, and return ALL items from the matched shipment.

    Args:
        transaction: Transaction to match
        parsed_orders: List of parsed order records
        used_email_ids: Set of email_ids already matched (to prevent reuse)

    Returns:
        (matched_items, confidence_score, match_status, matched_email_id)
        - matched_items: List of matched order items, or None
        - confidence_score: 0-100
        - match_status: 'matched', 'low_confidence', 'unmatched'
        - matched_email_id: The email_id that was matched, or None
    """
    if used_email_ids is None:
        used_email_ids = set()
    trans_date = parse_date(transaction.get(COL_DATE))
    trans_amount_raw = parse_amount(transaction.get(COL_AMOUNT, '0'))
    trans_amount = abs(trans_amount_raw)

    if not trans_date or trans_amount == 0:
        return None, 0, 'unmatched', None

    # Determine if charge (order/shipment) or credit (return)
    is_credit = trans_amount_raw > 0

    # For charges: try shipment first (more accurate), fall back to order
    # For credits: use return emails
    if is_credit:
        email_type_priority = ['return']
    else:
        email_type_priority = ['shipment', 'order']  # Try shipment first, then order

    best_match = None
    best_score = 0
    best_items = []

    for target_type in email_type_priority:
        # Filter candidates by email type and date window
        candidates = []
        for order in parsed_orders:
            if order['email_type'] != target_type:
                continue

            order_date = order.get('_parsed_date')
            if not order_date:
                continue

            date_diff = abs((trans_date - order_date).days)
            if date_diff > DATE_MATCHING_WINDOW_DAYS:
                continue

            candidates.append((order, date_diff))

        if not candidates:
            continue  # Try next email type

        # Group candidates by email_id (each email = one shipment = one bank charge)
        orders_by_email = defaultdict(list)
        for order, date_diff in candidates:
            email_id = order.get('email_id')
            orders_by_email[email_id].append((order, date_diff))

        # Score each shipment (email)
        for email_id, order_items in orders_by_email.items():
            # Skip if this email was already matched to another transaction
            if email_id in used_email_ids:
                continue

            # Use shipment_total from the email (all items have same shipment_total)
            shipment_total = order_items[0][0].get('shipment_total', 0)
            min_date_diff = min(o[1] for o in order_items)

            # Skip if no shipment_total
            if not shipment_total:
                continue

            # Calculate score based on shipment_total matching transaction amount
            score = calculate_confidence_score(trans_amount, shipment_total, min_date_diff)

            if score > best_score:
                best_score = score
                best_match = email_id
                best_items = [o[0] for o in order_items]

        # If we found a good match with this email type, stop (don't fall back)
        if best_score >= CONFIDENCE_THRESHOLD_HIGH:
            break

    if best_match is None:
        return None, 0, 'unmatched', None

    # Determine match status based on confidence
    if best_score >= CONFIDENCE_THRESHOLD_HIGH:
        match_status = 'matched'
    elif best_score >= CONFIDENCE_THRESHOLD_LOW:
        match_status = 'low_confidence'
    else:
        match_status = 'unmatched'
        best_match = None  # Don't claim the email if below threshold

    return best_items, best_score, match_status, best_match


def calculate_confidence_score(
    trans_amount: float,
    order_total: float,
    date_diff_days: int
) -> int:
    """
    Calculate confidence score for a match (0-100).

    Scoring:
    - Exact amount: +50 points
    - Close amount (within $1): +30 points
    - Amount within tolerance: +20 points
    - Date proximity: +20 points minus 3 per day difference
    """
    score = 0
    amount_diff = abs(trans_amount - order_total)

    # Amount scoring
    if amount_diff < 0.01:
        score += 50  # Exact match
    elif amount_diff <= 1.0:
        score += 30  # Very close
    elif amount_diff <= AMOUNT_MATCHING_TOLERANCE:
        score += 20  # Within tolerance
    else:
        return 0  # Outside tolerance, no match

    # Date proximity scoring
    date_score = max(0, 20 - (date_diff_days * 3))
    score += date_score

    # Bonus for same-day match
    if date_diff_days == 0:
        score += 10

    return min(score, 100)


def match_all_transactions_optimally(
    transactions: List[Dict[str, Any]],
    parsed_orders: List[Dict[str, Any]]
) -> Dict[int, Tuple[Optional[List[Dict[str, Any]]], int, str]]:
    """
    Match all transactions to parsed orders using optimal assignment.

    Instead of matching transactions in order (which can lead to suboptimal
    assignments), this function:
    1. Scores ALL possible transaction-email pairs
    2. Sorts by confidence score descending
    3. Assigns matches greedily from highest to lowest score

    This ensures that high-confidence matches aren't "stolen" by lower-confidence
    matches that happen to appear earlier in the transaction list.

    Args:
        transactions: List of Amazon transactions to match
        parsed_orders: List of parsed order records

    Returns:
        Dict mapping row_number -> (matched_items, confidence, status)
    """
    # Build index of emails by type for faster lookup
    emails_by_type = defaultdict(list)
    for order in parsed_orders:
        email_type = order.get('email_type', '')
        email_id = order.get('email_id', '')
        if email_type and email_id:
            emails_by_type[email_type].append(order)

    # Group orders by email_id (each email = one shipment)
    orders_by_email = defaultdict(list)
    for order in parsed_orders:
        email_id = order.get('email_id', '')
        if email_id:
            orders_by_email[email_id].append(order)

    # Phase 1: Score all possible matches
    # List of (score, row_number, email_id, items)
    all_matches = []

    for trans in transactions:
        row_num = trans.get('_row_number')
        trans_date = parse_date(trans.get(COL_DATE))
        trans_amount_raw = parse_amount(trans.get(COL_AMOUNT, '0'))
        trans_amount = abs(trans_amount_raw)

        if not trans_date or trans_amount == 0:
            continue

        # Determine email types to search
        is_credit = trans_amount_raw > 0
        if is_credit:
            email_types = ['return']
        else:
            email_types = ['shipment', 'order']

        # Find all candidate emails
        seen_emails = set()
        for email_type in email_types:
            for order in emails_by_type.get(email_type, []):
                email_id = order.get('email_id', '')
                if email_id in seen_emails:
                    continue
                seen_emails.add(email_id)

                order_date = order.get('_parsed_date')
                if not order_date:
                    continue

                date_diff = abs((trans_date - order_date).days)
                if date_diff > DATE_MATCHING_WINDOW_DAYS:
                    continue

                # Get shipment total and all items for this email
                email_items = orders_by_email.get(email_id, [])
                if not email_items:
                    continue

                shipment_total = email_items[0].get('shipment_total', 0)
                if not shipment_total:
                    continue

                # Calculate score
                score = calculate_confidence_score(trans_amount, shipment_total, date_diff)
                if score > 0:
                    all_matches.append((score, row_num, email_id, email_items))

    # Phase 2: Sort by score descending and assign greedily
    all_matches.sort(key=lambda x: x[0], reverse=True)

    used_emails = set()
    used_transactions = set()
    match_results = {}

    for score, row_num, email_id, items in all_matches:
        # Skip if transaction or email already assigned
        if row_num in used_transactions or email_id in used_emails:
            continue

        # Determine match status
        if score >= CONFIDENCE_THRESHOLD_HIGH:
            status = 'matched'
        elif score >= CONFIDENCE_THRESHOLD_LOW:
            status = 'low_confidence'
        else:
            continue  # Below threshold, don't assign

        match_results[row_num] = (items, score, status)
        used_emails.add(email_id)
        used_transactions.add(row_num)

    # Phase 3: Mark unmatched transactions
    for trans in transactions:
        row_num = trans.get('_row_number')
        if row_num not in match_results:
            match_results[row_num] = (None, 0, 'unmatched')

    return match_results


# ============================================================================
# ITEM NAME SUMMARIZATION
# ============================================================================

# Words to remove from item names (common filler words)
FILLER_WORDS = {
    'the', 'a', 'an', 'for', 'with', 'and', 'or', 'in', 'on', 'to', 'of', 'by',
    'your', 'our', 'all', 'new', 'best', 'great', 'perfect', 'premium', 'quality',
    'professional', 'upgraded', 'improved', 'enhanced', 'deluxe', 'ultimate',
    'extra', 'super', 'mega', 'ultra', 'max', 'pro', 'plus', 'pack', 'count',
    'up', 'use', 'multi', 'high', 'performance', 'long', 'lasting', 'natural',
}

# Brand patterns to preserve at start
BRAND_PATTERNS = [
    'amazon basics', 'amazonbasics', 'amazon essentials', 'zippo', 'tylenol',
    'advil', 'clorox', 'dawn', 'bounty', 'charmin', 'tide', 'glad', 'ziploc',
    'duracell', 'energizer', 'scotch', 'post-it', 'sharpie', 'bic', 'honest company',
]


def summarize_item_name(item_name: str, max_length: int = 45) -> str:
    """
    Summarize a long Amazon product name to a shorter, readable version.

    Examples:
    - "Zippo Classic Brushed Chrome Pocket Lighter - Windproof..."
      -> "Zippo Chrome Pocket Lighter"
    - "100 Pack Hand Warmers Disposable - Up to 15 Hours..."
      -> "Hand Warmers (100)"
    - "Amazon Basics AAA Alkaline High-Performance Batteries, 36 Count"
      -> "Amazon Basics AAA Batteries (36)"
    """
    import re

    if not item_name:
        return ''

    # Clean up the name
    name = item_name.strip()

    # Remove quantity prefix like "1of2_" or "2of3_"
    name = re.sub(r'^\d+of\d+_', '', name)

    # Check for brand at start and preserve it
    name_lower = name.lower()
    brand = ''
    for pattern in BRAND_PATTERNS:
        if name_lower.startswith(pattern):
            brand = name[:len(pattern)].title()
            name = name[len(pattern):].strip()
            name = re.sub(r'^[\s\-,:\|]+', '', name)
            break

    # Extract quantity patterns (e.g., "100 Pack", "36 Count", "20-Pack", "5PCS")
    qty_match = re.search(r'(\d+)[\s\-]*(pack|count|ct|pc|pcs|piece|pieces|sheets|wipes|pods|capsules|tablets|gels)\b', name, re.IGNORECASE)
    qty_str = ''
    if qty_match:
        qty_str = f"({qty_match.group(1)})"
        # Remove the quantity phrase from name to avoid duplication
        name = name[:qty_match.start()] + name[qty_match.end():]

    # Also check for leading quantity like "100 Pack Hand Warmers"
    leading_qty = re.match(r'^(\d+)\s*(pack|count|ct|pc|pcs)?\s+', name, re.IGNORECASE)
    if leading_qty and not qty_str:
        qty_str = f"({leading_qty.group(1)})"
        name = name[leading_qty.end():]

    # Split on common separators (but not hyphen within words) and take first segment
    segments = re.split(r'\s*[\|\,]\s*|\s+\-\s+', name)
    main_segment = segments[0] if segments else name

    # Split into words and filter
    words = main_segment.split()

    # Keep meaningful words
    meaningful_words = []
    for word in words:
        word_clean = re.sub(r'[^\w\']', '', word)
        word_lower = word_clean.lower()

        # Skip filler words, very short words, pure numbers, and size specs
        if (word_lower in FILLER_WORDS or
            len(word_clean) < 2 or
            word_clean.isdigit() or
            re.match(r'^\d+\w{1,2}$', word_clean) or
            re.match(r'^\d+x\d+', word_lower)):
            continue

        meaningful_words.append(word_clean)

        # Limit to 4 meaningful words
        if len(meaningful_words) >= 4:
            break

    # Build the summary
    summary_parts = []
    if brand:
        summary_parts.append(brand)
    if meaningful_words:
        summary_parts.append(' '.join(meaningful_words))
    if qty_str:
        summary_parts.append(qty_str)

    summary = ' '.join(summary_parts)

    # Final cleanup
    summary = re.sub(r'\s+', ' ', summary).strip()

    # Truncate if still too long
    if len(summary) > max_length:
        summary = summary[:max_length-3].rsplit(' ', 1)[0] + '...'

    return summary if summary else item_name[:max_length]


# ============================================================================
# OUTPUT GENERATION
# ============================================================================

def generate_output_rows(
    all_transactions: List[Dict[str, Any]],
    amazon_transactions: List[Dict[str, Any]],
    match_results: Dict[int, Tuple[Optional[List[Dict[str, Any]]], int, str]]
) -> List[Dict[str, Any]]:
    """
    Generate output rows for all transactions.

    For matched Amazon transactions: one row per item with details.
    For non-Amazon transactions: pass through as-is.

    Each row includes:
    - source_row: The row number from the source sheet (for deduplication)
    - processed_at: Timestamp when this row was generated
    """
    output_rows = []
    amazon_row_numbers = {t.get('_row_number') for t in amazon_transactions}
    processed_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for trans in all_transactions:
        row_num = trans.get('_row_number')

        if row_num in amazon_row_numbers and row_num in match_results:
            items, confidence, status = match_results[row_num]

            if items and status in ('matched', 'low_confidence'):
                original_desc = str(trans.get(COL_DESCRIPTION, ''))
                original_amount = parse_amount(trans.get(COL_AMOUNT, '0'))
                amount_sign = '-' if original_amount < 0 else ''

                for item in items:
                    item_name = item.get('item_name', '')
                    item_price = item.get('item_price', 0)

                    if not item_name:
                        continue

                    # Summarize item name for readability
                    short_name = summarize_item_name(item_name)

                    row = {}
                    for col in REQUIRED_COLUMNS:
                        row[col] = trans.get(col, '')

                    row[COL_DESCRIPTION] = short_name
                    row[COL_AMOUNT] = f'{amount_sign}${item_price:.2f}'
                    row['amazon_order_id'] = original_desc
                    row['match_confidence'] = confidence
                    row['match_status'] = status
                    row['source_row'] = row_num
                    row['processed_at'] = processed_at

                    output_rows.append(row)
            else:
                # Unmatched Amazon transaction - check for known patterns
                row = {}
                for col in REQUIRED_COLUMNS:
                    row[col] = trans.get(col, '')

                # Check if it's a known subscription/digital pattern
                original_desc = str(trans.get(COL_DESCRIPTION, ''))
                clean_name = get_known_amazon_pattern(original_desc)

                if clean_name:
                    row[COL_DESCRIPTION] = clean_name
                    # Category is NOT set - left for manual/MCP categorization
                    row['match_status'] = 'known_pattern'
                    logger.info(f"  Row {row_num}: Known pattern '{original_desc}' → '{clean_name}'")
                else:
                    row['match_status'] = status

                row['amazon_order_id'] = ''
                row['match_confidence'] = confidence
                row['source_row'] = row_num
                row['processed_at'] = processed_at
                output_rows.append(row)
        else:
            # Non-Amazon transaction
            row = {}
            for col in REQUIRED_COLUMNS:
                row[col] = trans.get(col, '')
            row['amazon_order_id'] = ''
            row['match_confidence'] = ''
            row['match_status'] = ''
            row['source_row'] = row_num
            row['processed_at'] = processed_at
            output_rows.append(row)

    return output_rows


def write_output(output_rows: List[Dict[str, Any]], creds, dev_mode: bool = True):
    """
    Write output rows to the output sheet.

    Args:
        output_rows: List of row dictionaries to write
        creds: Google API credentials
        dev_mode: If True, clear and overwrite. If False, append with deduplication.
    """
    service = get_sheets_service(creds)

    if not output_rows:
        logger.warning("No output rows to write")
        return

    # Headers: required columns + amazon_order_id + match columns + tracking
    headers = REQUIRED_COLUMNS + ['amazon_order_id', 'match_confidence', 'match_status', 'source_row', 'processed_at']

    if dev_mode:
        # DEV MODE: Clear and overwrite entire sheet
        logger.info("DEV MODE: Clearing and overwriting output sheet")

        values = [headers]
        for row in output_rows:
            row_values = [str(row.get(h, '')) for h in headers]
            values.append(row_values)

        # Clear sheet
        range_name = f'{OUTPUT_SHEET_NAME}!A:Z'
        try:
            service.spreadsheets().values().clear(
                spreadsheetId=OUTPUT_SHEET_ID,
                range=range_name
            ).execute()
        except Exception as e:
            logger.debug(f"Could not clear output sheet (may be empty): {e}")

        # Write all rows
        body = {'values': values}
        service.spreadsheets().values().update(
            spreadsheetId=OUTPUT_SHEET_ID,
            range=f'{OUTPUT_SHEET_NAME}!A1',
            valueInputOption='RAW',
            body=body
        ).execute()

        logger.info(f"Wrote {len(output_rows)} rows to output sheet")

    else:
        # PRODUCTION MODE: Append only new rows (deduplicate by source_row)
        logger.info("PRODUCTION MODE: Appending new rows with deduplication")

        # Read existing output to get already-written source rows
        existing_source_rows = set()
        try:
            result = service.spreadsheets().values().get(
                spreadsheetId=OUTPUT_SHEET_ID,
                range=f'{OUTPUT_SHEET_NAME}!A:Z'
            ).execute()

            existing_values = result.get('values', [])
            if existing_values:
                existing_headers = existing_values[0]
                # Find source_row column index
                try:
                    source_row_idx = existing_headers.index('source_row')
                    for row in existing_values[1:]:
                        if len(row) > source_row_idx and row[source_row_idx]:
                            try:
                                existing_source_rows.add(int(row[source_row_idx]))
                            except (ValueError, TypeError):
                                pass
                    logger.info(f"Found {len(existing_source_rows)} existing source rows in output")
                except ValueError:
                    logger.warning("No 'source_row' column in existing output - cannot deduplicate")

        except Exception as e:
            logger.warning(f"Could not read existing output (may be empty): {e}")

        # Filter out rows that already exist
        new_rows = []
        skipped_count = 0
        for row in output_rows:
            source_row = row.get('source_row')
            if source_row and source_row in existing_source_rows:
                skipped_count += 1
                continue
            new_rows.append(row)

        if skipped_count > 0:
            logger.info(f"Skipped {skipped_count} duplicate rows (already in output)")

        if not new_rows:
            logger.info("No new rows to append")
            return

        # Check if sheet is empty (needs headers)
        if not existing_source_rows:
            # Write headers first
            try:
                result = service.spreadsheets().values().get(
                    spreadsheetId=OUTPUT_SHEET_ID,
                    range=f'{OUTPUT_SHEET_NAME}!1:1'
                ).execute()
                existing_headers = result.get('values', [[]])
                if not existing_headers or not existing_headers[0]:
                    # No headers, write them
                    service.spreadsheets().values().update(
                        spreadsheetId=OUTPUT_SHEET_ID,
                        range=f'{OUTPUT_SHEET_NAME}!A1',
                        valueInputOption='RAW',
                        body={'values': [headers]}
                    ).execute()
                    logger.info("Added headers to output sheet")
            except Exception as e:
                logger.debug(f"Could not check/write headers: {e}")

        # Append new rows
        values = []
        for row in new_rows:
            row_values = [str(row.get(h, '')) for h in headers]
            values.append(row_values)

        body = {'values': values}
        service.spreadsheets().values().append(
            spreadsheetId=OUTPUT_SHEET_ID,
            range=f'{OUTPUT_SHEET_NAME}!A:Z',
            valueInputOption='RAW',
            insertDataOption='INSERT_ROWS',
            body=body
        ).execute()

        logger.info(f"Appended {len(new_rows)} new rows to output sheet")


def update_processed_flags(row_numbers: List[int], creds):
    """Update processed flag for matched transactions."""
    if not row_numbers:
        return

    service = get_sheets_service(creds)

    # Get headers to find processed flag column
    result = service.spreadsheets().values().get(
        spreadsheetId=SOURCE_SHEET_ID,
        range=f'{SOURCE_SHEET_NAME}!1:1'
    ).execute()

    headers = result.get('values', [['']])[0]

    try:
        processed_col_idx = headers.index(COL_PROCESSED_FLAG)
    except ValueError:
        processed_col_idx = len(headers)
        col_letter = column_index_to_letter(processed_col_idx)
        service.spreadsheets().values().update(
            spreadsheetId=SOURCE_SHEET_ID,
            range=f'{SOURCE_SHEET_NAME}!{col_letter}1',
            valueInputOption='RAW',
            body={'values': [[COL_PROCESSED_FLAG]]}
        ).execute()

    col_letter = column_index_to_letter(processed_col_idx)

    # Batch update
    batch_data = []
    for row_num in row_numbers:
        batch_data.append({
            'range': f'{SOURCE_SHEET_NAME}!{col_letter}{row_num}',
            'values': [['TRUE']]
        })

    # Update in chunks
    chunk_size = 50
    for i in range(0, len(batch_data), chunk_size):
        chunk = batch_data[i:i + chunk_size]
        body = {'valueInputOption': 'RAW', 'data': chunk}
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=SOURCE_SHEET_ID,
            body=body
        ).execute()

        if i + chunk_size < len(batch_data):
            time.sleep(0.5)

    logger.info(f"Updated processed flag for {len(row_numbers)} rows")


def clear_processed_flags(creds):
    """Clear all Processed_Flag values in source sheet (DEV_MODE only)."""
    service = get_sheets_service(creds)

    # Get headers to find processed flag column
    result = service.spreadsheets().values().get(
        spreadsheetId=SOURCE_SHEET_ID,
        range=f'{SOURCE_SHEET_NAME}!1:1'
    ).execute()

    headers = result.get('values', [['']])[0]

    try:
        processed_col_idx = headers.index(COL_PROCESSED_FLAG)
    except ValueError:
        logger.info("No Processed_Flag column found, nothing to clear")
        return

    col_letter = column_index_to_letter(processed_col_idx)

    # Get row count
    result = service.spreadsheets().values().get(
        spreadsheetId=SOURCE_SHEET_ID,
        range=f'{SOURCE_SHEET_NAME}!A:A'
    ).execute()
    row_count = len(result.get('values', []))

    # Clear the entire column (except header)
    if row_count > 1:
        clear_range = f'{SOURCE_SHEET_NAME}!{col_letter}2:{col_letter}{row_count}'
        service.spreadsheets().values().clear(
            spreadsheetId=SOURCE_SHEET_ID,
            range=clear_range
        ).execute()
        logger.info(f"Cleared Processed_Flag for {row_count - 1} rows")


def clear_output_sheet(creds):
    """Clear the output sheet (DEV_MODE only)."""
    service = get_sheets_service(creds)

    try:
        service.spreadsheets().values().clear(
            spreadsheetId=OUTPUT_SHEET_ID,
            range=f'{OUTPUT_SHEET_NAME}!A:Z'
        ).execute()
        logger.info("Cleared output sheet")
    except Exception as e:
        logger.warning(f"Could not clear output sheet: {e}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main execution."""
    global DEV_MODE

    # Interactive mode selection (unless environment variable is explicitly set)
    env_dev_mode = os.environ.get('DEV_MODE')
    if env_dev_mode is None:
        print("\n" + "=" * 50)
        print("SELECT PROCESSING MODE")
        print("=" * 50)
        print("\n  [1] Production")
        print("      Appends new rows, skips already-processed transactions")
        print("      Best for: daily/regular runs, preserves existing data")
        print("")
        print("  [2] Development")
        print("      Clears output sheet, reprocesses all transactions")
        print("      Best for: testing, debugging, full refresh\n")

        while True:
            choice = input("Enter choice [1/2] (default: 1): ").strip()
            if choice == '' or choice == '1':
                DEV_MODE = False
                print("→ Using PRODUCTION mode\n")
                break
            elif choice == '2':
                DEV_MODE = True
                print("→ Using DEVELOPMENT mode\n")
                break
            else:
                print("Invalid choice. Enter 1 or 2.")

    logger.info("=" * 60)
    logger.info("Transaction Matcher - Starting")
    if DEV_MODE:
        logger.info("*** DEV MODE ENABLED - Will reprocess all transactions ***")
        logger.info("*** To skip prompt: export DEV_MODE=false ***")
    else:
        logger.info("*** PRODUCTION MODE - Only processing new transactions ***")
        logger.info("*** To skip prompt: export DEV_MODE=true ***")
    logger.info("=" * 60)

    # Get credentials
    creds = get_credentials_oauth()

    # Validate sheet access early to fail fast with clear errors
    logger.info("Validating sheet access...")
    try:
        validate_sheet_access(SOURCE_SHEET_ID, "Source Transactions", creds)
        validate_sheet_access(PARSED_ORDERS_SHEET_ID, "Parsed Orders", creds)
        validate_sheet_access(OUTPUT_SHEET_ID, "Output", creds)
    except ValueError as e:
        logger.error(str(e))
        return

    # Check cell usage and warn if approaching limit
    logger.info("Checking sheet cell usage...")
    for sheet_id, name in [
        (PARSED_ORDERS_SHEET_ID, "Parsed Orders"),
        (OUTPUT_SHEET_ID, "Output")
    ]:
        usage = check_sheet_cell_usage(sheet_id, creds)
        if usage.get('warning'):
            logger.warning(f"*** {name} sheet approaching cell limit! Consider archiving old data. ***")

    # Check Gmail watch status (for real-time email processing)
    logger.info("Checking Gmail watch status...")
    watch_status = check_gmail_watch_status()
    if watch_status.get('warning'):
        logger.warning("*** Gmail watch issue detected - real-time email processing may be affected ***")

    # DEV MODE: Clear processed flags and output sheet for fresh run
    if DEV_MODE:
        logger.info("DEV MODE: Clearing processed flags and output sheet")
        clear_processed_flags(creds)
        clear_output_sheet(creds)

    # Load data
    logger.info("Step 1: Loading transactions")
    all_transactions, col_indices = load_transactions(creds)

    if not all_transactions:
        logger.info("No unprocessed transactions found")
        return

    # Filter by date if configured
    if FILTER_MONTH is not None or FILTER_YEAR is not None:
        logger.info("Step 2: Filtering by date")
        all_transactions = filter_by_date(all_transactions)

    # Filter Amazon transactions
    logger.info("Step 3: Identifying Amazon transactions")
    amazon_transactions = filter_amazon_transactions(all_transactions)
    amazon_transactions = deduplicate_transactions(amazon_transactions)

    if not amazon_transactions:
        logger.info("No Amazon transactions to match")
        # Still write non-Amazon transactions to output
        output_rows = generate_output_rows(all_transactions, [], {})
        write_output(output_rows, creds)
        return

    # Calculate date range for parsed orders (reduces memory usage)
    trans_dates = [parse_date(t.get(COL_DATE)) for t in amazon_transactions]
    trans_dates = [d for d in trans_dates if d is not None]
    if trans_dates:
        min_trans_date = min(trans_dates) - timedelta(days=DATE_MATCHING_WINDOW_DAYS + 7)
        max_trans_date = max(trans_dates) + timedelta(days=DATE_MATCHING_WINDOW_DAYS + 7)
    else:
        min_trans_date = None
        max_trans_date = None

    # Load parsed orders (filtered by date range)
    logger.info("Step 4: Loading parsed orders")
    parsed_orders = load_parsed_orders(creds, min_date=min_trans_date, max_date=max_trans_date)

    if not parsed_orders:
        logger.warning("No parsed orders found. Run backfill_emails.py first.")
        return

    # Match transactions using optimal assignment (scores all pairs first)
    logger.info("Step 5: Matching transactions to parsed orders (optimal assignment)")
    match_results = match_all_transactions_optimally(amazon_transactions, parsed_orders)

    # Count results
    matched_count = sum(1 for _, (_, _, s) in match_results.items() if s == 'matched')
    low_confidence_count = sum(1 for _, (_, _, s) in match_results.items() if s == 'low_confidence')
    unmatched_count = sum(1 for _, (_, _, s) in match_results.items() if s == 'unmatched')

    # Log individual results
    for row_num, (items, confidence, status) in sorted(match_results.items()):
        if status == 'matched':
            logger.info(f"  Row {row_num}: MATCHED (confidence: {confidence})")
        elif status == 'low_confidence':
            logger.warning(f"  Row {row_num}: LOW CONFIDENCE (score: {confidence})")
        else:
            logger.warning(f"  Row {row_num}: UNMATCHED")

    # Count known patterns among unmatched
    known_pattern_count = 0
    truly_unmatched_count = 0
    for trans in amazon_transactions:
        row_num = trans.get('_row_number')
        if row_num in match_results:
            _, _, status = match_results[row_num]
            if status == 'unmatched':
                desc = str(trans.get(COL_DESCRIPTION, ''))
                if get_known_amazon_pattern(desc):
                    known_pattern_count += 1
                else:
                    truly_unmatched_count += 1

    # Summary
    logger.info("")
    logger.info(f"Match Results:")
    logger.info(f"  - Matched: {matched_count}")
    logger.info(f"  - Low confidence: {low_confidence_count}")
    logger.info(f"  - Known patterns (subscriptions/digital): {known_pattern_count}")
    logger.info(f"  - Truly unmatched: {truly_unmatched_count}")

    # Generate output
    logger.info("Step 6: Generating output rows")
    output_rows = generate_output_rows(all_transactions, amazon_transactions, match_results)

    # Write output
    logger.info("Step 7: Writing to output sheet")
    write_output(output_rows, creds, dev_mode=DEV_MODE)

    # Update processed flags for matched transactions (skip in DEV_MODE)
    if not DEV_MODE:
        matched_rows = [
            row_num for row_num, (_, _, status) in match_results.items()
            if status == 'matched'
        ]

        if matched_rows:
            logger.info("Step 8: Updating processed flags")
            update_processed_flags(matched_rows, creds)
    else:
        logger.info("Step 8: Skipping processed flag update (DEV_MODE)")

    logger.info("")
    logger.info("=" * 60)
    logger.info("Transaction Matcher - Complete")
    logger.info(f"Total output rows: {len(output_rows)}")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
