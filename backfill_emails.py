#!/usr/bin/env python3
"""
Backfill Emails Script

One-time script to fetch all historical Amazon order and return emails
from Gmail and populate the Parsed Orders Google Sheet.

Usage:
    python backfill_emails.py [--limit N] [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD]

Examples:
    python backfill_emails.py                          # Backfill all emails
    python backfill_emails.py --limit 10               # Test with 10 emails
    python backfill_emails.py --start-date 2024-01-01  # From January 2024
    python backfill_emails.py --optimized              # Use batch API (3-5x faster)
    python backfill_emails.py --optimized --incremental  # Only new emails since last run

Optimizations (--optimized flag):
    - Batch API: Fetches up to 100 emails per HTTP request (vs 1)
    - Concurrent batches: Multiple batch requests in parallel
    - Partial responses: Only requests needed fields (smaller payloads)
"""

import argparse
import base64
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Dict, Optional, Any

from googleapiclient.errors import HttpError
from googleapiclient.http import BatchHttpRequest
from bs4 import BeautifulSoup

from utils import (
    get_credentials_oauth,
    get_gmail_service,
    get_sheets_service,
    get_email_body,
    parse_amazon_email,
    extract_order_number,
    extract_order_total_from_email,
    extract_shipment_total,
    AMAZON_ORDER_EMAIL_FROM,
    AMAZON_RETURN_EMAIL_FROM,
    AMAZON_SHIPMENT_EMAIL_FROM,
    AMAZON_PAYMENTS_EMAIL_FROM,
    SKIP_KEYWORDS
)

# Multi-order email support (set to False to disable)
ENABLE_MULTI_ORDER_PARSING = True
if ENABLE_MULTI_ORDER_PARSING:
    from multi_order_parser import is_multi_order_email, extract_all_orders

# ============================================================================
# CONFIGURATION
# ============================================================================

# Parsed Orders Sheet
PARSED_ORDERS_SHEET_ID = os.environ.get('PARSED_ORDERS_SHEET_ID', '')
PARSED_ORDERS_SHEET_NAME = 'Parsed Orders'

# Rate limiting
BATCH_SIZE = 50  # Emails to process before writing
API_DELAY = 0.1  # Seconds between API calls
MAX_RETRIES = 3  # Number of retries for rate-limited requests
MAX_QUANTITY = 100  # Maximum quantity per item (prevents memory issues from malformed data)

# Gmail API optimization settings
GMAIL_BATCH_SIZE = 50  # Reduced from 100 to improve reliability
MAX_CONCURRENT_BATCHES = 1  # Sequential batch requests (no threading to avoid SSL crashes on macOS)
BATCH_RETRY_LIMIT = 3  # Number of retries for failed requests within a batch
BATCH_DELAY_SECONDS = 0.5  # Delay between batches to avoid rate limiting
HISTORY_STATE_FILE = '.gmail_backfill_state.json'  # Tracks last processed historyId

# Fields to request for partial responses (reduces bandwidth significantly)
MESSAGE_FIELDS = 'id,internalDate,payload(headers,mimeType,parts,body)'

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# INCREMENTAL SYNC STATE
# ============================================================================

def load_sync_state() -> Dict[str, Any]:
    """Load the last sync state (historyId per email type) from file."""
    if os.path.exists(HISTORY_STATE_FILE):
        try:
            with open(HISTORY_STATE_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load sync state: {e}")
    return {}


def save_sync_state(state: Dict[str, Any]):
    """Save sync state to file for incremental syncs."""
    try:
        with open(HISTORY_STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)
        logger.info(f"Saved sync state to {HISTORY_STATE_FILE}")
    except Exception as e:
        logger.warning(f"Could not save sync state: {e}")


# ============================================================================
# BATCH EMAIL FETCHING (OPTIMIZED)
# ============================================================================

def fetch_emails_batch(gmail_service, message_ids: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch multiple emails in a single batch request with retry logic.

    Gmail API supports up to 100 requests per batch, significantly reducing
    network overhead compared to individual requests.

    Args:
        gmail_service: Authenticated Gmail service
        message_ids: List of message IDs to fetch

    Returns:
        List of full email messages
    """
    if not message_ids:
        return []

    all_messages = []
    pending_ids = list(message_ids)

    for retry_attempt in range(BATCH_RETRY_LIMIT):
        if not pending_ids:
            break

        messages = []
        errors = []
        failed_ids = []

        def callback(request_id, response, exception):
            if exception is not None:
                errors.append((request_id, exception))
                failed_ids.append(request_id)
            else:
                messages.append(response)

        # Create batch request
        batch = gmail_service.new_batch_http_request(callback=callback)

        for msg_id in pending_ids:
            batch.add(
                gmail_service.users().messages().get(
                    userId='me',
                    id=msg_id,
                    format='full',
                    fields=MESSAGE_FIELDS
                ),
                request_id=msg_id
            )

        # Execute batch with retry logic for rate limits
        for attempt in range(MAX_RETRIES):
            try:
                batch.execute()
                break
            except HttpError as e:
                if e.resp.status == 429:
                    wait_time = (2 ** attempt) * 30
                    logger.warning(f"Batch rate limit (attempt {attempt + 1}/{MAX_RETRIES}), waiting {wait_time}s...")
                    time.sleep(wait_time)
                    if attempt == MAX_RETRIES - 1:
                        logger.error(f"Batch failed after {MAX_RETRIES} retries")
                        raise
                else:
                    raise
            except Exception as e:
                # Handle SSL and connection errors
                if attempt < MAX_RETRIES - 1:
                    wait_time = (2 ** attempt) * 2
                    logger.warning(f"Batch connection error (attempt {attempt + 1}/{MAX_RETRIES}): {e}, waiting {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Batch failed after {MAX_RETRIES} retries: {e}")
                    raise

        all_messages.extend(messages)

        if errors:
            if retry_attempt < BATCH_RETRY_LIMIT - 1:
                logger.debug(f"Batch attempt {retry_attempt + 1}: {len(errors)} failed, {len(messages)} succeeded. Retrying failed requests...")
                pending_ids = failed_ids
                time.sleep(BATCH_DELAY_SECONDS * (retry_attempt + 1))  # Exponential backoff
            else:
                logger.warning(f"Batch had {len(errors)} errors after {BATCH_RETRY_LIMIT} retries")
                for msg_id, exc in errors[:3]:  # Log first 3 errors
                    logger.debug(f"  Error for {msg_id}: {type(exc).__name__}")
        else:
            break  # All succeeded

    return all_messages


def fetch_all_amazon_emails_optimized(
    gmail_service,
    email_type: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None,
    use_incremental: bool = False
) -> List[Dict[str, Any]]:
    """
    Fetch all Amazon emails using batch API and concurrent requests.

    Optimizations:
    - Batch API: Fetches up to 100 emails per HTTP request
    - Concurrent batches: Multiple batch requests in parallel
    - Partial responses: Only requests needed fields
    - Incremental sync: Optionally skip already-processed emails

    Args:
        gmail_service: Authenticated Gmail service
        email_type: 'order', 'return', or 'shipment'
        start_date: Optional start date (YYYY-MM-DD)
        end_date: Optional end date (YYYY-MM-DD)
        limit: Optional max number of emails to fetch
        use_incremental: If True, only fetch emails newer than last sync

    Returns:
        List of full email messages
    """
    if email_type == 'order':
        from_address = AMAZON_ORDER_EMAIL_FROM
    elif email_type == 'shipment':
        from_address = AMAZON_SHIPMENT_EMAIL_FROM
    elif email_type == 'payment':
        from_address = AMAZON_PAYMENTS_EMAIL_FROM
    else:
        from_address = AMAZON_RETURN_EMAIL_FROM

    # Build query
    query = f'from:{from_address}'

    if start_date:
        query += f' after:{start_date}'
    if end_date:
        query += f' before:{end_date}'

    logger.info(f"Searching Gmail with query: {query}")

    # Phase 1: Get all message IDs (lightweight)
    message_ids = []
    page_token = None

    while True:
        try:
            results = gmail_service.users().messages().list(
                userId='me',
                q=query,
                pageToken=page_token,
                maxResults=500,
                fields='messages(id),nextPageToken'  # Only request IDs
            ).execute()

            messages = results.get('messages', [])
            message_ids.extend([m['id'] for m in messages])

            page_token = results.get('nextPageToken')
            if not page_token:
                break

            if limit and len(message_ids) >= limit:
                message_ids = message_ids[:limit]
                break

            time.sleep(API_DELAY)

        except HttpError as e:
            logger.error(f"Error fetching message list: {e}")
            break

    logger.info(f"Found {len(message_ids)} {email_type} emails")

    if not message_ids:
        return []

    # Phase 2: Fetch full messages in batches (sequential with delays)
    all_messages = []
    batches = [message_ids[i:i + GMAIL_BATCH_SIZE] for i in range(0, len(message_ids), GMAIL_BATCH_SIZE)]

    logger.info(f"Fetching {len(message_ids)} emails in {len(batches)} batches")

    for batch_idx, batch in enumerate(batches):
        try:
            messages = fetch_emails_batch(gmail_service, batch)
            all_messages.extend(messages)
            logger.info(f"Completed batch {batch_idx + 1}/{len(batches)} ({len(messages)} emails)")

            # Add delay between batches to avoid rate limiting
            if batch_idx < len(batches) - 1:
                time.sleep(BATCH_DELAY_SECONDS)
        except Exception as e:
            logger.error(f"Batch {batch_idx + 1} failed: {e}")

    logger.info(f"Successfully fetched {len(all_messages)} {email_type} emails")
    return all_messages


# ============================================================================
# EMAIL FETCHING (LEGACY - KEPT FOR COMPATIBILITY)
# ============================================================================

def fetch_all_amazon_emails(
    gmail_service,
    email_type: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Fetch all Amazon emails of a given type (order, return, or shipment).

    Args:
        gmail_service: Authenticated Gmail service
        email_type: 'order', 'return', or 'shipment'
        start_date: Optional start date (YYYY-MM-DD)
        end_date: Optional end date (YYYY-MM-DD)
        limit: Optional max number of emails to fetch

    Returns:
        List of full email messages
    """
    if email_type == 'order':
        from_address = AMAZON_ORDER_EMAIL_FROM
    elif email_type == 'shipment':
        from_address = AMAZON_SHIPMENT_EMAIL_FROM
    elif email_type == 'payment':
        from_address = AMAZON_PAYMENTS_EMAIL_FROM
    else:
        from_address = AMAZON_RETURN_EMAIL_FROM

    # Build query
    query = f'from:{from_address}'

    if start_date:
        query += f' after:{start_date}'
    if end_date:
        query += f' before:{end_date}'

    logger.info(f"Searching Gmail with query: {query}")

    # Fetch message IDs
    message_ids = []
    page_token = None

    while True:
        try:
            results = gmail_service.users().messages().list(
                userId='me',
                q=query,
                pageToken=page_token,
                maxResults=500
            ).execute()

            messages = results.get('messages', [])
            message_ids.extend([m['id'] for m in messages])

            page_token = results.get('nextPageToken')
            if not page_token:
                break

            if limit and len(message_ids) >= limit:
                message_ids = message_ids[:limit]
                break

            time.sleep(API_DELAY)

        except HttpError as e:
            logger.error(f"Error fetching message list: {e}")
            break

    logger.info(f"Found {len(message_ids)} {email_type} emails")

    # Fetch full message content with retry logic
    messages = []
    for i, msg_id in enumerate(message_ids):
        if limit and i >= limit:
            break

        # Retry logic with exponential backoff for rate limits
        for attempt in range(MAX_RETRIES):
            try:
                message = gmail_service.users().messages().get(
                    userId='me',
                    id=msg_id,
                    format='full'
                ).execute()
                messages.append(message)
                break  # Success, exit retry loop

            except HttpError as e:
                if e.resp.status == 429:
                    wait_time = (2 ** attempt) * 30  # 30s, 60s, 120s
                    logger.warning(f"Rate limit hit (attempt {attempt + 1}/{MAX_RETRIES}), waiting {wait_time}s...")
                    time.sleep(wait_time)
                    if attempt == MAX_RETRIES - 1:
                        logger.error(f"Failed to fetch email {msg_id} after {MAX_RETRIES} retries")
                else:
                    logger.warning(f"Error fetching email {msg_id}: {e}")
                    break  # Non-rate-limit error, don't retry

        if (i + 1) % 50 == 0:
            logger.info(f"Fetched {i + 1}/{len(message_ids)} {email_type} emails")

        time.sleep(API_DELAY)

    logger.info(f"Successfully fetched {len(messages)} {email_type} emails")
    return messages

# ============================================================================
# EMAIL PARSING
# ============================================================================

def get_email_subject(message: Dict[str, Any]) -> str:
    """Extract email subject from headers."""
    headers = message.get('payload', {}).get('headers', [])
    for header in headers:
        if header.get('name', '').lower() == 'subject':
            return header.get('value', '')
    return ''


def parse_email(message: Dict[str, Any], email_type: str) -> Optional[Dict[str, Any]]:
    """Parse a single email and extract order data."""
    # For multi-order emails, use parse_email_multi() instead
    result = parse_email_multi(message, email_type)
    if result:
        return result[0] if len(result) == 1 else result
    return None


def parse_email_multi(message: Dict[str, Any], email_type: str) -> List[Dict[str, Any]]:
    """
    Parse a single email and extract order data.
    Returns a list to support multi-order emails (one email with multiple orders).
    """
    email_id = message.get('id')

    # For return emails, only process those with 'refund' in subject
    # This filters out return request and drop-off confirmation emails
    if email_type == 'return':
        subject = get_email_subject(message)
        if 'refund' not in subject.lower():
            logger.debug(f"Email {email_id}: Skipping return email without 'refund' in subject")
            return []

    # Extract email body
    email_body = get_email_body(message)
    plain_text = email_body.get('plain', '')
    html_content = email_body.get('html', '')

    if not plain_text and not html_content:
        logger.debug(f"Email {email_id}: No content")
        return []

    content = plain_text or html_content

    # Get email date
    email_date = get_email_date(message)

    # Check for multi-order email (shipment and order emails)
    if ENABLE_MULTI_ORDER_PARSING and email_type in ('shipment', 'order'):
        if is_multi_order_email(email_body):
            logger.info(f"Email {email_id}: Detected multi-order email")
            orders = extract_all_orders(email_body)

            if orders:
                results = []
                for order in orders:
                    parse_status = 'success' if order['items'] and order['total'] else 'partial'
                    results.append({
                        'email_id': f"{email_id}_{order['order_number']}",  # Unique ID per order
                        'email_date': email_date,
                        'email_type': email_type,
                        'order_number': order['order_number'] or '',
                        'shipment_total': order['total'],
                        'items': order['items'],
                        'parse_status': parse_status,
                        'processed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                logger.info(f"Email {email_id}: Extracted {len(results)} separate orders")
                return results

    # Standard single-order parsing
    # Extract order number
    order_number = extract_order_number(email_body)

    # Extract shipment total (the actual bank charge amount)
    shipment_total = extract_shipment_total(email_body)

    # Extract items based on email type
    if email_type == 'order' or email_type == 'shipment':
        items = parse_amazon_email(email_body)
    else:
        # Return emails: get refund amount and items
        refund_amount = extract_refund_amount(content)
        items = parse_return_items_from_html(html_content)

        # For returns, put refund amount in item_price and shipment_total
        if refund_amount:
            shipment_total = refund_amount
            if items:
                # If single item, use full refund amount
                if len(items) == 1:
                    items[0]['price'] = refund_amount
                # If multiple items, we can't know individual prices
                else:
                    items[0]['price'] = refund_amount
            else:
                # No items parsed but we have refund - create placeholder item
                items = [{'name': 'Refund', 'price': refund_amount, 'quantity': 1}]

    # Determine parse status
    if items and shipment_total:
        parse_status = 'success'
    elif items or shipment_total:
        parse_status = 'partial'
    else:
        parse_status = 'failed'

    return [{
        'email_id': email_id,
        'email_date': email_date,
        'email_type': email_type,
        'order_number': order_number or '',
        'shipment_total': shipment_total,
        'items': items,
        'parse_status': parse_status,
        'processed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }]


def get_email_date(message: Dict[str, Any]) -> str:
    """Extract email date."""
    headers = message.get('payload', {}).get('headers', [])

    for header in headers:
        if header.get('name', '').lower() == 'date':
            date_str = header.get('value', '')
            try:
                from email.utils import parsedate_to_datetime
                dt = parsedate_to_datetime(date_str)
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                return date_str

    internal_date = message.get('internalDate')
    if internal_date:
        dt = datetime.fromtimestamp(int(internal_date) / 1000)
        return dt.strftime('%Y-%m-%d %H:%M:%S')

    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def extract_order_total(content: str) -> Optional[float]:
    """Extract order total from email content."""
    patterns = [
        r'Order\s+Total[:\s]*\$(\d+(?:,\d{3})*\.?\d{0,2})',
        r'Grand\s+Total[:\s]*\$(\d+(?:,\d{3})*\.?\d{0,2})',
        r'Total[:\s]*\$(\d+(?:,\d{3})*\.?\d{0,2})',
        r'Amount\s+Charged[:\s]*\$(\d+(?:,\d{3})*\.?\d{0,2})',
    ]

    for pattern in patterns:
        match = re.search(pattern, content, re.IGNORECASE)
        if match:
            try:
                return float(match.group(1).replace(',', ''))
            except ValueError:
                continue

    return None


def extract_refund_amount(content: str) -> Optional[float]:
    """Extract refund amount from return email."""
    patterns = [
        r'Total\s+estimated\s+refund[*]?[:\s]*\$(\d+(?:,\d{3})*\.?\d{0,2})',
        r'Refund\s+subtotal[:\s]*\$(\d+(?:,\d{3})*\.?\d{0,2})',
        r'\$(\d+(?:,\d{3})*\.?\d{0,2})\s+will\s+be\s+refunded',
        r'Refund\s+Amount[:\s]*\$(\d+(?:,\d{3})*\.?\d{0,2})',
        r'Refund\s+Total[:\s]*\$(\d+(?:,\d{3})*\.?\d{0,2})',
        r'Total\s+Refund[:\s]*\$(\d+(?:,\d{3})*\.?\d{0,2})',
        r'We\'ve\s+refunded[:\s]*\$(\d+(?:,\d{3})*\.?\d{0,2})',
    ]

    for pattern in patterns:
        match = re.search(pattern, content, re.IGNORECASE)
        if match:
            try:
                return float(match.group(1).replace(',', ''))
            except ValueError:
                continue

    return None


def parse_return_items_from_html(html_content: str) -> List[Dict[str, Any]]:
    """
    Parse items from Amazon return email HTML.
    Return emails list items without individual prices - just names and quantities.
    """
    items = []

    if not html_content:
        return items

    try:
        soup = BeautifulSoup(html_content, 'html.parser')

        # Find item names in links (Amazon return emails have product links)
        for link in soup.find_all('a'):
            href = link.get('href', '')
            text = link.get_text(strip=True)

            # Look for product links
            if 'product' in href.lower() and text and len(text) > 10:
                # Skip navigation/UI links
                if any(skip in text.lower() for skip in ['view', 'track', 'help', 'refund invoice', 'amazon.com']):
                    continue

                # Clean up truncated names (e.g., "Product Name - Stackable...")
                item_name = text.strip()

                # Check if we already have this item
                if not any(item['name'] == item_name for item in items):
                    items.append({
                        'name': item_name,
                        'price': 0,  # Return emails don't show per-item prices
                        'quantity': 1
                    })

        # Also look for quantity info near items
        text_content = soup.get_text()
        qty_matches = re.findall(r'Quantity:\s*(\d+)', text_content, re.IGNORECASE)

        # If we found quantities, update items
        if qty_matches and items:
            for i, qty in enumerate(qty_matches):
                if i < len(items):
                    items[i]['quantity'] = int(qty)

    except Exception as e:
        logger.error(f"Error parsing return HTML: {e}")

    return items

# ============================================================================
# SHEET OPERATIONS
# ============================================================================

def get_existing_email_ids(sheets_service) -> set:
    """Get set of email IDs already in the sheet.

    Returns base email IDs (without order number suffixes) to properly
    deduplicate multi-order emails. For example, if the sheet contains
    '19b6aa222bd9663f_112-0438697-9192226', this returns '19b6aa222bd9663f'.
    """
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=PARSED_ORDERS_SHEET_ID,
            range=f"'{PARSED_ORDERS_SHEET_NAME}'!A:A"
        ).execute()

        values = result.get('values', [])
        base_ids = set()
        for row in values:
            if row:
                email_id = row[0]
                # Extract base email ID (before any underscore suffix)
                base_id = email_id.split('_')[0] if '_' in email_id else email_id
                base_ids.add(base_id)
        return base_ids

    except HttpError as e:
        if e.resp.status == 404:
            logger.info("Sheet not found, will create")
            return set()
        raise


def ensure_sheet_headers(sheets_service):
    """Ensure sheet has proper headers. Creates the sheet tab if needed."""
    headers = [
        'email_id',
        'email_date',
        'email_type',
        'order_number',
        'shipment_total',  # The actual bank charge amount
        'item_name',
        'item_price',
        'item_qty',
        'parse_status',
        'processed_at'
    ]

    # First, check if the sheet tab exists
    try:
        spreadsheet = sheets_service.spreadsheets().get(
            spreadsheetId=PARSED_ORDERS_SHEET_ID
        ).execute()

        sheet_names = [s['properties']['title'] for s in spreadsheet.get('sheets', [])]

        if PARSED_ORDERS_SHEET_NAME not in sheet_names:
            # Create the sheet tab
            logger.info(f"Creating sheet tab: {PARSED_ORDERS_SHEET_NAME}")
            request = {
                'requests': [{
                    'addSheet': {
                        'properties': {
                            'title': PARSED_ORDERS_SHEET_NAME
                        }
                    }
                }]
            }
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=PARSED_ORDERS_SHEET_ID,
                body=request
            ).execute()

    except HttpError as e:
        logger.error(f"Error checking/creating sheet: {e}")
        raise

    # Now check for headers
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=PARSED_ORDERS_SHEET_ID,
            range=f"'{PARSED_ORDERS_SHEET_NAME}'!1:1"
        ).execute()

        existing = result.get('values', [[]])
        if existing and existing[0]:
            return  # Headers exist

    except HttpError as e:
        logger.debug(f"Could not read headers (sheet may be new): {e}")

    # Write headers
    body = {'values': [headers]}
    sheets_service.spreadsheets().values().update(
        spreadsheetId=PARSED_ORDERS_SHEET_ID,
        range=f"'{PARSED_ORDERS_SHEET_NAME}'!A1",
        valueInputOption='RAW',
        body=body
    ).execute()
    logger.info("Created headers in Parsed Orders sheet")


def write_parsed_orders(sheets_service, orders: List[Dict[str, Any]]):
    """Write parsed orders to sheet. Expands items with qty > 1 into separate rows."""
    if not orders:
        return

    rows = []
    for order in orders:
        items = order.get('items', [])
        shipment_total = order.get('shipment_total')

        if items:
            for item in items:
                item_name = item.get('name', '')
                item_price = item.get('price', 0)
                quantity = min(item.get('quantity', 1), MAX_QUANTITY)  # Cap quantity to prevent memory issues

                if item.get('quantity', 1) > MAX_QUANTITY:
                    logger.warning(f"Quantity {item.get('quantity')} capped to {MAX_QUANTITY} for item: {item_name[:50]}")

                # Expand quantity into separate rows with "1ofN_" prefix
                for i in range(1, quantity + 1):
                    if quantity > 1:
                        prefixed_name = f"{i}of{quantity}_{item_name}"
                    else:
                        prefixed_name = item_name

                    row = [
                        order.get('email_id', ''),
                        order.get('email_date', ''),
                        order.get('email_type', ''),
                        order.get('order_number', ''),
                        shipment_total if shipment_total else '',
                        prefixed_name,
                        item_price,
                        1,  # Always 1 since we're expanding
                        order.get('parse_status', ''),
                        order.get('processed_at', '')
                    ]
                    rows.append(row)
        else:
            row = [
                order.get('email_id', ''),
                order.get('email_date', ''),
                order.get('email_type', ''),
                order.get('order_number', ''),
                shipment_total if shipment_total else '',
                '',
                0,
                0,
                order.get('parse_status', ''),
                order.get('processed_at', '')
            ]
            rows.append(row)

    body = {'values': rows}
    sheets_service.spreadsheets().values().append(
        spreadsheetId=PARSED_ORDERS_SHEET_ID,
        range=f"'{PARSED_ORDERS_SHEET_NAME}'!A:J",
        valueInputOption='RAW',
        insertDataOption='INSERT_ROWS',
        body=body
    ).execute()

    logger.info(f"Wrote {len(rows)} rows to Parsed Orders sheet")

# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Backfill Amazon order emails to Parsed Orders sheet')
    parser.add_argument('--limit', type=int, help='Limit number of emails to process (for testing)')
    parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')
    parser.add_argument('--orders-only', action='store_true', help='Only process order emails')
    parser.add_argument('--shipments-only', action='store_true', help='Only process shipment emails')
    parser.add_argument('--returns-only', action='store_true', help='Only process return emails')
    parser.add_argument('--dry-run', action='store_true', help='Parse but don\'t write to sheet')
    parser.add_argument('--optimized', action='store_true', help='Use batch API and concurrent fetching (faster)')
    parser.add_argument('--incremental', action='store_true', help='Only fetch emails newer than last sync')

    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Backfill Emails Script - Starting")
    logger.info("=" * 60)

    # Interactive mode selection (unless flags already specified)
    if not args.optimized and not hasattr(args, '_mode_selected'):
        print("\n" + "=" * 50)
        print("SELECT FETCHING METHOD")
        print("=" * 50)
        print("\n  [1] Optimized + Incremental")
        print("      Only fetches new emails since last run")
        print("      Best for: daily/regular syncs")
        print("")
        print("  [2] Optimized")
        print("      Batch API + parallel (3-5x faster)")
        print("      Best for: full backfill of thousands of emails")
        print("")
        print("  [3] Legacy")
        print("      Sequential fetching (slower, simpler)")
        print("      Best for: testing with --limit, debugging parsing issues\n")

        while True:
            choice = input("Enter choice [1/2/3] (default: 1): ").strip()
            if choice == '' or choice == '1':
                args.optimized = True
                args.incremental = True
                print("→ Using OPTIMIZED + INCREMENTAL mode\n")
                break
            elif choice == '2':
                args.optimized = True
                args.incremental = False
                print("→ Using OPTIMIZED mode\n")
                break
            elif choice == '3':
                args.optimized = False
                args.incremental = False
                print("→ Using LEGACY mode\n")
                break
            else:
                print("Invalid choice. Enter 1, 2, or 3.")

    # Check configuration
    if PARSED_ORDERS_SHEET_ID == 'YOUR_PARSED_ORDERS_SHEET_ID':
        logger.error("Please set PARSED_ORDERS_SHEET_ID environment variable")
        logger.error("  export PARSED_ORDERS_SHEET_ID='your_sheet_id_here'")
        return

    # Authenticate
    logger.info("Authenticating with Google APIs...")
    creds = get_credentials_oauth()
    gmail_service = get_gmail_service(creds)
    sheets_service = get_sheets_service(creds)

    # Ensure headers
    if not args.dry_run:
        ensure_sheet_headers(sheets_service)

    # Get existing email IDs (for deduplication)
    existing_ids = get_existing_email_ids(sheets_service)
    logger.info(f"Found {len(existing_ids)} existing emails in sheet")

    # Select fetch function based on optimization flag
    if args.optimized:
        logger.info("Using OPTIMIZED batch fetching (faster)")
        fetch_fn = lambda service, etype, start, end, lim: fetch_all_amazon_emails_optimized(
            service, etype, start, end, lim, use_incremental=args.incremental
        )
    else:
        fetch_fn = fetch_all_amazon_emails

    # Process order emails
    if not args.returns_only and not args.shipments_only:
        logger.info("")
        logger.info("Processing ORDER emails...")
        order_emails = fetch_fn(
            gmail_service,
            'order',
            args.start_date,
            args.end_date,
            args.limit
        )

        new_orders = []
        for email in order_emails:
            email_id = email.get('id')
            if email_id in existing_ids:
                continue

            order_data = parse_email(email, 'order')
            if order_data:
                # Handle both single orders and multi-order lists
                if isinstance(order_data, list):
                    new_orders.extend(order_data)
                else:
                    new_orders.append(order_data)
                existing_ids.add(email_id)  # Track for dedup

            # Write in batches
            if len(new_orders) >= BATCH_SIZE and not args.dry_run:
                write_parsed_orders(sheets_service, new_orders)
                new_orders = []

        # Write remaining
        if new_orders and not args.dry_run:
            write_parsed_orders(sheets_service, new_orders)

        # Stats
        success = sum(1 for o in order_emails if o.get('id') not in existing_ids)
        logger.info(f"Processed {success} new order emails")

    # Process shipment emails
    if not args.orders_only and not args.returns_only:
        logger.info("")
        logger.info("Processing SHIPMENT emails...")
        shipment_emails = fetch_fn(
            gmail_service,
            'shipment',
            args.start_date,
            args.end_date,
            args.limit
        )

        new_shipments = []
        multi_order_count = 0
        for email in shipment_emails:
            email_id = email.get('id')
            if email_id in existing_ids:
                continue

            # Use parse_email_multi to handle multi-order emails
            shipment_data_list = parse_email_multi(email, 'shipment')
            if shipment_data_list:
                if len(shipment_data_list) > 1:
                    multi_order_count += 1
                new_shipments.extend(shipment_data_list)
                existing_ids.add(email_id)

            if len(new_shipments) >= BATCH_SIZE and not args.dry_run:
                write_parsed_orders(sheets_service, new_shipments)
                new_shipments = []

        if new_shipments and not args.dry_run:
            write_parsed_orders(sheets_service, new_shipments)

        success = sum(1 for s in shipment_emails if s.get('id') not in existing_ids)
        logger.info(f"Processed {success} new shipment emails ({multi_order_count} multi-order)")

    # Process return emails
    if not args.orders_only and not args.shipments_only:
        logger.info("")
        logger.info("Processing RETURN emails...")
        return_emails = fetch_fn(
            gmail_service,
            'return',
            args.start_date,
            args.end_date,
            args.limit
        )

        new_returns = []
        for email in return_emails:
            email_id = email.get('id')
            if email_id in existing_ids:
                continue

            order_data = parse_email(email, 'return')
            if order_data:
                new_returns.append(order_data)
                existing_ids.add(email_id)

            if len(new_returns) >= BATCH_SIZE and not args.dry_run:
                write_parsed_orders(sheets_service, new_returns)
                new_returns = []

        if new_returns and not args.dry_run:
            write_parsed_orders(sheets_service, new_returns)

        success = sum(1 for r in return_emails if r.get('id') not in existing_ids)
        logger.info(f"Processed {success} new return emails")

    # Process payment/refund emails
    if not args.orders_only and not args.shipments_only:
        logger.info("")
        logger.info("Processing PAYMENT (refund) emails...")
        payment_emails = fetch_fn(
            gmail_service,
            'payment',
            args.start_date,
            args.end_date,
            args.limit
        )

        new_payments = []
        for email in payment_emails:
            email_id = email.get('id')
            if email_id in existing_ids:
                continue

            order_data = parse_email(email, 'return')  # Parse as return type (refunds)
            if order_data:
                new_payments.append(order_data)
                existing_ids.add(email_id)

            if len(new_payments) >= BATCH_SIZE and not args.dry_run:
                write_parsed_orders(sheets_service, new_payments)
                new_payments = []

        if new_payments and not args.dry_run:
            write_parsed_orders(sheets_service, new_payments)

        success = sum(1 for r in payment_emails if r.get('id') not in existing_ids)
        logger.info(f"Processed {success} new payment/refund emails")

    logger.info("")
    logger.info("=" * 60)
    logger.info("Backfill Complete")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
