#!/usr/bin/env python3
"""
Shared utilities for Amazon Order Parser ecosystem.

Contains:
- Google API authentication (OAuth for local scripts, service account for Cloud Functions)
- Email parsing functions (plain text + HTML fallback)
- Amount and date parsing utilities
- Sheets read/write helpers
"""

import os
import re
import base64
import logging
import time
from datetime import datetime
from typing import List, Dict, Optional, Any

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from bs4 import BeautifulSoup

# ============================================================================
# CONFIGURATION
# ============================================================================

# Google API Scopes
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/gmail.readonly'
]

# Default credential files (for local scripts)
CREDENTIALS_FILE = 'credentials.json'
TOKEN_FILE = 'token.json'
SERVICE_ACCOUNT_FILE = 'service_account.json'

# Amazon email senders
AMAZON_ORDER_EMAIL_FROM = 'auto-confirm@amazon.com'
AMAZON_RETURN_EMAIL_FROM = 'return@amazon.com'
AMAZON_SHIPMENT_EMAIL_FROM = 'shipment-tracking@amazon.com'
AMAZON_PAYMENTS_EMAIL_FROM = 'payments-messages@amazon.com'  # Refund notifications

# Logging
logger = logging.getLogger(__name__)

# Skip keywords for filtering out non-item lines
SKIP_KEYWORDS = [
    'order total', 'subtotal', 'tax', 'shipping', 'grand total',
    'item subtotal', 'total before tax', 'estimated tax', 'free shipping',
    'delivery', 'gift card', 'promotional', 'discount', 'savings',
    'credit card', 'payment method', 'billing', 'order #', 'order number',
    'arriving', 'delivered', 'track package', 'view order', 'edit order',
    'view or edit', 'continue shopping', 'deals related', 'items you\'ve saved'
]

# ============================================================================
# AUTHENTICATION
# ============================================================================

def get_credentials_oauth(credentials_file: str = CREDENTIALS_FILE,
                          token_file: str = TOKEN_FILE) -> Credentials:
    """
    Get valid user credentials from storage or prompt for authorization.
    Used by local scripts (transaction_matcher, backfill_emails).
    """
    creds = None

    # Load existing token
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)

    # If there are no (valid) credentials available, let the user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                logger.warning(f"Token refresh failed: {e}")
                logger.info("Will re-authenticate with OAuth flow...")
                # Delete invalid token and re-auth
                if os.path.exists(token_file):
                    os.remove(token_file)
                creds = None

        if not creds:
            if not os.path.exists(credentials_file):
                raise FileNotFoundError(
                    f"Credentials file '{credentials_file}' not found. "
                    "Please download it from Google Cloud Console."
                )
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_file, SCOPES
            )
            creds = flow.run_local_server(port=0)

        # Save credentials for next run
        with open(token_file, 'w') as token:
            token.write(creds.to_json())

    return creds


def get_credentials_service_account(service_account_file: str = SERVICE_ACCOUNT_FILE) -> Any:
    """
    Get credentials from service account file.
    Used by Cloud Functions.
    """
    if os.path.exists(service_account_file):
        return service_account.Credentials.from_service_account_file(
            service_account_file, scopes=SCOPES
        )
    # In Cloud Functions, use default credentials
    from google.auth import default
    creds, _ = default(scopes=SCOPES)
    return creds


def get_gmail_service(creds=None):
    """Get authenticated Gmail service."""
    if creds is None:
        creds = get_credentials_oauth()
    return build('gmail', 'v1', credentials=creds)


def get_sheets_service(creds=None):
    """Get authenticated Sheets service."""
    if creds is None:
        creds = get_credentials_oauth()
    return build('sheets', 'v4', credentials=creds)


def check_gmail_watch_status() -> Dict[str, Any]:
    """
    Check Gmail push notification watch status and warn if expired or expiring soon.

    Reads from .gmail_watch_state.json created by setup_gmail_watch.py.

    Returns:
        Dict with 'active', 'warning', 'message', 'days_remaining' keys
    """
    import json

    result = {
        'active': False,
        'warning': False,
        'message': '',
        'days_remaining': None
    }

    if not os.path.exists(GMAIL_WATCH_STATE_FILE):
        result['message'] = 'No Gmail watch configured (run setup_gmail_watch.py)'
        result['warning'] = True
        logger.warning(f"Gmail watch: {result['message']}")
        return result

    try:
        with open(GMAIL_WATCH_STATE_FILE, 'r') as f:
            state = json.load(f)

        expiration = state.get('expiration')
        if not expiration:
            result['message'] = 'Gmail watch state missing expiration'
            result['warning'] = True
            logger.warning(f"Gmail watch: {result['message']}")
            return result

        exp_ts = int(expiration) / 1000
        now_ts = datetime.now().timestamp()
        days_remaining = (exp_ts - now_ts) / 86400  # seconds per day

        result['days_remaining'] = days_remaining

        if now_ts > exp_ts:
            result['active'] = False
            result['warning'] = True
            result['message'] = f"Gmail watch EXPIRED on {state.get('expiration_readable')}. Run: python setup_gmail_watch.py --renew"
            logger.error(f"Gmail watch: {result['message']}")
        elif days_remaining < GMAIL_WATCH_WARNING_DAYS:
            result['active'] = True
            result['warning'] = True
            result['message'] = f"Gmail watch expires in {days_remaining:.1f} days. Consider renewing soon."
            logger.warning(f"Gmail watch: {result['message']}")
        else:
            result['active'] = True
            result['message'] = f"Gmail watch active, expires in {days_remaining:.1f} days"
            logger.info(f"Gmail watch: {result['message']}")

        return result

    except Exception as e:
        result['message'] = f'Error reading Gmail watch state: {e}'
        result['warning'] = True
        logger.error(f"Gmail watch: {result['message']}")
        return result


def validate_sheet_access(sheet_id: str, sheet_name: str, creds=None) -> bool:
    """
    Validate that a Google Sheet is accessible.

    Args:
        sheet_id: The Google Sheet ID
        sheet_name: Human-readable name for error messages
        creds: Optional credentials

    Returns:
        True if accessible

    Raises:
        ValueError: If sheet cannot be accessed with helpful error message
    """
    from googleapiclient.errors import HttpError

    service = get_sheets_service(creds)
    try:
        result = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        logger.info(f"Validated access to {sheet_name}: {result.get('properties', {}).get('title', 'Unknown')}")
        return True
    except HttpError as e:
        if e.resp.status == 404:
            raise ValueError(
                f"Cannot access {sheet_name} sheet (ID: {sheet_id}). "
                f"Sheet not found. Check that the ID is correct."
            )
        elif e.resp.status == 403:
            raise ValueError(
                f"Cannot access {sheet_name} sheet (ID: {sheet_id}). "
                f"Permission denied. Share the sheet with your service account or OAuth user."
            )
        else:
            raise ValueError(
                f"Cannot access {sheet_name} sheet (ID: {sheet_id}). "
                f"Error: {e}"
            )

# ============================================================================
# REGEX SAFETY
# ============================================================================

# Maximum input length for regex operations to prevent ReDoS attacks
MAX_REGEX_INPUT_LENGTH = 100000  # 100KB


def safe_regex_search(pattern: str, text: str, flags: int = 0, max_length: int = MAX_REGEX_INPUT_LENGTH):
    """
    Perform regex search with input length limit to prevent ReDoS.

    Args:
        pattern: Regex pattern
        text: Input text to search
        flags: Regex flags (e.g., re.IGNORECASE)
        max_length: Maximum input length (default 100KB)

    Returns:
        Match object or None
    """
    if not text:
        return None
    if len(text) > max_length:
        logger.warning(f"Input too long for regex ({len(text)} > {max_length}), truncating")
        text = text[:max_length]
    return re.search(pattern, text, flags)


# ============================================================================
# PARSING UTILITIES
# ============================================================================

def parse_amount(amount_value) -> float:
    """
    Parse amount from various formats:
    - ($41.03) -> -41.03 (accounting notation for negative)
    - -$41.03 -> -41.03
    - $41.03 -> 41.03
    - -41.03 -> -41.03
    - 41.03 -> 41.03
    """
    if amount_value is None:
        return 0.0

    amount_str = str(amount_value).strip()

    # Check for accounting notation: ($XX.XX) means negative
    is_negative = False
    if amount_str.startswith('(') and amount_str.endswith(')'):
        is_negative = True
        amount_str = amount_str[1:-1]
    elif amount_str.startswith('-'):
        is_negative = True
        amount_str = amount_str[1:]

    # Remove currency symbols and commas
    amount_str = amount_str.replace('$', '').replace(',', '').strip()

    if not amount_str:
        return 0.0

    try:
        amount = float(amount_str)
        return -amount if is_negative else amount
    except ValueError:
        logger.warning(f"Could not parse amount: {amount_value}")
        return 0.0


def parse_date(date_str: str) -> Optional[datetime]:
    """
    Parse date string to datetime object.

    Returns datetime normalized to midnight (00:00:00) for consistent
    date comparisons. This avoids timezone-related off-by-one errors.
    """
    if not date_str:
        return None

    formats = [
        '%Y-%m-%d',
        '%m/%d/%Y',
        '%m-%d-%Y',
        '%Y/%m/%d',
        '%d/%m/%Y',
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(str(date_str).strip(), fmt)
            # Normalize to midnight for consistent comparisons
            return dt.replace(hour=0, minute=0, second=0, microsecond=0)
        except ValueError:
            continue

    logger.warning(f"Could not parse date: {date_str}")
    return None

# ============================================================================
# EMAIL BODY EXTRACTION
# ============================================================================

def get_email_body(message: Dict[str, Any]) -> Dict[str, str]:
    """
    Extract email body with improved multipart handling.
    Returns both plain text and HTML for better parsing flexibility.
    """
    try:
        payload = message.get('payload', {})

        html_parts = []
        text_parts = []

        def extract_parts(part, depth=0):
            """Recursively extract all body parts."""
            if depth > 10:
                return

            mime_type = part.get('mimeType', '')

            if 'parts' in part:
                for subpart in part['parts']:
                    extract_parts(subpart, depth + 1)

            body_data = part.get('body', {}).get('data')
            if body_data:
                try:
                    decoded = base64.urlsafe_b64decode(body_data).decode('utf-8', errors='ignore')

                    if mime_type == 'text/html':
                        html_parts.append(decoded)
                    elif mime_type == 'text/plain':
                        text_parts.append(decoded)
                except Exception as e:
                    logger.debug(f"Error decoding body part: {e}")

        extract_parts(payload)

        return {
            'plain': '\n'.join(text_parts) if text_parts else '',
            'html': '\n'.join(html_parts) if html_parts else ''
        }

    except Exception as e:
        logger.error(f"Error extracting email body: {e}")
        return {'plain': '', 'html': ''}


def get_email_headers(message: Dict[str, Any]) -> Dict[str, str]:
    """Extract common email headers."""
    headers = {}
    for header in message.get('payload', {}).get('headers', []):
        name = header.get('name', '').lower()
        if name in ['from', 'to', 'subject', 'date']:
            headers[name] = header.get('value', '')
    return headers

# ============================================================================
# EMAIL PARSING - PLAIN TEXT (PRIMARY)
# ============================================================================

def parse_amazon_plain_text(plain_text: str) -> List[Dict[str, Any]]:
    """
    Parse Amazon order items from the plain text version of the email.

    Amazon's plain text format is very clean:
    * Product Name Here
      Quantity: N
      XX.XX USD

    This is much more reliable than parsing HTML.
    """
    items = []

    if not plain_text:
        return items

    # Limit input length to prevent ReDoS attacks
    if len(plain_text) > MAX_REGEX_INPUT_LENGTH:
        logger.debug(f"Truncating email content for parsing ({len(plain_text)} chars)")
        plain_text = plain_text[:MAX_REGEX_INPUT_LENGTH]

    # Decode quoted-printable encoding if present
    plain_text = plain_text.replace('=\n', '')
    plain_text = plain_text.replace('=20', ' ')

    lines = plain_text.split('\n')
    i = 0

    while i < len(lines):
        line = lines[i].strip()

        # Look for lines starting with "* " which indicate product names
        if line.startswith('* '):
            product_name = line[2:].strip()
            quantity = 1
            price = None

            # Look at the next few lines for Quantity and Price
            for j in range(1, 5):
                if i + j >= len(lines):
                    break

                next_line = lines[i + j].strip()

                # Check for quantity
                qty_match = re.search(r'Quantity:\s*(\d+)', next_line, re.IGNORECASE)
                if qty_match:
                    quantity = int(qty_match.group(1))

                # Check for price in USD format (e.g., "9.3 USD" or "14.4 USD")
                price_match = re.search(r'^(\d+(?:\.\d{1,2})?)\s*USD', next_line, re.IGNORECASE)
                if price_match:
                    try:
                        price = float(price_match.group(1))
                    except ValueError:
                        continue

                # Also check for price with $ sign
                price_match2 = re.search(r'\$(\d+(?:\.\d{1,2})?)', next_line)
                if price_match2 and price is None:
                    try:
                        price = float(price_match2.group(1))
                    except ValueError:
                        continue

                if price is not None:
                    break

            if product_name and len(product_name) >= 3 and price is not None and price > 0:
                product_name = re.sub(r'\s+', ' ', product_name).strip()

                name_lower = product_name.lower()
                if not any(skip in name_lower for skip in SKIP_KEYWORDS):
                    items.append({
                        'name': product_name[:200],
                        'price': price,
                        'quantity': quantity
                    })
                    logger.debug(f"Plain text parser found: {product_name[:50]}... qty={quantity} ${price:.2f}")

        i += 1

    return items

# ============================================================================
# EMAIL PARSING - HTML (FALLBACK)
# ============================================================================

def parse_amazon_html(email_html: str) -> List[Dict[str, Any]]:
    """
    Parse Amazon order items from HTML email using BeautifulSoup.
    Fallback when plain text parsing fails.
    """
    items = []

    if not email_html:
        return items

    try:
        soup = BeautifulSoup(email_html, 'html.parser')

        # Strategy 1: Find table rows containing prices
        for table in soup.find_all('table'):
            rows = table.find_all('tr')
            for row in rows:
                item = _extract_item_from_bs_row(row)
                if item:
                    items.append(item)

        # Strategy 2: Find divs that might contain items
        for div in soup.find_all(['div', 'td', 'span']):
            text = div.get_text(strip=True)
            if '$' in text and 15 < len(text) < 500:
                item = _extract_item_from_text_block(text)
                if item:
                    items.append(item)

        # Deduplicate
        items = _deduplicate_items(items)

    except Exception as e:
        logger.error(f"Error parsing HTML email: {e}")

    return items


def _extract_item_from_bs_row(row) -> Optional[Dict[str, Any]]:
    """Extract item from a BeautifulSoup table row."""
    try:
        cells = row.find_all(['td', 'th'])
        if not cells:
            return None

        price = None
        description = None

        for cell in cells:
            cell_text = cell.get_text(strip=True)

            price_match = re.search(r'\$(\d+(?:,\d{3})*\.?\d{0,2})', cell_text)
            if price_match and price is None:
                try:
                    p = float(price_match.group(1).replace(',', ''))
                    if 0.01 <= p <= 10000:
                        price = p
                except ValueError:
                    pass

            if len(cell_text) > 15 and not re.match(r'^[\$\d\.,\s]+$', cell_text):
                text_lower = cell_text.lower()
                if not any(skip in text_lower for skip in SKIP_KEYWORDS):
                    description = cell_text

        if price and description:
            return {
                'name': description[:200],
                'price': price,
                'quantity': 1
            }

    except Exception as e:
        logger.debug(f"Error extracting item from table row: {e}")

    return None


def _extract_item_from_text_block(text: str) -> Optional[Dict[str, Any]]:
    """Extract item from a text block containing price and description."""
    try:
        text = re.sub(r'\s+', ' ', text).strip()

        text_lower = text.lower()
        if any(skip in text_lower for skip in SKIP_KEYWORDS):
            return None

        price_match = re.search(r'\$(\d+(?:,\d{3})*\.?\d{0,2})', text)
        if not price_match:
            return None

        try:
            price = float(price_match.group(1).replace(',', ''))
            if price < 0.01 or price > 10000:
                return None
        except ValueError:
            return None

        description = text[:price_match.start()].strip()
        description = re.sub(r'^[\d\s\-\.x]+', '', description)
        description = re.sub(r'\s+', ' ', description).strip()

        if len(description) >= 5:
            return {
                'name': description[:200],
                'price': price,
                'quantity': 1
            }

    except Exception as e:
        logger.debug(f"Error extracting item from text block: {e}")

    return None


def _deduplicate_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Deduplicate items based on name similarity and price."""
    if not items:
        return []

    seen = set()
    unique_items = []

    for item in items:
        name_key = item['name'][:50].lower().strip()
        price_key = round(item['price'], 2)

        if len(name_key) < 5:
            continue

        if re.match(r'^[\d\s\.,\-]+$', name_key):
            continue

        # Skip promotional content (items starting with percentage or discount patterns)
        if re.match(r'^-?\d+%', item['name']):
            continue

        key = (name_key, price_key)
        if key not in seen:
            seen.add(key)
            unique_items.append(item)

    return unique_items

# ============================================================================
# COMBINED EMAIL PARSING
# ============================================================================

def parse_amazon_email(email_body: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Parse Amazon order confirmation email to extract order items.
    Tries plain text first (most reliable), then falls back to HTML.

    Args:
        email_body: Dict with 'plain' and 'html' keys containing email content

    Returns:
        List of item dicts with 'name', 'price', 'quantity' keys
    """
    plain_text = email_body.get('plain', '') if isinstance(email_body, dict) else ''
    email_html = email_body.get('html', '') if isinstance(email_body, dict) else email_body

    if not plain_text and not email_html:
        logger.warning("Empty email body (both plain text and HTML)")
        return []

    # STRATEGY 1 (BEST): Parse from plain text
    if plain_text:
        plain_text_items = parse_amazon_plain_text(plain_text)
        if plain_text_items:
            logger.info(f"Plain text strategy found {len(plain_text_items)} items")
            return plain_text_items

    # STRATEGY 2 (FALLBACK): Parse from HTML
    if email_html:
        html_items = parse_amazon_html(email_html)
        if html_items:
            logger.info(f"HTML strategy found {len(html_items)} items")
            return html_items

    logger.warning("No items extracted from email")
    return []

# ============================================================================
# AMOUNT EXTRACTION FROM EMAIL
# ============================================================================

def extract_order_total_from_email(email_body: Dict[str, str]) -> Optional[float]:
    """Extract the order total from email content."""
    plain_text = email_body.get('plain', '')
    email_html = email_body.get('html', '')

    # Try plain text first
    if plain_text:
        patterns = [
            r'Order\s+Total[:\s]*\$(\d+\.\d{2})',
            r'Total[:\s]*\$(\d+\.\d{2})',
            r'Grand\s+Total[:\s]*\$(\d+\.\d{2})',
            r'Amount\s+Charged[:\s]*\$(\d+\.\d{2})',
        ]

        for pattern in patterns:
            match = re.search(pattern, plain_text, re.IGNORECASE)
            if match:
                try:
                    return float(match.group(1))
                except ValueError:
                    continue

    # Try HTML
    if email_html:
        soup = BeautifulSoup(email_html, 'html.parser')
        text = soup.get_text()

        patterns = [
            r'Order\s+Total[:\s]*\$(\d+\.\d{2})',
            r'Total[:\s]*\$(\d+\.\d{2})',
            r'Grand\s+Total[:\s]*\$(\d+\.\d{2})',
            r'Amount\s+Charged[:\s]*\$(\d+\.\d{2})',
            r'Refund\s+Amount[:\s]*\$(\d+\.\d{2})',
            r'Refund\s+Total[:\s]*\$(\d+\.\d{2})',
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    return float(match.group(1))
                except ValueError:
                    continue

    return None

# ============================================================================
# SHIPMENT TOTAL EXTRACTION
# ============================================================================

def extract_shipment_total(email_body: Dict[str, str]) -> Optional[float]:
    """
    Extract the shipment total (actual bank charge) from email content.

    This is the key field for matching transactions. Different email types
    use different formats:
    - auto-confirm@amazon.com: "Grand Total:\nX.XX USD"
    - shipment-tracking@amazon.com: "Total\nX.XX USD" or "Shipment Total: $X.XX"

    Returns:
        The shipment total as a float, or None if not found.
    """
    plain_text = email_body.get('plain', '')

    if not plain_text:
        return None

    # Limit input length to prevent ReDoS attacks
    if len(plain_text) > MAX_REGEX_INPUT_LENGTH:
        logger.debug(f"Truncating email content for regex ({len(plain_text)} chars)")
        plain_text = plain_text[:MAX_REGEX_INPUT_LENGTH]

    # Normalize line endings to \n for consistent matching
    plain_text = plain_text.replace('\r\n', '\n').replace('\r', '\n')

    # Pattern 1: "Grand Total:\nX.XX USD" (auto-confirm emails)
    match = re.search(r'Grand\s+Total[:\s]*\n?\s*([\d,]+\.?\d*)\s*USD', plain_text, re.IGNORECASE)
    if match:
        try:
            return float(match.group(1).replace(',', ''))
        except ValueError:
            pass

    # Pattern 2: "Shipment Total: $X.XX" (shipment-tracking combined emails)
    match = re.search(r'Shipment\s+Total[:\s]*\$?([\d,]+\.?\d*)', plain_text, re.IGNORECASE)
    if match:
        try:
            return float(match.group(1).replace(',', ''))
        except ValueError:
            pass

    # Pattern 3: "Total\nX.XX USD" (older order emails and shipment-tracking)
    # Be careful to not match "Total Before Tax" or "Item Subtotal"
    match = re.search(r'(?<![a-zA-Z])\nTotal\s*\n\s*([\d,]+\.?\d*)\s*USD', plain_text)
    if match:
        try:
            return float(match.group(1).replace(',', ''))
        except ValueError:
            pass

    # Pattern 4: Just "Total" followed by USD amount on same line
    match = re.search(r'\bTotal[:\s]+([\d,]+\.?\d*)\s*USD', plain_text)
    if match:
        try:
            return float(match.group(1).replace(',', ''))
        except ValueError:
            pass

    # Pattern 5: "Order Total: $X.XX" (older order confirmation format)
    match = re.search(r'Order\s+Total[:\s]*\$\s*([\d,]+\.?\d*)', plain_text, re.IGNORECASE)
    if match:
        try:
            return float(match.group(1).replace(',', ''))
        except ValueError:
            pass

    # Pattern 6: "Grand Total: $X.XX" (with dollar sign instead of USD)
    match = re.search(r'Grand\s+Total[:\s]*\$\s*([\d,]+\.?\d*)', plain_text, re.IGNORECASE)
    if match:
        try:
            return float(match.group(1).replace(',', ''))
        except ValueError:
            pass

    # Pattern 7: "refund of $X.XX" (payments-messages refund notifications)
    match = re.search(r'refund\s+of\s+\$\s*([\d,]+\.?\d*)', plain_text, re.IGNORECASE)
    if match:
        try:
            return float(match.group(1).replace(',', ''))
        except ValueError:
            pass

    return None


# ============================================================================
# ORDER NUMBER EXTRACTION
# ============================================================================

def extract_order_number(email_body: Dict[str, str]) -> Optional[str]:
    """Extract Amazon order number from email."""
    plain_text = email_body.get('plain', '')
    email_html = email_body.get('html', '')

    # Amazon order number pattern: XXX-XXXXXXX-XXXXXXX
    order_pattern = r'\b(\d{3}-\d{7}-\d{7})\b'

    if plain_text:
        match = re.search(order_pattern, plain_text)
        if match:
            return match.group(1)

    if email_html:
        soup = BeautifulSoup(email_html, 'html.parser')
        text = soup.get_text()
        match = re.search(order_pattern, text)
        if match:
            return match.group(1)

    return None

# ============================================================================
# SHEETS HELPERS
# ============================================================================

# Google Sheets limits
SHEETS_CELL_LIMIT = 10_000_000  # 10 million cells max per spreadsheet
SHEETS_CELL_WARNING_THRESHOLD = 0.8  # Warn at 80% capacity

# Gmail watch settings
GMAIL_WATCH_STATE_FILE = '.gmail_watch_state.json'
GMAIL_WATCH_WARNING_DAYS = 2  # Warn if watch expires within N days


def check_sheet_cell_usage(sheet_id: str, creds=None) -> Dict[str, Any]:
    """
    Check cell usage for a Google Sheet and warn if approaching limit.

    Args:
        sheet_id: The Google Sheet ID
        creds: Optional credentials

    Returns:
        Dict with 'total_cells', 'percentage', 'warning' keys
    """
    service = get_sheets_service(creds)

    try:
        # Get spreadsheet metadata
        spreadsheet = service.spreadsheets().get(
            spreadsheetId=sheet_id,
            fields='sheets.properties'
        ).execute()

        total_cells = 0
        for sheet in spreadsheet.get('sheets', []):
            props = sheet.get('properties', {})
            grid = props.get('gridProperties', {})
            rows = grid.get('rowCount', 0)
            cols = grid.get('columnCount', 0)
            total_cells += rows * cols

        percentage = total_cells / SHEETS_CELL_LIMIT
        warning = percentage >= SHEETS_CELL_WARNING_THRESHOLD

        result = {
            'total_cells': total_cells,
            'percentage': percentage,
            'warning': warning,
            'limit': SHEETS_CELL_LIMIT
        }

        if warning:
            logger.warning(
                f"Sheet cell usage at {percentage:.1%} ({total_cells:,} / {SHEETS_CELL_LIMIT:,}). "
                f"Consider archiving old data."
            )
        else:
            logger.info(f"Sheet cell usage: {percentage:.1%} ({total_cells:,} cells)")

        return result

    except HttpError as e:
        logger.error(f"Error checking cell usage: {e}")
        return {'total_cells': 0, 'percentage': 0, 'warning': False, 'error': str(e)}


def column_index_to_letter(col_index: int) -> str:
    """Convert 0-based column index to Google Sheets column letter."""
    result = ''
    col_index += 1
    while col_index > 0:
        col_index -= 1
        result = chr(65 + (col_index % 26)) + result
        col_index //= 26
    return result


def read_sheet_data(sheet_id: str, range_name: str, creds=None) -> List[List[Any]]:
    """Read data from a Google Sheet."""
    service = get_sheets_service(creds)
    result = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=range_name
    ).execute()
    return result.get('values', [])


def _retry_on_error(func, max_retries: int = 3, base_delay: float = 2.0):
    """
    Retry a function with exponential backoff on transient errors.

    Args:
        func: Callable to execute
        max_retries: Maximum number of retry attempts
        base_delay: Base delay in seconds (doubles each retry)

    Returns:
        Result of the function call

    Raises:
        Last exception if all retries fail
    """
    last_exception = None

    for attempt in range(max_retries):
        try:
            return func()
        except HttpError as e:
            last_exception = e
            # Retry on rate limits (429) and server errors (500, 503)
            if e.resp.status in (429, 500, 503):
                delay = base_delay * (2 ** attempt)
                logger.warning(f"Sheets API error {e.resp.status} (attempt {attempt + 1}/{max_retries}), retrying in {delay}s...")
                time.sleep(delay)
            else:
                raise  # Don't retry client errors (400, 403, 404)
        except Exception as e:
            # Network errors, timeouts, etc.
            last_exception = e
            delay = base_delay * (2 ** attempt)
            logger.warning(f"Sheets API error (attempt {attempt + 1}/{max_retries}): {e}, retrying in {delay}s...")
            time.sleep(delay)

    logger.error(f"All {max_retries} retries failed")
    raise last_exception


def append_to_sheet(sheet_id: str, range_name: str, values: List[List[Any]], creds=None):
    """Append rows to a Google Sheet with retry logic."""
    service = get_sheets_service(creds)
    body = {'values': values}

    def do_append():
        return service.spreadsheets().values().append(
            spreadsheetId=sheet_id,
            range=range_name,
            valueInputOption='RAW',
            insertDataOption='INSERT_ROWS',
            body=body
        ).execute()

    return _retry_on_error(do_append)


def write_to_sheet(sheet_id: str, range_name: str, values: List[List[Any]], creds=None):
    """Write/overwrite data in a Google Sheet with retry logic."""
    service = get_sheets_service(creds)
    body = {'values': values}

    def do_write():
        return service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()

    return _retry_on_error(do_write)
