#!/usr/bin/env python3
"""
Email Ingestion Cloud Function

Triggered by Gmail push notifications via Pub/Sub.
Parses Amazon order/return emails and writes to Parsed Orders Google Sheet.
"""

import base64
import json
import logging
import os
from datetime import datetime
from typing import Optional, Dict, Any, List

import functions_framework
from google.auth import default
from google.cloud import pubsub_v1
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from bs4 import BeautifulSoup
import re

# ============================================================================
# CONFIGURATION
# ============================================================================

# Google Cloud Project (set via environment variable or detect automatically)
PROJECT_ID = os.environ.get('GCP_PROJECT', os.environ.get('GOOGLE_CLOUD_PROJECT'))

# Parsed Orders Sheet Configuration
PARSED_ORDERS_SHEET_ID = os.environ.get('PARSED_ORDERS_SHEET_ID', 'YOUR_PARSED_ORDERS_SHEET_ID')
PARSED_ORDERS_SHEET_NAME = os.environ.get('PARSED_ORDERS_SHEET_NAME', 'Parsed Orders')

# Gmail user to impersonate (for domain-wide delegation) or 'me' for service account
GMAIL_USER = os.environ.get('GMAIL_USER', 'me')

# Amazon email senders
AMAZON_ORDER_EMAIL_FROM = 'auto-confirm@amazon.com'
AMAZON_RETURN_EMAIL_FROM = 'return@amazon.com'

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Skip keywords for filtering out non-item lines
SKIP_KEYWORDS = [
    'order total', 'subtotal', 'tax', 'shipping', 'grand total',
    'item subtotal', 'total before tax', 'estimated tax', 'free shipping',
    'delivery', 'gift card', 'promotional', 'discount', 'savings',
    'credit card', 'payment method', 'billing', 'order #', 'order number',
    'arriving', 'delivered', 'track package', 'view order'
]

# ============================================================================
# GOOGLE API SERVICES
# ============================================================================

_gmail_service = None
_sheets_service = None


def get_credentials():
    """Get default credentials for Cloud Functions."""
    creds, _ = default(scopes=[
        'https://www.googleapis.com/auth/gmail.readonly',
        'https://www.googleapis.com/auth/spreadsheets'
    ])
    return creds


def get_gmail_service():
    """Get Gmail API service (cached)."""
    global _gmail_service
    if _gmail_service is None:
        creds = get_credentials()
        _gmail_service = build('gmail', 'v1', credentials=creds)
    return _gmail_service


def get_sheets_service():
    """Get Sheets API service (cached)."""
    global _sheets_service
    if _sheets_service is None:
        creds = get_credentials()
        _sheets_service = build('sheets', 'v4', credentials=creds)
    return _sheets_service

# ============================================================================
# CLOUD FUNCTION ENTRY POINT
# ============================================================================

@functions_framework.cloud_event
def process_gmail_notification(cloud_event):
    """
    Cloud Function entry point - triggered by Pub/Sub message from Gmail.

    The Pub/Sub message contains:
    - emailAddress: The user's email address
    - historyId: The Gmail history ID to fetch changes from
    """
    try:
        # Decode Pub/Sub message
        pubsub_message = base64.b64decode(cloud_event.data["message"]["data"]).decode()
        message_data = json.loads(pubsub_message)

        email_address = message_data.get('emailAddress')
        history_id = message_data.get('historyId')

        logger.info(f"Received notification for {email_address}, historyId: {history_id}")

        if not history_id:
            logger.warning("No historyId in notification, skipping")
            return

        # Fetch emails since the history ID
        emails = fetch_emails_by_history(history_id)

        if not emails:
            logger.info("No new emails to process")
            return

        # Process each email
        processed_count = 0
        for email in emails:
            result = process_single_email(email)
            if result:
                processed_count += 1

        logger.info(f"Processed {processed_count} Amazon emails out of {len(emails)} total")

    except Exception as e:
        logger.error(f"Error processing notification: {e}", exc_info=True)
        raise


def fetch_emails_by_history(history_id: str) -> List[Dict[str, Any]]:
    """
    Fetch emails that triggered the notification using history ID.
    Returns list of full email messages.
    """
    gmail = get_gmail_service()
    emails = []

    try:
        # Get history since the given ID
        history = gmail.users().history().list(
            userId=GMAIL_USER,
            startHistoryId=history_id,
            historyTypes=['messageAdded']
        ).execute()

        # Extract message IDs from history
        message_ids = set()
        for record in history.get('history', []):
            for msg_added in record.get('messagesAdded', []):
                message_ids.add(msg_added['message']['id'])

        logger.info(f"Found {len(message_ids)} new messages in history")

        # Fetch full message content for each
        for msg_id in message_ids:
            try:
                message = gmail.users().messages().get(
                    userId=GMAIL_USER,
                    id=msg_id,
                    format='full'
                ).execute()
                emails.append(message)
            except HttpError as e:
                if e.resp.status == 404:
                    logger.debug(f"Message {msg_id} not found (may have been deleted)")
                else:
                    logger.warning(f"Error fetching message {msg_id}: {e}")

    except HttpError as e:
        if e.resp.status == 404:
            logger.warning(f"History ID {history_id} not found or expired")
        else:
            logger.error(f"Error fetching history: {e}")

    return emails

# ============================================================================
# EMAIL PROCESSING
# ============================================================================

def get_email_subject(message: Dict[str, Any]) -> str:
    """Extract email subject from headers."""
    headers = message.get('payload', {}).get('headers', [])
    for header in headers:
        if header.get('name', '').lower() == 'subject':
            return header.get('value', '')
    return ''


def process_single_email(message: Dict[str, Any]) -> bool:
    """
    Process a single email message.
    Returns True if it was an Amazon email that was processed.
    """
    email_id = message.get('id')

    # Get email type (order, return, or None)
    email_type = get_email_type(message)

    if email_type is None:
        logger.debug(f"Email {email_id} is not from Amazon, skipping")
        return False

    # For return emails, only process those with 'refund' in subject
    # This filters out return request and drop-off confirmation emails
    if email_type == 'return':
        subject = get_email_subject(message)
        if 'refund' not in subject.lower():
            logger.debug(f"Email {email_id}: Skipping return email without 'refund' in subject")
            return False

    # Check if already processed (dedup by email_id)
    if is_email_processed(email_id):
        logger.info(f"Email {email_id} already processed, skipping")
        return False

    # Extract email body
    email_body = get_email_body(message)

    if not email_body.get('plain') and not email_body.get('html'):
        logger.warning(f"Email {email_id} has no body content")
        return False

    # Get email date
    email_date = get_email_date(message)

    # Parse based on email type
    if email_type == 'order':
        order_data = parse_order_email(email_body, email_id, email_date)
    else:  # return
        order_data = parse_return_email(email_body, email_id, email_date)

    if order_data:
        order_data['email_type'] = email_type
        write_to_parsed_orders_sheet(order_data)
        logger.info(f"Processed {email_type} email {email_id}: order {order_data.get('order_number')}")
        return True
    else:
        # Write partial record even if parsing failed
        write_failed_parse_record(email_id, email_date, email_type)
        logger.warning(f"Failed to parse {email_type} email {email_id}")
        return False


def get_email_type(message: Dict[str, Any]) -> Optional[str]:
    """
    Determine email type based on sender.
    Returns 'order', 'return', or None.
    """
    headers = message.get('payload', {}).get('headers', [])

    from_header = None
    for header in headers:
        if header.get('name', '').lower() == 'from':
            from_header = header.get('value', '').lower()
            break

    if not from_header:
        return None

    if AMAZON_ORDER_EMAIL_FROM.lower() in from_header:
        return 'order'
    elif AMAZON_RETURN_EMAIL_FROM.lower() in from_header:
        return 'return'

    return None


def get_email_date(message: Dict[str, Any]) -> str:
    """Extract email date from headers."""
    headers = message.get('payload', {}).get('headers', [])

    for header in headers:
        if header.get('name', '').lower() == 'date':
            # Parse and reformat the date
            date_str = header.get('value', '')
            try:
                # Gmail date format: "Mon, 2 Jan 2006 15:04:05 -0700"
                from email.utils import parsedate_to_datetime
                dt = parsedate_to_datetime(date_str)
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                return date_str

    # Fallback to internal date
    internal_date = message.get('internalDate')
    if internal_date:
        dt = datetime.fromtimestamp(int(internal_date) / 1000)
        return dt.strftime('%Y-%m-%d %H:%M:%S')

    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def get_email_body(message: Dict[str, Any]) -> Dict[str, str]:
    """Extract email body (plain text and HTML)."""
    payload = message.get('payload', {})

    html_parts = []
    text_parts = []

    def extract_parts(part, depth=0):
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

# ============================================================================
# EMAIL PARSING
# ============================================================================

def parse_order_email(email_body: Dict[str, str], email_id: str, email_date: str) -> Optional[Dict[str, Any]]:
    """Parse Amazon order confirmation email."""
    plain_text = email_body.get('plain', '')
    html_content = email_body.get('html', '')

    # Extract order number
    order_number = extract_order_number(plain_text or html_content)

    # Extract shipment total (the actual bank charge amount)
    shipment_total = extract_shipment_total(email_body)

    # Extract items
    items = []

    # Try plain text first (more reliable)
    if plain_text:
        items = parse_items_from_plain_text(plain_text)

    # Fallback to HTML
    if not items and html_content:
        items = parse_items_from_html(html_content)

    # Determine parse status
    if items and shipment_total:
        parse_status = 'success'
    elif items or shipment_total:
        parse_status = 'partial'
    else:
        parse_status = 'failed'

    return {
        'email_id': email_id,
        'email_date': email_date,
        'order_number': order_number or '',
        'shipment_total': shipment_total,
        'items': items,
        'parse_status': parse_status,
        'processed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }


def parse_return_email(email_body: Dict[str, str], email_id: str, email_date: str) -> Optional[Dict[str, Any]]:
    """Parse Amazon return/refund email."""
    plain_text = email_body.get('plain', '')
    html_content = email_body.get('html', '')

    content = plain_text or html_content

    # Extract order number
    order_number = extract_order_number(content)

    # Extract refund amount
    refund_amount = extract_refund_amount(content)

    # For returns, shipment_total is the refund amount
    shipment_total = refund_amount or extract_shipment_total(email_body)

    # Extract returned items - return emails have different structure
    items = parse_return_items_from_html(html_content)

    # For returns, put refund amount in item_price
    if refund_amount and items:
        # If single item, use full refund amount
        if len(items) == 1:
            items[0]['price'] = refund_amount
        else:
            # Multiple items - put total in first item
            items[0]['price'] = refund_amount
    elif refund_amount and not items:
        # No items parsed but we have refund - create placeholder item
        items = [{'name': 'Refund', 'price': refund_amount, 'quantity': 1}]

    # Determine parse status
    if items and shipment_total:
        parse_status = 'success'
    elif items or shipment_total:
        parse_status = 'partial'
    else:
        parse_status = 'failed'

    return {
        'email_id': email_id,
        'email_date': email_date,
        'order_number': order_number or '',
        'shipment_total': shipment_total,
        'items': items,
        'parse_status': parse_status,
        'processed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }


def extract_order_number(content: str) -> Optional[str]:
    """Extract Amazon order number from email content."""
    # Amazon order number pattern: XXX-XXXXXXX-XXXXXXX
    match = re.search(r'\b(\d{3}-\d{7}-\d{7})\b', content)
    return match.group(1) if match else None


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


def extract_shipment_total(email_body: Dict[str, str]) -> Optional[float]:
    """
    Extract the shipment total (actual bank charge) from email content.

    This is the key field for matching transactions. Different email types
    use different formats:
    - auto-confirm@amazon.com: "Grand Total:\nX.XX USD"
    - shipment-tracking@amazon.com: "Total\nX.XX USD" or "Shipment Total: $X.XX"
    """
    plain_text = email_body.get('plain', '')

    if not plain_text:
        return None

    # Normalize line endings
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

    # Pattern 3: "Total\nX.XX USD" (older order emails)
    match = re.search(r'(?<![a-zA-Z])\nTotal\s*\n\s*([\d,]+\.?\d*)\s*USD', plain_text)
    if match:
        try:
            return float(match.group(1).replace(',', ''))
        except ValueError:
            pass

    # Pattern 4: "Order Total: $X.XX"
    match = re.search(r'Order\s+Total[:\s]*\$\s*([\d,]+\.?\d*)', plain_text, re.IGNORECASE)
    if match:
        try:
            return float(match.group(1).replace(',', ''))
        except ValueError:
            pass

    # Pattern 5: "refund of $X.XX" (for refund emails)
    match = re.search(r'refund\s+of\s+\$\s*([\d,]+\.?\d*)', plain_text, re.IGNORECASE)
    if match:
        try:
            return float(match.group(1).replace(',', ''))
        except ValueError:
            pass

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


def parse_items_from_plain_text(plain_text: str) -> List[Dict[str, Any]]:
    """Parse items from plain text email."""
    items = []

    # Decode quoted-printable encoding
    plain_text = plain_text.replace('=\n', '')
    plain_text = plain_text.replace('=20', ' ')

    lines = plain_text.split('\n')
    i = 0

    while i < len(lines):
        line = lines[i].strip()

        # Look for lines starting with "* "
        if line.startswith('* '):
            product_name = line[2:].strip()
            quantity = 1
            price = None

            for j in range(1, 5):
                if i + j >= len(lines):
                    break

                next_line = lines[i + j].strip()

                qty_match = re.search(r'Quantity:\s*(\d+)', next_line, re.IGNORECASE)
                if qty_match:
                    quantity = int(qty_match.group(1))

                price_match = re.search(r'^(\d+(?:\.\d{1,2})?)\s*USD', next_line, re.IGNORECASE)
                if price_match:
                    try:
                        price = float(price_match.group(1))
                    except ValueError:
                        continue

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

        i += 1

    return items


def parse_items_from_html(html_content: str) -> List[Dict[str, Any]]:
    """Parse items from HTML email using BeautifulSoup."""
    items = []

    try:
        soup = BeautifulSoup(html_content, 'html.parser')

        # Find table rows containing prices
        for table in soup.find_all('table'):
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if not cells:
                    continue

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
                    items.append({
                        'name': description[:200],
                        'price': price,
                        'quantity': 1
                    })

        # Deduplicate
        seen = set()
        unique_items = []
        for item in items:
            key = (item['name'][:50].lower(), round(item['price'], 2))
            if key not in seen:
                seen.add(key)
                unique_items.append(item)
        items = unique_items

    except Exception as e:
        logger.error(f"Error parsing HTML: {e}")

    return items

# ============================================================================
# GOOGLE SHEETS OPERATIONS
# ============================================================================

def is_email_processed(email_id: str) -> bool:
    """
    Check if email has already been processed (dedup check).

    Note: This check is not atomic. For race condition protection,
    we also run cleanup_duplicate_emails() after writes.
    """
    sheets = get_sheets_service()

    try:
        # Read email_id column (column A)
        result = sheets.spreadsheets().values().get(
            spreadsheetId=PARSED_ORDERS_SHEET_ID,
            range=f'{PARSED_ORDERS_SHEET_NAME}!A:A'
        ).execute()

        values = result.get('values', [])
        existing_ids = {row[0] for row in values if row}

        return email_id in existing_ids

    except HttpError as e:
        if e.resp.status == 404:
            logger.info("Parsed Orders sheet not found, will create")
            return False
        raise


def cleanup_duplicate_emails(email_id: str):
    """
    Remove duplicate rows for an email_id, keeping only the first occurrence.
    Called after writes to handle race conditions between concurrent invocations.
    """
    sheets = get_sheets_service()

    try:
        # Get all rows with this email_id
        result = sheets.spreadsheets().values().get(
            spreadsheetId=PARSED_ORDERS_SHEET_ID,
            range=f'{PARSED_ORDERS_SHEET_NAME}!A:A'
        ).execute()

        values = result.get('values', [])

        # Find all row indices (1-indexed for Sheets) with this email_id
        duplicate_rows = []
        first_found = False
        for i, row in enumerate(values):
            if row and row[0] == email_id:
                if first_found:
                    duplicate_rows.append(i + 1)  # 1-indexed
                else:
                    first_found = True

        if not duplicate_rows:
            return  # No duplicates

        logger.warning(f"Found {len(duplicate_rows)} duplicate rows for email {email_id}, cleaning up")

        # Delete duplicates in reverse order (to preserve row indices)
        # Use batchUpdate with deleteDimension requests
        requests = []
        for row_idx in sorted(duplicate_rows, reverse=True):
            requests.append({
                'deleteDimension': {
                    'range': {
                        'sheetId': get_sheet_id(sheets),
                        'dimension': 'ROWS',
                        'startIndex': row_idx - 1,  # 0-indexed for API
                        'endIndex': row_idx
                    }
                }
            })

        if requests:
            sheets.spreadsheets().batchUpdate(
                spreadsheetId=PARSED_ORDERS_SHEET_ID,
                body={'requests': requests}
            ).execute()
            logger.info(f"Deleted {len(requests)} duplicate rows")

    except HttpError as e:
        logger.error(f"Error cleaning up duplicates for {email_id}: {e}")


def get_sheet_id(sheets_service) -> int:
    """Get the sheet ID for the Parsed Orders tab."""
    try:
        spreadsheet = sheets_service.spreadsheets().get(
            spreadsheetId=PARSED_ORDERS_SHEET_ID
        ).execute()

        for sheet in spreadsheet.get('sheets', []):
            if sheet['properties']['title'] == PARSED_ORDERS_SHEET_NAME:
                return sheet['properties']['sheetId']

        return 0  # Default to first sheet
    except HttpError:
        return 0


MAX_QUANTITY = 100  # Cap quantity to prevent memory issues from malformed data


def write_to_parsed_orders_sheet(order_data: Dict[str, Any]):
    """
    Write parsed order data to Parsed Orders sheet.
    Expands items with qty > 1 into separate rows with "1ofN_" prefix.
    Schema: email_id, email_date, email_type, order_number, shipment_total, item_name, item_price, item_qty, parse_status, processed_at
    """
    sheets = get_sheets_service()

    # Prepare rows - one per item, expanding quantities
    rows = []
    items = order_data.get('items', [])
    shipment_total = order_data.get('shipment_total')

    if items:
        for item in items:
            item_name = item.get('name', '')
            item_price = item.get('price', 0)
            quantity = min(item.get('quantity', 1), MAX_QUANTITY)  # Cap quantity

            if item.get('quantity', 1) > MAX_QUANTITY:
                logger.warning(f"Quantity {item.get('quantity')} capped to {MAX_QUANTITY} for item: {item_name[:50]}")

            # Expand quantity into separate rows with "1ofN_" prefix
            for i in range(1, quantity + 1):
                if quantity > 1:
                    prefixed_name = f"{i}of{quantity}_{item_name}"
                else:
                    prefixed_name = item_name

                row = [
                    order_data.get('email_id', ''),
                    order_data.get('email_date', ''),
                    order_data.get('email_type', ''),
                    order_data.get('order_number', ''),
                    shipment_total if shipment_total else '',
                    prefixed_name,
                    item_price,
                    1,  # Always 1 since we're expanding
                    order_data.get('parse_status', ''),
                    order_data.get('processed_at', '')
                ]
                rows.append(row)
    else:
        # No items - write single row with order summary
        row = [
            order_data.get('email_id', ''),
            order_data.get('email_date', ''),
            order_data.get('email_type', ''),
            order_data.get('order_number', ''),
            shipment_total if shipment_total else '',
            '',  # item_name
            0,   # item_price
            0,   # item_qty
            order_data.get('parse_status', ''),
            order_data.get('processed_at', '')
        ]
        rows.append(row)

    # Append to sheet (10 columns: A:J)
    body = {'values': rows}
    sheets.spreadsheets().values().append(
        spreadsheetId=PARSED_ORDERS_SHEET_ID,
        range=f'{PARSED_ORDERS_SHEET_NAME}!A:J',
        valueInputOption='RAW',
        insertDataOption='INSERT_ROWS',
        body=body
    ).execute()

    logger.info(f"Wrote {len(rows)} row(s) for order {order_data.get('order_number')}")

    # Clean up any duplicates from race conditions
    cleanup_duplicate_emails(order_data.get('email_id', ''))


def write_failed_parse_record(email_id: str, email_date: str, email_type: str):
    """Write a record for emails that failed to parse."""
    sheets = get_sheets_service()

    row = [
        email_id,
        email_date,
        email_type,
        '',  # order_number
        '',  # shipment_total
        '',  # item_name
        0,   # item_price
        0,   # item_qty
        'failed',
        datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    ]

    body = {'values': [row]}
    sheets.spreadsheets().values().append(
        spreadsheetId=PARSED_ORDERS_SHEET_ID,
        range=f'{PARSED_ORDERS_SHEET_NAME}!A:J',
        valueInputOption='RAW',
        insertDataOption='INSERT_ROWS',
        body=body
    ).execute()


def ensure_sheet_headers():
    """Ensure the Parsed Orders sheet has proper headers."""
    sheets = get_sheets_service()

    headers = [
        'email_id',
        'email_date',
        'email_type',
        'order_number',
        'shipment_total',
        'item_name',
        'item_price',
        'item_qty',
        'parse_status',
        'processed_at'
    ]

    try:
        result = sheets.spreadsheets().values().get(
            spreadsheetId=PARSED_ORDERS_SHEET_ID,
            range=f'{PARSED_ORDERS_SHEET_NAME}!1:1'
        ).execute()

        existing = result.get('values', [[]])
        if existing and existing[0]:
            # Headers exist
            return

    except HttpError:
        pass

    # Write headers
    body = {'values': [headers]}
    sheets.spreadsheets().values().update(
        spreadsheetId=PARSED_ORDERS_SHEET_ID,
        range=f'{PARSED_ORDERS_SHEET_NAME}!A1',
        valueInputOption='RAW',
        body=body
    ).execute()
    logger.info("Created headers in Parsed Orders sheet")

# ============================================================================
# HTTP TRIGGER (Alternative to Pub/Sub)
# ============================================================================

@functions_framework.http
def process_gmail_notification_http(request):
    """
    HTTP endpoint for Gmail push notifications (alternative to Pub/Sub).
    Gmail can push directly to HTTPS endpoints.
    """
    try:
        # Verify the request is from Google
        # In production, validate using the X-Goog-* headers

        data = request.get_json(silent=True)
        if not data:
            return 'No data', 400

        # Extract message data
        message = data.get('message', {})
        pubsub_data = message.get('data', '')

        if pubsub_data:
            decoded = base64.b64decode(pubsub_data).decode()
            message_data = json.loads(decoded)

            history_id = message_data.get('historyId')
            if history_id:
                emails = fetch_emails_by_history(history_id)
                processed = sum(1 for email in emails if process_single_email(email))
                return f'Processed {processed} emails', 200

        return 'OK', 200

    except Exception as e:
        logger.error(f"Error in HTTP handler: {e}", exc_info=True)
        return f'Error: {str(e)}', 500

# ============================================================================
# LOCAL TESTING
# ============================================================================

if __name__ == '__main__':
    # For local testing
    import sys

    logging.basicConfig(level=logging.DEBUG)

    if len(sys.argv) > 1:
        # Test with a specific history ID
        history_id = sys.argv[1]
        emails = fetch_emails_by_history(history_id)
        for email in emails:
            process_single_email(email)
    else:
        print("Usage: python main.py <history_id>")
        print("Or deploy as Cloud Function")
