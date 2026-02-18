# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Amazon Order Parser - An event-driven system for processing Amazon transactions from bank statements, matching them with order confirmation emails, extracting item-level details, and generating categorized transaction data in Google Sheets.

## Architecture

```
+-----------------+     +------------------+     +-----------------+
|  Gmail Push     |---->|  Cloud Function  |---->|  Parsed Orders  |
|  (Pub/Sub)      |     |  (Email Parser)  |     |  Google Sheet   |
+-----------------+     +------------------+     +-----------------+
                                                         |
                                                         v
+-----------------+     +------------------+     +-----------------+
|  Transactions   |---->|  Matcher (Local) |---->|  Categorized    |
|  Google Sheet   |     |                  |     |  Google Sheet   |
+-----------------+     +------------------+     +-----------------+
```

### Component 1: Email Ingestion (Cloud Function)
- Triggered by Gmail push notifications via Pub/Sub
- Parses Amazon order/return emails automatically
- Writes to "Parsed Orders" Google Sheet with deduplication

### Component 2: Transaction Matcher (Local)
- Reads unprocessed bank transactions
- Matches against pre-parsed orders by date/amount
- Calculates confidence scores
- Outputs expanded item-level transactions

## Common Commands

```bash
# Install dependencies
pip install -r requirements.txt

# ===== SETUP =====

# Set up Gmail watch for real-time notifications
python setup_gmail_watch.py --project YOUR_PROJECT --topic amazon-order-emails

# Backfill historical emails to Parsed Orders sheet
python backfill_emails.py                          # All emails
python backfill_emails.py --limit 10               # Test with 10 emails
python backfill_emails.py --start-date 2024-01-01  # From specific date

# Deploy Cloud Function
cd cloud_function && ./deploy.sh

# ===== DAILY USAGE =====

# Run transaction matcher (main workflow)
python transaction_matcher.py

# ===== LEGACY SCRIPTS =====

# Original monolithic approach (deprecated)
python amazon_order_parser.py
```

## File Structure

```
claude_budget/
+-- utils.py                 # Shared utilities (auth, parsing)
+-- transaction_matcher.py   # Local matching script (main workflow)
+-- backfill_emails.py       # One-time historical backfill
+-- setup_gmail_watch.py     # Gmail push notification setup
+-- cloud_function/
|   +-- main.py              # Cloud Function entry point
|   +-- requirements.txt     # Cloud Function dependencies
|   +-- deploy.sh            # GCP deployment script
+-- amazon_order_parser.py   # LEGACY: Original monolithic script
+-- credentials.json         # OAuth client config (from GCP Console)
+-- token.json               # Cached access token (auto-generated)
```

## Key Configuration

### Environment Variables
- `PARSED_ORDERS_SHEET_ID` - Google Sheet ID for parsed orders
- `GCP_PROJECT` - Google Cloud Project ID

### In transaction_matcher.py
- `SOURCE_SHEET_ID` / `OUTPUT_SHEET_ID` - Source and output sheets
- `DATE_MATCHING_WINDOW_DAYS` - Days tolerance for matching (default: 7)
- `AMOUNT_MATCHING_TOLERANCE` - Dollar tolerance (default: $3)
- `CONFIDENCE_THRESHOLD_HIGH` - Above 70 = high confidence
- `CONFIDENCE_THRESHOLD_LOW` - Below 40 = flagged for review

### Parsed Orders Sheet Schema
| Column | Description |
|--------|-------------|
| email_id | Gmail message ID (dedup key) |
| email_date | When email was received |
| email_type | "order" or "return" |
| order_number | Amazon order number (111-xxx-xxx) |
| order_total | Total amount |
| item_name | Individual item name |
| item_price | Individual item price |
| item_qty | Quantity |
| parse_status | "success" / "partial" / "failed" |
| processed_at | Timestamp of parsing |

## Google API Authentication

First run opens browser for OAuth. Credentials stored in:
- `credentials.json` - OAuth client config (from Google Cloud Console)
- `token.json` - Cached access token (auto-generated)

For Cloud Functions, uses default service account credentials.

## Matching Logic

```
For each transaction:
  1. Determine if charge (negative) or credit (positive)
  2. Filter parsed_orders by email_type:
     - Charges -> match against "order" entries
     - Credits -> match against "return" entries
  3. Find candidates within date window (+/-7 days)
  4. Score each candidate:
     - Exact amount: +50 points
     - Close amount (within $1): +30 points
     - Within tolerance ($3): +20 points
     - Date proximity: +20 points minus 3 per day difference
  5. Take highest scoring match above threshold
  6. Flag low confidence / unmatched for review
```

## GCP Setup Requirements

1. **Pub/Sub Topic:**
   ```bash
   gcloud pubsub topics create amazon-order-emails
   gcloud pubsub topics add-iam-policy-binding amazon-order-emails \
     --member="serviceAccount:gmail-api-push@system.gserviceaccount.com" \
     --role="roles/pubsub.publisher"
   ```

2. **APIs to Enable:**
   - Cloud Functions
   - Cloud Pub/Sub
   - Gmail API
   - Google Sheets API

3. **Gmail Watch (expires every 7 days):**
   ```bash
   python setup_gmail_watch.py --project YOUR_PROJECT --topic amazon-order-emails
   ```
