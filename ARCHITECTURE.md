# Budget Categorizer - Architecture Overview

## System Diagram

```
 ┌─────────────────────────────────────────────────────────────────────┐
 │                        DATA SOURCES                                 │
 │                                                                     │
 │  ┌──────────────┐                           ┌────────────────────┐  │
 │  │    Gmail      │ Amazon order/return emails│  Tiller Money      │  │
 │  │    Inbox      │                           │  (Bank Sync)       │  │
 │  └──────┬───────┘                           └─────────┬──────────┘  │
 └─────────┼─────────────────────────────────────────────┼─────────────┘
           │                                             │
           ▼                                             ▼
 ┌───────────────────────┐                 ┌──────────────────────────┐
 │  PIPELINE 1: Emails   │                 │  Source Transactions     │
 │                       │                 │  Google Sheet            │
 │  Real-time:           │                 │  (Date, Desc, Amount,    │
 │   Gmail Push/Pub/Sub  │                 │   Account)               │
 │   → Cloud Function    │                 └─────────┬────────────────┘
 │                       │                           │
 │  Batch:               │                           │
 │   backfill_emails.py  │                           │
 │   (3 modes: incr,     │                           │
 │    full, legacy)      │                           │
 └───────────┬───────────┘                           │
             ▼                                       │
 ┌───────────────────────┐                           │
 │  Parsed Orders        │                           │
 │  Google Sheet         │                           │
 │  (email_id, order_#,  │                           │
 │   item_name, price,   │                           │
 │   qty, type)          │                           │
 └───────────┬───────────┘                           │
             │                                       │
             └──────────────┬────────────────────────┘
                            ▼
              ┌──────────────────────────┐
              │  PIPELINE 2: Matching    │
              │  transaction_matcher.py  │
              │                          │
              │  • Date window: ±30 days │
              │  • Amount tolerance: ±$3 │
              │  • Confidence scoring    │
              │  • Item-level expansion  │
              │    (1 txn → N items)     │
              └────────────┬─────────────┘
                           ▼
              ┌──────────────────────────┐
              │  Processed Transactions  │
              │  Google Sheet            │
              │  (matched + expanded     │
              │   item-level rows)       │
              └────────────┬─────────────┘
                           │
       ┌───────────────────┘
       │         ┌──────────────────────┐
       │         │  Budget Config       │
       │         │  Google Sheet        │
       │         │  • Categories        │
       │         │  • Merchant Rules    │
       │         │  • Keywords          │
       │         │  • Monthly Budgets   │
       │         └─────────┬────────────┘
       │                   │
       ▼                   ▼
 ┌──────────────────────────────────────────────────────────────────┐
 │                  PIPELINE 3: Categorization                      │
 │                  MCP Server (server.py, 25 tools)                │
 │                                                                  │
 │  ┌────────────────────────────────────────────────────────────┐  │
 │  │  Layer 1: Merchant Rules (deterministic, highest priority) │  │
 │  │  "Whole Foods Market" → groceries (100% confidence)        │  │
 │  └──────────────────────────┬─────────────────────────────────┘  │
 │                  unmatched  ▼                                    │
 │  ┌────────────────────────────────────────────────────────────┐  │
 │  │  Layer 2: Ambiguous Check                                  │  │
 │  │  Amazon/Target/Costco/Walmart → SKIP (needs item context)  │  │
 │  └──────────────────────────┬─────────────────────────────────┘  │
 │                  unmatched  ▼                                    │
 │  ┌────────────────────────────────────────────────────────────┐  │
 │  │  Layer 3: Keyword Rules (word-boundary match)              │  │
 │  │  "battery" → electronics  (flagged for review)             │  │
 │  └──────────────────────────┬─────────────────────────────────┘  │
 │                  unmatched  ▼                                    │
 │  ┌────────────────────────────────────────────────────────────┐  │
 │  │  Layer 4: Claude (LLM fallback)                            │  │
 │  │  • Interactive batch (auto_categorize_batch)               │  │
 │  │  • Bulk API mode (bulk_categorize_api, ~$0.04/100 txns)   │  │
 │  │  • Auto-learns merchant rules from patterns (2+ hits)      │  │
 │  └────────────────────────────────────────────────────────────┘  │
 └──────────────────────────────────┬───────────────────────────────┘
                                    ▼
                     ┌──────────────────────────┐
                     │  OUTPUTS                  │
                     │                           │
                     │  • Categorized sheet       │
                     │    (category, source,      │
                     │     confidence, review)    │
                     │                           │
                     │  • HTML Dashboard          │
                     │    ~/claude_budget/        │
                     │    dashboard.html          │
                     │    (budget bars, spending   │
                     │     breakdown, drill-down)  │
                     └──────────────────────────┘
```

## Process Flow (typical run)

| Step | Action | Tool/Script |
|------|--------|-------------|
| 1 | Sync Amazon emails from Gmail | `run_email_backfill` (incremental mode) |
| 2 | Match bank transactions to orders, expand to item-level | `run_transaction_matcher` (production mode) |
| 3 | Apply all deterministic rules in one pass | `batch_apply_all_rules` |
| 4 | Categorize remaining with Claude | `bulk_categorize_api` or `auto_categorize_batch` |
| 5 | Review flagged transactions | `query_transactions(needs_review=true)` |
| 6 | Generate spending report | `get_spending_summary` / `generate_dashboard` |

## Key Numbers

- **4 Google Sheets**: Source Transactions, Parsed Orders, Processed Transactions, Budget Config
- **25 MCP tools** exposed to Claude
- **4-layer categorization**: Merchant Rules → Ambiguous Skip → Keywords → Claude
- **~14,800 parsed orders**, **~6,500 processed rows**, **97% match rate**

## Self-Learning Loop

When Claude categorizes the same merchant pattern 2+ times with the same category, a new merchant rule is automatically created — so that merchant becomes deterministic on future runs.

## Google Sheets

| Sheet | ID | Purpose |
|-------|----|---------|
| Source Transactions | Set via `SOURCE_SHEET_ID` env var | Raw bank data from Tiller |
| Parsed Orders | Set via `PARSED_ORDERS_SHEET_ID` env var | Amazon email data |
| Processed Transactions | Set via `PROCESSED_TRANSACTIONS_SHEET_ID` env var | Matched + categorized output |
| Budget Config | Set via `BUDGET_CONFIG_SHEET_ID` env var | Categories, rules, keywords, budgets |

## Key Files

```
claude_budget/
├── mcp_categorizer/              # MCP server (main orchestrator)
│   ├── server.py                 # 25 MCP tools
│   ├── sheets_client.py          # Google Sheets operations
│   ├── categorizer.py            # Deterministic categorization engine
│   ├── config.py                 # Configuration constants
│   └── dashboard_generator.py    # HTML dashboard
├── bulk_categorizer/             # Large batch processing
│   ├── bulk_categorize.py        # Direct Claude API calls
│   └── api_client.py             # Anthropic API wrapper
├── transaction_matcher.py        # Bank → Parsed Orders matching
├── backfill_emails.py            # Gmail → Parsed Orders sync
├── utils.py                      # Shared utilities (auth, parsing)
├── multi_order_parser.py         # Multi-order email handling
├── setup_gmail_watch.py          # Gmail push notification setup
└── cloud_function/               # Real-time email ingestion
    ├── main.py                   # Cloud Function entry
    └── deploy.sh                 # GCP deployment
```
