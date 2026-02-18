# Budget Categorizer MCP

A Claude-powered budget categorization system that runs as an MCP server in Claude Desktop. It reads your bank transactions from Google Sheets, applies deterministic merchant/keyword rules first, then uses Claude AI as an intelligent fallback for ambiguous transactions.

## Quick Start

```bash
git clone https://github.com/YOUR_USERNAME/claude-budget-categorizer.git
cd claude-budget-categorizer
./setup.sh        # interactive — sets up venv, deps, and Claude Desktop config
# Restart Claude Desktop
```

The setup script walks you through:
1. Python 3.10+ check and virtual environment
2. Dependency installation
3. Google API credentials
4. Google Sheet IDs (config sheet + transactions sheet)
5. Optional Anthropic API key
6. Auto-configures Claude Desktop

## How It Works

```
┌─────────────────────┐      ┌──────────────────────┐
│  Budget Config Sheet │      │  Transactions Sheet   │
│  (Categories, Rules, │◄────►│  (Bank transactions   │
│   Keywords)          │      │   + categorization)   │
└────────┬────────────┘      └──────────┬───────────┘
         │                              │
         └──────────┬───────────────────┘
                    │
            ┌───────▼───────┐
            │  MCP Server   │
            │  (server.py)  │
            └───────┬───────┘
                    │
            ┌───────▼───────┐
            │ Claude Desktop │
            └───────────────┘
```

**Categorization priority:**
1. **Merchant Rules** (deterministic) — e.g. "Whole Foods" → Groceries
2. **Keyword Rules** (deterministic) — e.g. "battery" → Electronics
3. **Claude AI** (intelligent fallback) — handles ambiguous transactions

## Google Sheets Structure

### Budget Config Sheet (3 tabs)

| Tab | Purpose |
|-----|---------|
| `Categories` | Master list of budget categories (id, name, parent, budget) |
| `Merchant Rules` | Pattern → category mappings with confidence scores |
| `Keywords` | Keyword → category hints with priority |

### Processed Transactions Sheet

Your bank transactions with added categorization columns: `claude_category`, `category_source`, `category_confidence`, `needs_review`, etc.

## Environment Variables

Set via Claude Desktop MCP config (the setup script handles this):

| Variable | Required | Description |
|----------|----------|-------------|
| `BUDGET_CONFIG_SHEET_ID` | Yes | Google Sheet ID for categories/rules/keywords |
| `PROCESSED_TRANSACTIONS_SHEET_ID` | Yes | Google Sheet ID for bank transactions |
| `ANTHROPIC_API_KEY` | No | For Claude-powered categorization (can use macOS Keychain instead) |
| `KEYCHAIN_SERVICE_NAME` | No | macOS Keychain entry name (default: `budget-categorizer-api-key`) |
| `LOG_LEVEL` | No | Logging verbosity (default: `INFO`) |

## Sharing with Family

If you share the same Google Sheets (e.g., with a spouse):

1. Clone this repo on their machine
2. Copy `credentials.json` from the original setup (share securely, not via git)
3. Run `./setup.sh` — enter the **same** Sheet IDs
4. They'll complete their own OAuth flow (browser opens on first use)
5. Restart Claude Desktop

Each person gets their own `token.json` (OAuth token) but shares the same sheets.

## File Structure

```
claude_budget/
├── mcp_categorizer/           # MCP server (runs in Claude Desktop)
│   ├── server.py              # Main MCP server
│   ├── config.py              # Configuration (reads env vars)
│   ├── categorizer.py         # Categorization logic
│   ├── sheets_client.py       # Google Sheets API client
│   ├── setup_config_sheet.py  # Creates a new config sheet
│   ├── requirements.txt       # MCP server dependencies
│   ├── USER_GUIDE.md          # Detailed usage guide
│   └── SHARING_GUIDE.md       # Sharing options & team setup
├── bulk_categorizer/          # Standalone bulk categorization script
├── setup.sh                   # Interactive setup script
├── configure_claude_desktop.py # Auto-configures Claude Desktop
├── credentials.json           # Google OAuth client config (DO NOT COMMIT)
├── token.json                 # Cached OAuth token (auto-generated, DO NOT COMMIT)
└── requirements.txt           # Root project dependencies
```

## Troubleshooting

**MCP not loading in Claude Desktop:**
- Restart Claude Desktop after running setup
- Check config: `cat ~/Library/Application\ Support/Claude/claude_desktop_config.json`
- Verify venv exists: `ls -la venv/bin/python`

**"Missing required environment variables":**
- Re-run `./setup.sh` to set sheet IDs in Claude Desktop config

**Google auth errors:**
- Delete `token.json` and restart Claude Desktop (will re-authorize on next use)
- Ensure `credentials.json` is a valid OAuth 2.0 Desktop client

**"No uncategorized transactions found":**
- Ensure your Processed Transactions sheet has data
- Check that the `claude_category` column isn't already filled

## Optional: Bulk Categorization

For processing large backlogs outside of Claude Desktop, use the standalone bulk categorizer:

```bash
source venv/bin/activate
python bulk_categorizer/bulk_categorize.py --batch-size 50
```

Requires an Anthropic API key (via env var or macOS Keychain).

## License

MIT
