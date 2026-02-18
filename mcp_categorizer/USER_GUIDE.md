# Budget Categorizer User Guide

A Claude-powered transaction categorization system that uses deterministic rules first, with intelligent AI fallback for ambiguous transactions.

---

## Table of Contents

1. [Overview](#overview)
2. [Google Sheets Structure](#google-sheets-structure)
3. [Workflow: Start to Finish](#workflow-start-to-finish)
4. [Managing Categories](#managing-categories)
5. [Managing Merchant Rules](#managing-merchant-rules)
6. [Managing Keywords](#managing-keywords)
7. [Using Claude to Categorize](#using-claude-to-categorize)
8. [Reviewing Flagged Transactions](#reviewing-flagged-transactions)
9. [Updating Google API Credentials](#updating-google-api-credentials)
10. [Troubleshooting](#troubleshooting)

---

## Overview

### How It Works

The categorizer uses a **layered approach** for maximum consistency:

```
Transaction → Merchant Rules → Keyword Rules → Claude AI
              (100% deterministic)  (deterministic)   (intelligent fallback)
```

1. **Merchant Rules** (checked first): Exact pattern matches like "Whole Foods" → Groceries
2. **Keyword Rules** (checked second): Word matches like "battery" → Electronics
3. **Claude AI** (fallback): Handles ambiguous or unknown transactions

### Key Benefits

- **Consistent**: Same merchant always gets same category
- **Customizable**: You control the rules
- **Intelligent**: Claude handles edge cases
- **Auditable**: Every categorization is tracked with source and confidence

---

## Google Sheets Structure

### Sheet 1: Budget Config
**URL**: `https://docs.google.com/spreadsheets/d/<BUDGET_CONFIG_SHEET_ID>/edit`

Contains three tabs:

| Tab Name | Purpose | Key Columns |
|----------|---------|-------------|
| `Categories` | Master list of budget categories | category_id, category_name, parent_category, description, monthly_budget |
| `Merchant Rules` | Deterministic merchant → category mappings | merchant_pattern, category_id, confidence, notes |
| `Keywords` | Keyword-based category hints | keyword, category_id, priority |

### Sheet 2: Processed Transactions (Output)
**URL**: `https://docs.google.com/spreadsheets/d/<PROCESSED_TRANSACTIONS_SHEET_ID>/edit`

This is where categorized transactions are written. The categorizer adds these columns:

| Column | Description |
|--------|-------------|
| `claude_category` | The assigned category ID |
| `category_source` | How it was categorized: `merchant_rule`, `keyword`, `claude`, or `manual` |
| `category_confidence` | 0-100 confidence score |
| `categorized_at` | Timestamp of categorization |
| `categorized_by` | System identifier (e.g., `mcp_v1:batch_2026-01-28`) |
| `needs_review` | `TRUE` if flagged for human review |
| `review_reason` | Why it was flagged |
| `previous_category` | For undo capability |

---

## Workflow: Start to Finish

### Initial Setup (One Time)

1. **Populate Budget Config sheet** with your categories, merchant rules, and keywords
2. **Run transaction_matcher.py** to populate the Processed Transactions sheet
3. **Tell Claude**: "Categorize my uncategorized transactions"

### Ongoing Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│  1. New transactions appear in Processed Transactions sheet     │
│     (via transaction_matcher.py)                                │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  2. Tell Claude: "Categorize my uncategorized transactions"     │
│                                                                 │
│     Claude will:                                                │
│     • Fetch uncategorized rows                                  │
│     • Apply merchant rules (auto-categorize known merchants)    │
│     • Apply keyword rules (suggest categories)                  │
│     • Ask you about ambiguous transactions                      │
│     • Write categories back to the sheet                        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  3. Review flagged transactions (needs_review = TRUE)           │
│                                                                 │
│     • Filter sheet by needs_review column                       │
│     • Verify or correct categories                              │
│     • Add new merchant rules for frequently seen merchants      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  4. Analyze your budget                                         │
│                                                                 │
│     Tell Claude:                                                │
│     • "Show me spending by category this month"                 │
│     • "How much did I spend on groceries in January?"           │
│     • "Am I over budget in any categories?"                     │
└─────────────────────────────────────────────────────────────────┘
```

---

## Managing Categories

### Categories Tab Structure

| Column | Required | Description |
|--------|----------|-------------|
| `category_id` | ✅ Yes | Unique identifier (lowercase, no spaces). Example: `groceries` |
| `category_name` | ✅ Yes | Display name. Example: `Groceries` |
| `parent_category` | No | For grouping. Example: `Food & Dining` |
| `description` | No | What belongs here. Example: `Supermarkets and grocery stores` |
| `monthly_budget` | No | Your budget amount (not used by categorizer, for your reference) |
| Additional columns | No | Add any columns you want (notes, YTD, etc.) |

### Adding a New Category

1. Open the Budget Config sheet → Categories tab
2. Add a new row at the bottom:
   ```
   category_id: home_office
   category_name: Home Office
   parent_category: Shopping
   description: Home office furniture and equipment
   monthly_budget: 100
   ```
3. Tell Claude: "Reload your categorization config" (or it will auto-reload next session)

### Removing a Category

1. **First**: Re-categorize any transactions using this category
2. **Then**: Delete the row from the Categories tab
3. **Important**: Update any merchant rules or keywords pointing to this category

### Renaming a Category (Display Name Only)

If you just want to change how a category appears (e.g., "Groceries" → "Grocery Shopping"):

1. Open Categories tab
2. Change the `category_name` column — **leave `category_id` unchanged**
3. Done! All references remain valid.

### Changing a `category_id` (Migration Required)

Changing a `category_id` requires updating it everywhere it's used. **Avoid this if possible** — just change the display name instead.

If you must change an ID (e.g., renaming `gas` to `fuel`):

**Step 1: Update the Budget Config sheet manually**
1. Categories tab: Change `gas` → `fuel`
2. Merchant Rules tab: Change all `category_id` values from `gas` to `fuel`
3. Keywords tab: Change all `category_id` values from `gas` to `fuel`

**Step 2: Ask Claude to migrate transaction history**

Tell Claude:
> "Migrate all transactions from category 'gas' to 'fuel'"

Claude will:
1. Find all transactions with `claude_category = "gas"`
2. Update them to `claude_category = "fuel"`
3. Report how many were updated

**Step 3: Reload config**

Tell Claude:
> "Reload your categorization config"

**Example conversation:**
```
You: "I'm renaming the 'gas' category to 'fuel'. I've already updated 
      the Budget Config sheet. Please migrate all transactions."

Claude: "I found 127 transactions categorized as 'gas'. 
         Updating them to 'fuel'... Done! 
         127 transactions migrated."
```

### Merging Two Categories

To merge `coffee_shops` into `restaurants`:

1. Delete `coffee_shops` from Categories tab (or leave it for now)
2. Update any merchant rules pointing to `coffee_shops` → `restaurants`
3. Update any keywords pointing to `coffee_shops` → `restaurants`
4. Tell Claude: "Migrate all transactions from 'coffee_shops' to 'restaurants'"
5. Delete `coffee_shops` from Categories tab (if not done in step 1)

### Best Practices

- Keep `category_id` short and lowercase (e.g., `groceries`, not `Groceries & Food`)
- Use consistent `parent_category` names for grouping
- Aim for 30-50 categories (enough detail without being overwhelming)
- Don't create overlapping categories (e.g., don't have both "Electronics" and "Tech")

---

## Managing Merchant Rules

### Merchant Rules Tab Structure

| Column | Required | Description |
|--------|----------|-------------|
| `merchant_pattern` | ✅ Yes | Text to match (case-insensitive). Example: `whole foods` |
| `category_id` | ✅ Yes | Category to assign. Must exist in Categories tab |
| `confidence` | ✅ Yes | 0-100. Use 100 for definite matches |
| `notes` | No | Why this rule exists |

### How Matching Works

- **Case-insensitive**: `whole foods` matches "WHOLE FOODS MARKET #123"
- **Substring match**: `shell` matches "SHELL OIL 57442135"
- **First match wins**: Rules are checked in order; first match is used

### Adding a New Merchant Rule

1. Open Budget Config sheet → Merchant Rules tab
2. Add a new row:
   ```
   merchant_pattern: trader joe
   category_id: groceries
   confidence: 100
   notes: Always groceries
   ```

### Confidence Levels

| Confidence | When to Use | Behavior |
|------------|-------------|----------|
| **100** | Merchant is ALWAYS this category | Auto-categorize, no review |
| **80-99** | Almost always this category | Auto-categorize, no review |
| **50-79** | Usually this category | Auto-categorize, flag for review |
| **Below 50** | Uncertain | Don't auto-categorize |

### Handling Ambiguous Merchants

Some merchants (Target, Walmart, Costco, Amazon) sell multiple categories of items. For these:

**Option 1**: Don't create a rule (let Claude decide based on item details)

**Option 2**: Create a low-confidence rule:
```
merchant_pattern: target
category_id: general_shopping
confidence: 50
notes: Ambiguous - could be groceries, household, or clothing
```

**Option 3**: Create specific rules for sub-merchants:
```
merchant_pattern: costco gas
category_id: gas
confidence: 100

merchant_pattern: costco whse
category_id: groceries
confidence: 60
notes: Usually groceries but could be household
```

### Common Merchant Rules to Add

After running categorization a few times, add rules for merchants you see frequently:

```
# Local grocery stores
local_grocery_name, groceries, 100, My local grocery

# Favorite restaurants
favorite_restaurant, restaurants, 100, Date night spot

# Subscriptions
gym_name, fitness, 100, Monthly gym membership
```

---

## Managing Keywords

### Keywords Tab Structure

| Column | Required | Description |
|--------|----------|-------------|
| `keyword` | ✅ Yes | Word to match (case-insensitive). Example: `battery` |
| `category_id` | ✅ Yes | Suggested category. Must exist in Categories tab |
| `priority` | ✅ Yes | Higher = stronger signal (1-100). Example: `20` |

### How Keywords Work

- Keywords are checked **after** merchant rules fail
- **Word boundary match**: `battery` matches "Battery Pack" but not "Combattery"
- Multiple keywords can match; highest priority wins
- Keywords always flag for review (they're suggestions, not certainties)

### When to Use Keywords vs Merchant Rules

| Use Case | Solution |
|----------|----------|
| Specific merchant | Merchant Rule |
| Product type across merchants | Keyword |
| Amazon item descriptions | Keyword |

### Example Keywords

```
# Electronics
battery, electronics, 20
charger, electronics, 20
usb, electronics, 15
hdmi, electronics, 20

# Household
paper towel, household, 20
detergent, household, 20
cleaning, household, 15

# Pets
dog food, pets, 25
cat litter, pets, 25
```

---

## Using Claude to Categorize

### In Cursor IDE

1. Open your `claude_budget` project
2. Start a conversation with Claude
3. Say: **"Categorize my uncategorized transactions"**

### In Claude Desktop

1. Open Claude Desktop
2. The budget-categorizer MCP should be available
3. Say: **"Categorize my uncategorized transactions"**

### What Claude Does

1. **Fetches** uncategorized transactions (batches of 50)
2. **Applies** merchant rules → auto-categorizes known merchants
3. **Applies** keyword rules → suggests categories for matches
4. **Shows you** ambiguous transactions and asks for your input
5. **Writes** categories back to the sheet
6. **Offers to add merchant rules** for merchants you categorize (so it's automatic next time)
7. **Reports** summary (X auto-categorized, Y by Claude, Z flagged)

### Learning from Your Input

When you tell Claude how to categorize an ambiguous transaction, Claude can **automatically add a merchant rule** so the same merchant is auto-categorized next time:

**Example:**
```
Claude: "How should I categorize these?
  1. [Row 234] LOCAL GROCERY STORE $45.67
  2. [Row 235] MY GYM MEMBERSHIP $50.00"

You: "1 is groceries, 2 is fitness"

Claude: "Done! I categorized both transactions.
  
  Would you like me to add merchant rules so these are automatic next time?
  - 'local grocery store' → groceries
  - 'my gym membership' → fitness"

You: "Yes"

Claude: "Added 2 merchant rules. Future transactions from these merchants
  will be auto-categorized."
```

### Useful Commands

| Command | What It Does |
|---------|--------------|
| "Categorize my uncategorized transactions" | Main workflow |
| "Get my categorization stats" | Show progress |
| "Reload categorization config" | Pick up rule changes |
| "Show me the category taxonomy" | List all categories |
| "Categorize the next batch" | Continue after first batch |
| "Add a merchant rule for X → category" | Create new merchant rule |
| "Add a keyword rule for X → category" | Create new keyword rule |
| "Sync my Amazon emails" | Run email backfill (Claude will ask which mode) |
| "Backfill Amazon emails from January" | Run with date filter |
| "Match my transactions" | Run transaction matcher (Claude will ask which mode) |
| "Process bank transactions" | Same as above |

### Tips

- **Start with small batches**: First time, let Claude do 50 transactions so you can review
- **Add rules as you go**: When you see the same merchant multiple times, add a rule
- **Trust the merchant rules**: They're deterministic and consistent
- **Review Claude's decisions**: Especially early on, check that Claude's categorizations make sense

---

## Reviewing Flagged Transactions

### Finding Flagged Transactions

1. Open your Processed Transactions sheet
2. Filter by `needs_review` = `TRUE`
3. Review each one:
   - Check `claude_category` — is it correct?
   - Check `review_reason` — why was it flagged?

### Correcting a Category

1. Change the `claude_category` value to the correct category_id
2. Change `category_source` to `manual`
3. Clear the `needs_review` flag (or leave it for audit trail)

### Learning from Reviews

When you correct the same type of transaction multiple times:

1. **Add a merchant rule** if it's a specific merchant
2. **Add a keyword** if it's a product type
3. This prevents the same mistake in the future

---

## Updating Google API Credentials

### When Credentials Expire

Google OAuth tokens expire periodically. If you see authentication errors:

### Refreshing the Token

1. **Delete the existing token**:
   ```bash
   rm ~/claude_budget/token.json
   ```

2. **Run any script that uses Google APIs**:
   ```bash
   cd ~/claude_budget
   source venv/bin/activate
   python transaction_matcher.py
   ```

3. **Browser will open** — sign in with your Google account and authorize

4. **New token.json created** — credentials are refreshed

### If credentials.json is Missing or Invalid

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Select your project
3. Go to **APIs & Services** → **Credentials**
4. Download OAuth 2.0 Client ID (Desktop app type)
5. Save as `~/claude_budget/credentials.json`
6. Delete `token.json` and re-authorize

### Required API Scopes

The system needs these permissions:
- `https://www.googleapis.com/auth/spreadsheets` — Read/write Google Sheets
- `https://www.googleapis.com/auth/gmail.readonly` — Read Gmail (for email parsing)

---

## Troubleshooting

### "No uncategorized transactions found"

**Cause**: All transactions already have a `claude_category` value

**Solutions**:
- Run `transaction_matcher.py` to process new transactions
- Check if `claude_category` column exists and is being read correctly

### "Category not in allowed list"

**Cause**: A merchant rule or keyword references a category_id that doesn't exist

**Solution**:
1. Check the Categories tab for the missing category
2. Either add the category or fix the rule/keyword

### "Unable to parse range: 'Merchant Rules'!A:D"

**Cause**: Tab name mismatch

**Solution**:
- Ensure tab is named exactly `Merchant Rules` (with space, not underscore)
- Tab names are case-sensitive

### "Credentials file not found"

**Cause**: Running from wrong directory

**Solution**:
```bash
cd ~/claude_budget
source venv/bin/activate
python mcp_categorizer/test_categorizer.py
```

### "Token has been expired or revoked"

**Cause**: Google OAuth token expired

**Solution**:
```bash
rm ~/claude_budget/token.json
# Then run any script to re-authorize
```

### MCP Not Loading in Cursor/Claude Desktop

**Check config file exists**:
```bash
# Cursor
cat ~/.cursor/mcp.json

# Claude Desktop
cat ~/Library/Application\ Support/Claude/claude_desktop_config.json
```

**Check Python path is correct**:
```bash
ls -la ~/claude_budget/venv/bin/python
```

**Check server runs manually**:
```bash
cd ~/claude_budget
source venv/bin/activate
python mcp_categorizer/server.py
# Should start without errors (Ctrl+C to stop)
```

---

## File Locations Reference

| File | Location | Purpose |
|------|----------|---------|
| MCP Server | `~/claude_budget/mcp_categorizer/server.py` | Main MCP server |
| Config | `~/claude_budget/mcp_categorizer/config.py` | Sheet IDs and settings |
| Credentials | `~/claude_budget/credentials.json` | Google OAuth client config |
| Token | `~/claude_budget/token.json` | Cached access token (auto-generated) |
| Cursor MCP Config | `~/.cursor/mcp.json` | Cursor MCP server config |
| Claude Desktop Config | `~/Library/Application Support/Claude/claude_desktop_config.json` | Claude Desktop MCP config |
| Python venv | `~/claude_budget/venv/` | Virtual environment with dependencies |

---

## Quick Reference Card

### Daily Commands

```
"Categorize my uncategorized transactions"
"Get my categorization stats"
"Show me spending by category this month"
```

### After Adding Rules

```
"Reload categorization config"
```

### Checking Status

```
"How many transactions are uncategorized?"
"Show me transactions flagged for review"
```

### Refresh Credentials

```bash
cd ~/claude_budget
rm token.json
source venv/bin/activate
python transaction_matcher.py
# Sign in when browser opens
```
