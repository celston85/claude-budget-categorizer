# Transaction Categorization Skill

This skill enables Claude to categorize budget transactions using the MCP Transaction Categorizer server.

## Overview

You have access to an MCP server called `budget-categorizer` that provides tools for reading and categorizing transactions in Google Sheets. The system uses a layered approach:

1. **Merchant Rules** (100% deterministic) - Known merchant → category mappings
2. **Keyword Rules** (deterministic) - Keyword-based category suggestions
3. **Claude Judgment** (your decision) - For ambiguous transactions

## Available MCP Tools

### Reading Data
- `get_uncategorized_transactions(limit?, offset?)` - Get transactions without categories
- `get_category_taxonomy()` - Get list of valid categories
- `get_categorization_stats()` - Get progress statistics

### Categorization
- `apply_rules_to_transactions(transactions, filter_parent_categories?, filter_categories?)` - Apply merchant/keyword rules (with optional filtering)
- `write_categories(updates)` - Write categories back to sheet
- `validate_category(category_id)` - Check if a category ID is valid

### Filtering Examples
```
# Only process Food-related transactions
apply_rules_to_transactions(
    transactions=...,
    filter_parent_categories=["Food"]
)

# Only process specific categories
apply_rules_to_transactions(
    transactions=...,
    filter_categories=["groceries", "restaurants"]
)
```

### Review & Config
- `flag_for_review(row_number, reason)` - Flag for human review
- `reload_config()` - Reload rules from config sheet
- `add_merchant_rule(merchant_pattern, category_id, confidence?, notes?)` - Add a new merchant rule
- `add_keyword(keyword, category_id, priority?)` - Add a new keyword rule
- `migrate_category(old_category_id, new_category_id)` - Bulk-update transactions from one category to another

### Email Backfill & Transaction Matching (Upstream Data)
- `run_email_backfill(mode, limit?, start_date?, end_date?, email_type?, dry_run?)` - Fetch Amazon emails from Gmail
- `run_transaction_matcher(mode)` - Match bank transactions with parsed Amazon orders

## Email Backfill Workflow

When the user asks to sync/backfill Amazon emails, **ALWAYS ASK which mode they want first**:

### Step 1: Ask the User
Present these options:

> **Which backfill mode would you like?**
> 
> 1. **Optimized + Incremental** (recommended)
>    - Only fetches NEW emails since last run
>    - Fast, uses batch API
>    - Best for: daily/weekly syncs
> 
> 2. **Optimized (Full)**
>    - Fetches ALL matching emails
>    - 3-5x faster than legacy
>    - Best for: initial setup, re-syncing everything
> 
> 3. **Legacy**
>    - Sequential fetching (slower)
>    - Best for: debugging, testing with small limits

### Step 2: Run with User's Choice
```
Call: run_email_backfill(
  mode: "optimized_incremental",  // or "optimized" or "legacy"
  email_type: "all"               // or "orders", "shipments", "returns"
)
```

### Step 3: Report Results
Tell the user:
- How many emails were processed
- Any errors encountered
- Suggestion to run transaction matcher next

---

## Transaction Matcher Workflow

When the user asks to match transactions or process bank data, **ALWAYS ASK which mode they want first**:

### Step 1: Ask the User
Present these options:

> **Which processing mode would you like?**
> 
> 1. **Production** (recommended)
>    - Appends new rows, skips already-processed transactions
>    - Preserves existing data
>    - Best for: daily/regular runs
> 
> 2. **Development**
>    - Clears output sheet, reprocesses ALL transactions
>    - Best for: testing, debugging, full refresh

### Step 2: Run with User's Choice
```
Call: run_transaction_matcher(mode: "production")  // or "development"
```

### Step 3: Report Results
Tell the user:
- How many transactions were matched
- Any errors encountered
- Suggestion to run categorization next

---

## Categorization Workflow

When the user asks to categorize transactions, follow this workflow:

### Step 1: Check Status
```
Call: get_categorization_stats()
```
Report current progress to user.

### Step 2: Get Uncategorized Transactions
```
Call: get_uncategorized_transactions(limit=50)
```

### Step 3: Get Category Taxonomy (if needed)
```
Call: get_category_taxonomy()
```
Review available categories before making decisions.

### Step 4: Apply Rules First
```
Call: apply_rules_to_transactions(transactions)
```
This returns:
- `auto_categorized`: Handled by rules, ready to write
- `needs_claude`: Need your judgment
- `needs_review`: Has category but should be verified

### Step 5: Categorize Remaining
For transactions in `needs_claude`, analyze each one:
- Look at the Description and Amount
- Consider any hints provided
- Choose the most appropriate category from the taxonomy
- Assign a confidence score (0-100)
- Flag for review if uncertain

### Step 6: Write Results
```
Call: write_categories([
  {
    "row_number": 123,
    "category_id": "groceries",
    "source": "claude",
    "confidence": 85,
    "needs_review": false
  },
  ...
])
```

### Step 7: Report Results
Tell the user:
- How many were auto-categorized by rules
- How many you categorized
- How many flagged for review
- Any errors encountered

## Category Assignment Guidelines

### High Confidence (80-100)
- Clear, unambiguous transactions
- Strong keyword/context match
- Example: "WHOLE FOODS MARKET #123" → groceries (100)

### Medium Confidence (50-79)
- Reasonable match but some ambiguity
- Set `needs_review: true`
- Example: "AMAZON.COM" with item "batteries" → electronics (65)

### Low Confidence (below 50)
- Use `flag_for_review()` instead
- Don't auto-categorize

### Ambiguous Merchants
These merchants require item-level detail:
- Target (could be groceries, household, clothing)
- Walmart (could be groceries, household, electronics)
- Costco (could be groceries, household, gas)
- Amazon (depends on item)

When you see these without clear item detail, flag for review.

## Example Conversation

**User:** Categorize my transactions

**Assistant:** Let me check your categorization status and process uncategorized transactions.

[Calls get_categorization_stats()]

You have 150 uncategorized transactions out of 1,234 total (88% complete).

[Calls get_uncategorized_transactions(limit=50)]
[Calls get_category_taxonomy()]
[Calls apply_rules_to_transactions(transactions)]

I found 50 uncategorized transactions. After applying merchant and keyword rules:
- 32 auto-categorized by rules
- 18 need my judgment

Let me analyze the remaining 18...

[Analyzes and prepares updates]
[Calls write_categories(updates)]

Done! Here's the summary:
- **32** categorized by merchant rules (100% confidence)
- **15** categorized by me (avg 78% confidence)
- **3** flagged for review (ambiguous merchants)

Would you like me to continue with the next batch?

## Learning from User Input

When a user tells you how to categorize an ambiguous transaction, **offer to add a merchant rule** so it's automatic next time:

### Example
```
User: "TARGET $45.67 is groceries"

You: [Writes category to the transaction]
     "Done! Would you like me to add a merchant rule so 'target' 
      is always categorized as groceries in the future?"

User: "Yes"

You: [Calls add_merchant_rule("target", "groceries", 100, "User rule")]
     "Added! Future Target transactions will be auto-categorized."
```

### When to Offer
- User provides explicit categorization for a new merchant
- User corrects an incorrect auto-categorization
- User categorizes multiple transactions from the same merchant

### When NOT to Add Rules
- The merchant is inherently ambiguous (Target, Amazon, Costco, Walmart)
- The categorization was item-specific, not merchant-specific
- User declines the offer

## Important Notes

1. **Always use valid category_ids** from get_category_taxonomy()
2. **Merchant rules take precedence** - don't override high-confidence rule matches
3. **Be conservative** - when uncertain, flag for review rather than guess
4. **Batch in groups of 50** to avoid context limits
5. **Report progress** so user knows what's happening
6. **Ask before running email backfill** - let user choose the mode
7. **Offer to add rules** when user teaches you a new categorization
