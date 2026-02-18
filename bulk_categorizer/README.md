# Bulk Transaction Categorizer

Processes large batches of transactions using the Claude API directly.

Use this for initial bulk categorization (1000+ transactions). For regular
updates with smaller batches, use the MCP tools in Claude Desktop.

## Setup

### 1. Install Dependencies

```bash
cd ~/claude_budget
source venv/bin/activate
pip install -r bulk_categorizer/requirements.txt
```

### 2. Set API Key

Get your API key from [console.anthropic.com](https://console.anthropic.com/)

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
```

Or add to your shell profile (~/.zshrc or ~/.bashrc):
```bash
echo 'export ANTHROPIC_API_KEY="sk-ant-..."' >> ~/.zshrc
```

## Usage

### Basic Run

```bash
cd ~/claude_budget
source venv/bin/activate
python bulk_categorizer/bulk_categorize.py
```

### Options

```bash
# Dry run (test without writing)
python bulk_categorizer/bulk_categorize.py --dry-run

# Custom batch size
python bulk_categorizer/bulk_categorize.py --batch-size 50

# Limit number of batches
python bulk_categorizer/bulk_categorize.py --max-batches 10

# Resume from previous run
python bulk_categorizer/bulk_categorize.py --resume

# Disable rule learning
python bulk_categorizer/bulk_categorize.py --no-learn-rules

# Combine options
python bulk_categorizer/bulk_categorize.py -b 50 -m 20 --dry-run
```

## How It Works

```
1. Load merchant/keyword rules from config sheet
2. Fetch uncategorized transactions
3. Apply deterministic rules first (free, instant)
4. Send remaining to Claude API in batches
5. Parse Claude's JSON response
6. Write categories to sheet
7. Auto-learn merchant rules from repeated patterns
8. Repeat until done
```

## Merchant Rule Learning

When Claude categorizes the same merchant pattern 2+ times with the same
category, the script automatically creates a merchant rule. This means:

- First run: Claude categorizes "TRADER JOE" -> groceries
- Second run: Claude categorizes "TRADER JOE" -> groceries again
- Script creates rule: `trader joe` -> `groceries`
- Future runs: All "TRADER JOE" transactions are instant (no API call)

## Cost Estimate

Using claude-sonnet-4-20250514:
- ~$0.003 per input token (1M tokens)
- ~$0.015 per output token (1M tokens)

Typical batch (100 transactions):
- Input: ~2,000 tokens
- Output: ~1,000 tokens
- Cost: ~$0.02 per batch

For 5,000 transactions:
- After rules: ~2,000-3,000 need Claude
- ~20-30 API calls
- **Total cost: ~$0.50-1.00**

## Output Files

- `bulk_categorize.log` - Detailed log
- `last_run_stats.json` - Statistics from last run
- `.progress.json` - Progress for resume capability

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ANTHROPIC_API_KEY` | (required) | Your Claude API key |
| `CLAUDE_MODEL` | claude-sonnet-4-20250514 | Model to use |
| `BATCH_SIZE` | 100 | Transactions per API call |
| `MAX_BATCHES` | 100 | Maximum batches |
| `BATCH_DELAY` | 1.0 | Seconds between batches |

## Comparison with MCP Approach

| Aspect | MCP (Claude Desktop) | Bulk Script |
|--------|---------------------|-------------|
| Best for | Regular updates (<100) | Initial bulk (1000+) |
| Speed | Slow (UI round-trips) | Fast (direct API) |
| Cost | Included in Pro | ~$0.02/batch |
| Interaction | Conversational | Automated |
| Context | Shared, limited | Fresh each batch |
