#!/bin/bash
set -e

echo "=== Budget Categorizer MCP Setup ==="
echo ""

# Get project directory (where this script lives)
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

# ── [1/6] Python ─────────────────────────────────────────────────────────────
echo "[1/6] Checking Python version..."
if ! command -v python3 &>/dev/null; then
    echo "  Error: python3 not found. Install with: brew install python@3.12"
    exit 1
fi
if python3 -c "import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)"; then
    python_version=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
    echo "  Python $python_version ✓"
else
    python_version=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
    echo "  Error: Python 3.10+ required (you have $python_version)"
    echo "  Install with: brew install python@3.12"
    exit 1
fi

# ── [2/6] Virtual environment & dependencies ─────────────────────────────────
echo "[2/6] Setting up virtual environment and dependencies..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "  Created venv ✓"
else
    echo "  venv already exists ✓"
fi
source venv/bin/activate
pip install --upgrade pip -q
pip install -r mcp_categorizer/requirements.txt -q
if [ -f "bulk_categorizer/requirements.txt" ]; then
    pip install -r bulk_categorizer/requirements.txt -q
fi
echo "  Dependencies installed ✓"

# ── [3/6] Google credentials ─────────────────────────────────────────────────
echo "[3/6] Checking Google API credentials..."
if [ -f "credentials.json" ]; then
    echo "  credentials.json found ✓"
else
    echo ""
    echo "  ⚠️  credentials.json not found!"
    echo ""
    echo "  To get this file:"
    echo "    1. Go to https://console.cloud.google.com/"
    echo "    2. Create a project (or select existing)"
    echo "    3. Enable Google Sheets API and Gmail API"
    echo "    4. Go to APIs & Services → Credentials"
    echo "    5. Create OAuth 2.0 Client ID (Desktop app type)"
    echo "    6. Download and save as: $PROJECT_DIR/credentials.json"
    echo ""
    echo "  If a team member shared this project, ask them for credentials.json"
    echo ""
    read -p "  Press Enter to continue (you can add it later)..."
fi

# ── [4/6] Google Sheets ──────────────────────────────────────────────────────
echo "[4/6] Configuring Google Sheets..."
echo ""
echo "  Your Budget Config Sheet holds categories, merchant rules, and keywords."
echo ""
read -p "  Create a new config sheet or use an existing one? (new/existing) [existing]: " config_choice
config_choice="${config_choice:-existing}"

if [ "$config_choice" = "new" ]; then
    echo "  Creating new config sheet..."
    python3 mcp_categorizer/setup_config_sheet.py
    echo ""
    read -p "  Enter the Config Sheet ID printed above: " config_sheet_id
else
    read -p "  Enter your Budget Config Sheet ID: " config_sheet_id
fi

if [ -z "$config_sheet_id" ]; then
    echo "  Error: Config Sheet ID is required."
    exit 1
fi

echo ""
echo "  Your Processed Transactions Sheet is where categorized transactions are written."
read -p "  Enter your Processed Transactions Sheet ID: " transactions_sheet_id

if [ -z "$transactions_sheet_id" ]; then
    echo "  Error: Transactions Sheet ID is required."
    exit 1
fi
echo "  Sheet IDs configured ✓"

# ── [5/6] Anthropic API key (optional) ───────────────────────────────────────
echo ""
echo "[5/6] Anthropic API key (optional)"
echo "  Used for Claude-powered categorization of ambiguous transactions."
echo "  You can also store the key in macOS Keychain later."
echo ""
read -p "  Enter Anthropic API key (or press Enter to skip): " api_key

api_key_arg=""
if [ -n "$api_key" ]; then
    api_key_arg="--api-key $api_key"
    echo "  API key configured ✓"
else
    echo "  Skipped"
fi

# ── [6/6] Claude Desktop config ──────────────────────────────────────────────
echo ""
echo "[6/6] Configuring Claude Desktop..."
# shellcheck disable=SC2086
python3 configure_claude_desktop.py \
    --config-sheet-id "$config_sheet_id" \
    --transactions-sheet-id "$transactions_sheet_id" \
    $api_key_arg

echo ""
echo "============================================"
echo "  ✅ Setup complete!"
echo "============================================"
echo ""
echo "Next steps:"
echo "  1. Restart Claude Desktop"
echo "  2. Test: Ask Claude 'What categorization tools do you have?'"
if [ ! -f "credentials.json" ]; then
    echo "  3. Add credentials.json to this directory"
fi
echo ""
