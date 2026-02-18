#!/usr/bin/env python3
"""Auto-configure Claude Desktop for the budget categorizer MCP."""

import argparse
import json
import os
import shutil
from datetime import datetime
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(
        description='Configure Claude Desktop for budget categorizer MCP'
    )
    parser.add_argument('--config-sheet-id', required=True,
                        help='Budget Config Google Sheet ID')
    parser.add_argument('--transactions-sheet-id', required=True,
                        help='Processed Transactions Google Sheet ID')
    parser.add_argument('--api-key',
                        help='Anthropic API key (optional, can use macOS Keychain instead)')
    args = parser.parse_args()

    project_dir = Path(__file__).parent.absolute()

    # Find python binary — prefer venv, fall back to system
    venv_python = project_dir / "venv" / "bin" / "python"
    if not venv_python.exists():
        venv_python = "python3"
    else:
        venv_python = str(venv_python)

    server_script = str(project_dir / "mcp_categorizer" / "server.py")

    # Claude Desktop config location
    if os.name == 'nt':  # Windows
        config_path = Path.home() / "AppData" / "Roaming" / "Claude" / "claude_desktop_config.json"
    else:  # macOS/Linux
        config_path = Path.home() / "Library" / "Application Support" / "Claude" / "claude_desktop_config.json"

    # Load existing config or create new
    if config_path.exists():
        backup_name = f".backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        backup_path = config_path.with_suffix(backup_name)
        shutil.copy2(config_path, backup_path)
        print(f"  Backup: {backup_path}")

        with open(config_path) as f:
            config = json.load(f)
    else:
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config = {}

    # Merge — only touch the budget-categorizer entry, preserve everything else
    if "mcpServers" not in config:
        config["mcpServers"] = {}

    env = {
        "BUDGET_CONFIG_SHEET_ID": args.config_sheet_id,
        "PROCESSED_TRANSACTIONS_SHEET_ID": args.transactions_sheet_id,
    }
    if args.api_key:
        env["ANTHROPIC_API_KEY"] = args.api_key

    config["mcpServers"]["budget-categorizer"] = {
        "command": venv_python,
        "args": [server_script],
        "cwd": str(project_dir),
        "env": env,
    }

    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)

    print(f"  Updated: {config_path}")
    print(f"  Python:  {venv_python}")
    print(f"  Config sheet:       {args.config_sheet_id}")
    print(f"  Transactions sheet: {args.transactions_sheet_id}")
    if args.api_key:
        print(f"  API key: configured via env var")


if __name__ == "__main__":
    main()
