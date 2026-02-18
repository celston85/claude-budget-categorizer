# Sharing the Budget Categorizer MCP with Your Team

This guide covers options for sharing the MCP Transaction Categorizer with other Claude Team members.

## Platform Compatibility

| Platform | MCP Support | Notes |
|----------|-------------|-------|
| Claude Desktop (Mac) | ✅ Yes | Full support via `mcp.json` config |
| Claude Desktop (Windows) | ✅ Yes | Full support via `mcp.json` config |
| Claude Web (claude.ai) | ❌ No | Browser can't run local servers |
| Claude iOS App | ❌ No | Mobile can't run local servers |
| Claude Android App | ❌ No | Mobile can't run local servers |
| Cursor IDE | ✅ Yes | Full support via `~/.cursor/mcp.json` |

**Bottom line:** MCPs only work on desktop applications that can spawn local processes.

---

## Sharing Options

### Option 1: Each Person Runs Locally (Recommended for Small Teams)

Each team member sets up the MCP on their own machine.

#### What Each Person Needs

1. **Code files** - Copy or clone `mcp_categorizer/` folder
2. **Python 3.10+** - Install via Homebrew or python.org
3. **Dependencies** - `pip install -r requirements.txt`
4. **Google credentials** - Either shared or individual (see below)
5. **Claude Desktop config** - Add MCP to their `claude_desktop_config.json`

#### Setup Steps for Team Members

```bash
# 1. Get the code (via git, shared drive, etc.)
cp -r /path/to/shared/mcp_categorizer ~/claude_budget/mcp_categorizer

# 2. Create virtual environment
cd ~/claude_budget
python3 -m venv venv
source venv/bin/activate

# 3. Install dependencies
pip install -r mcp_categorizer/requirements.txt

# 4. Copy shared credentials (get from team lead)
cp /path/to/shared/credentials.json ~/claude_budget/credentials.json

# 5. First run - will open browser for OAuth
python mcp_categorizer/test_categorizer.py
```

#### Configure Claude Desktop

Edit `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "budget-categorizer": {
      "command": "/Users/USERNAME/claude_budget/venv/bin/python",
      "args": ["/Users/USERNAME/claude_budget/mcp_categorizer/server.py"],
      "cwd": "/Users/USERNAME/claude_budget"
    }
  }
}
```

**Important:** Replace `USERNAME` with each person's actual username.

#### Pros & Cons

| Pros | Cons |
|------|------|
| Works offline | Setup required per person |
| No cloud costs | Each person needs Python setup |
| Fast (local execution) | Credential management |
| Most secure | Path differences per machine |

---

### Option 2: Shared GitHub Repo + Setup Script

Automate the setup process with a shared repository.

#### Repository Structure

```
claude-budget-mcp/
├── mcp_categorizer/
│   ├── server.py
│   ├── config.py
│   ├── sheets_client.py
│   ├── categorizer.py
│   ├── requirements.txt
│   └── ...
├── setup.sh                    # Automated setup script
├── configure_claude_desktop.py # Auto-configure Claude Desktop
├── credentials.json            # ⚠️ DO NOT COMMIT - share separately
├── .gitignore
└── README.md
```

#### Sample `setup.sh`

```bash
#!/bin/bash
set -e

echo "=== Budget Categorizer MCP Setup ==="

# Check Python version
python_version=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
if [[ $(echo "$python_version < 3.10" | bc -l) -eq 1 ]]; then
    echo "Error: Python 3.10+ required. You have $python_version"
    echo "Install with: brew install python@3.12"
    exit 1
fi

# Create virtual environment
echo "Creating virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install --upgrade pip
pip install -r mcp_categorizer/requirements.txt

# Check for credentials
if [ ! -f "credentials.json" ]; then
    echo ""
    echo "⚠️  credentials.json not found!"
    echo "Get this file from your team lead and place it in this directory."
    echo ""
fi

# Configure Claude Desktop
echo "Configuring Claude Desktop..."
python3 configure_claude_desktop.py

echo ""
echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Place credentials.json in this directory (if not already done)"
echo "2. Restart Claude Desktop"
echo "3. Test with: 'What categorization tools do you have?'"
```

#### Sample `configure_claude_desktop.py`

```python
#!/usr/bin/env python3
"""Auto-configure Claude Desktop for the budget categorizer MCP."""

import json
import os
from pathlib import Path

def main():
    # Paths
    project_dir = Path(__file__).parent.absolute()
    venv_python = project_dir / "venv" / "bin" / "python"
    server_script = project_dir / "mcp_categorizer" / "server.py"
    
    # Claude Desktop config location
    if os.name == 'nt':  # Windows
        config_path = Path.home() / "AppData" / "Roaming" / "Claude" / "claude_desktop_config.json"
    else:  # macOS/Linux
        config_path = Path.home() / "Library" / "Application Support" / "Claude" / "claude_desktop_config.json"
    
    # Load existing config or create new
    if config_path.exists():
        with open(config_path) as f:
            config = json.load(f)
    else:
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config = {}
    
    # Add/update MCP server
    if "mcpServers" not in config:
        config["mcpServers"] = {}
    
    config["mcpServers"]["budget-categorizer"] = {
        "command": str(venv_python),
        "args": [str(server_script)],
        "cwd": str(project_dir)
    }
    
    # Write config
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)
    
    print(f"✅ Updated {config_path}")
    print(f"   MCP server: budget-categorizer")
    print(f"   Python: {venv_python}")
    print(f"   Server: {server_script}")

if __name__ == "__main__":
    main()
```

#### Pros & Cons

| Pros | Cons |
|------|------|
| Automated setup | Still need credential sharing |
| Version controlled | Git knowledge required |
| Easy updates (git pull) | Initial repo setup effort |
| Consistent across team | |

---

### Option 3: Cloud-Hosted MCP Server

Deploy the MCP as a cloud service that all team members connect to.

#### Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ Team Member A   │     │  Cloud Server   │     │  Google Sheets  │
│ Claude Desktop  │────▶│  MCP Server     │────▶│  Budget Data    │
└─────────────────┘     │  (Single creds) │     └─────────────────┘
                        └─────────────────┘
┌─────────────────┐            ▲
│ Team Member B   │────────────┘
│ Claude Desktop  │
└─────────────────┘
```

#### Hosting Options

| Platform | Estimated Cost | Difficulty |
|----------|----------------|------------|
| DigitalOcean Droplet | ~$6/month | Medium |
| AWS EC2 t3.micro | ~$8/month | Medium |
| Google Cloud Run | ~$5/month | Higher |
| Fly.io | ~$5/month | Medium |
| Self-hosted (home server) | $0 | Higher |

#### Implementation Notes

1. **MCP Protocol**: Uses stdio by default; for remote, you'd need to wrap in HTTP/WebSocket
2. **Authentication**: Add API keys or OAuth for each user
3. **Security**: HTTPS required, consider VPN for extra security
4. **Latency**: Slight delay vs. local execution

#### Pros & Cons

| Pros | Cons |
|------|------|
| Single credential setup | Monthly hosting costs |
| No local Python needed | Network dependency |
| Central updates | More complex setup |
| Works anywhere with internet | Security considerations |

---

## The Credential Challenge

The main friction point is Google API authentication. Here are your options:

### Option A: Shared OAuth Client ID (Recommended)

1. **One person** creates a GCP project and OAuth client
2. **Share `credentials.json`** with team (via secure channel, NOT in git)
3. **Each person** authorizes on first run (gets their own `token.json`)

```
credentials.json  ─── Shared (OAuth client config)
token.json        ─── Individual (each person's auth token)
```

**Security:** Each person has their own access token; revoking one doesn't affect others.

### Option B: Service Account (Easiest but Less Secure)

1. Create a GCP service account
2. Share the service account JSON key with team
3. Everyone uses the same credentials

**Security concern:** If key is leaked, must rotate for everyone.

### Option C: Individual OAuth Setup (Most Secure)

Each person:
1. Creates their own GCP project
2. Enables Gmail + Sheets APIs
3. Creates their own OAuth client
4. Uses their own credentials

**Pros:** Complete isolation
**Cons:** Most setup effort

---

## Recommendation for Small Teams (2-5 people)

1. **Create a private GitHub repo** with the MCP code
2. **Include `setup.sh`** for automated installation
3. **Share `credentials.json`** via secure channel (Slack DM, 1Password, etc.)
4. **Each person runs locally** on their Mac/PC
5. **Use shared Google Sheet** for budget data (everyone accesses same data)

This balances:
- ✅ Easy sharing (git clone + run setup)
- ✅ Reasonable security (individual tokens)
- ✅ No cloud costs
- ✅ Works offline

---

## Quick Reference: Team Member Onboarding

```bash
# 1. Clone the repo
git clone https://github.com/your-org/claude-budget-mcp.git
cd claude-budget-mcp

# 2. Get credentials from team lead
# (they'll send credentials.json via secure channel)

# 3. Run setup
chmod +x setup.sh
./setup.sh

# 4. Restart Claude Desktop

# 5. Test in Claude Desktop
# Ask: "What categorization tools do you have?"
```

---

## Related Files

- MCP Server: `mcp_categorizer/server.py`
- User Guide: `mcp_categorizer/USER_GUIDE.md`
- Claude Desktop Config: `~/Library/Application Support/Claude/claude_desktop_config.json`
- Cursor Config: `~/.cursor/mcp.json`
