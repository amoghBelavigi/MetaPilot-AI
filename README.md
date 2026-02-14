# MetaPilot-AI

AI-powered Slack bot that answers enterprise data questions using the Alation metadata catalog via MCP tools and Claude.

## Overview

MetaPilot-AI connects to Slack via Socket Mode and uses a Model Context Protocol (MCP) server to query live metadata from Alation. It uses AWS Bedrock (Claude 4.5 Sonnet) with tool-use to search, browse, and explain data assets -- all from real-time catalog data, no hallucination.

### Key Features
- **Live Alation Queries** — Real-time metadata directly from your enterprise catalog
- **Search-First** — Finds tables, schemas, and columns by name across all data sources instantly
- **Governance-Aware** — Ownership, stewardship, certification, and trust status
- **Lineage Tracking** — Upstream sources and downstream dependencies
- **MCP Architecture** — Model Context Protocol bridges the LLM with Alation cleanly
- **Claude 4.5 Sonnet** — AWS Bedrock with tool-use for intelligent, multi-step reasoning
- **Thread Context** — Remembers conversation history within Slack threads
- **No Vector Store** — Alation is the single source of truth, no embeddings needed

## Prerequisites

- Python 3.10+
- Alation instance with API access
- AWS Account with Bedrock access (Claude 4.5 Sonnet)
- Slack App with Socket Mode enabled

## Setup

### 1. Environment Variables

Copy `.env.example` to `.env` and fill in your credentials:

```env
# Alation
ALATION_BASE_URL=https://your-company.alation.com
ALATION_API_TOKEN=your_refresh_token_here
ALATION_USER_ID=your_numeric_user_id

# Slack
SLACK_BOT_TOKEN=xoxb-...
SLACK_APP_TOKEN=xapp-...
SLACK_SIGNING_SECRET=...

# AWS (Bedrock)
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-west-2
```

### 2. Alation Token Setup

1. Log into Alation → Account Settings → Authentication → Refresh Tokens
2. Generate a Refresh Token and paste it as `ALATION_API_TOKEN`
3. Find your numeric user ID from your profile URL (`/user/<ID>/`)
4. The bot automatically exchanges the Refresh Token for an API Access Token on startup

### 3. Installation

```bash
python -m venv venv

# Windows
.\venv\Scripts\Activate.ps1
# Unix/macOS
source venv/bin/activate

pip install -r requirements.txt
```

## Running

```bash
python -m app.socket_mode
```

This starts both the Slack bot and the Alation MCP server automatically.

## Usage Examples

Ask your bot questions like:

- "What data sources are available?"
- "Tell me about the `FCT_STORE_TRANSACTION_ITEM` table"
- "What columns are in `TELEMETRY_NATIVE_APPLICATION_SESSION_END_BI`?"
- "Who owns the `customer_360` table?"
- "Where does the customer data come from?"
- "Show me the lineage for `customer_summary`"

## MCP Tools

MetaPilot-AI exposes 9 tools to Claude:

| Category | Tool | Purpose |
|----------|------|---------|
| **Search** | `search_table` | Find a table by name across all data sources |
| **Search** | `search_schema` | Find schemas matching a keyword |
| **Search** | `search_columns` | Find columns by name, optionally filtered by table |
| **Browse** | `list_data_sources` | List all accessible data sources |
| **Browse** | `list_schemas` | List schemas in a data source |
| **Browse** | `list_tables` | List tables in a schema |
| **Detail** | `get_table_metadata` | Ownership, certification, description |
| **Detail** | `get_column_metadata` | Column types, descriptions |
| **Detail** | `get_lineage` | Upstream/downstream dependencies |

## Project Structure

```
metapilot-ai/
├── app/
│   ├── main.py                    # FastAPI app (HTTP mode)
│   ├── socket_mode.py             # Main entry point (Socket Mode)
│   ├── core/
│   │   └── config.py              # Configuration and client initialization
│   ├── models/
│   │   └── schemas.py             # Pydantic data models
│   ├── services/rag/
│   │   ├── alation_adapter.py     # Alation REST API adapter (auth, caching, retry)
│   │   ├── alation_server.py      # MCP server with 9 tools
│   │   ├── alation_client.py      # SSE client for MCP communication
│   │   ├── engine.py              # MetadataAssistant orchestration
│   │   ├── generator.py           # LLM generation with parallel tool execution
│   │   └── prompts.py             # System prompt with Slack formatting
│   └── slack/
│       ├── handlers.py            # Slack event handlers + message splitting
│       └── events.py              # HTTP webhook routes (alternative mode)
├── requirements.txt
├── .env.example
├── README.md
└── ARCHITECTURE.md
```

## Tech Stack

- **LLM**: AWS Bedrock — Claude 4.5 Sonnet (cross-region inference)
- **Metadata Catalog**: Alation (REST API v1 + v2)
- **Protocol**: Model Context Protocol (MCP) via FastMCP
- **Slack**: Slack Bolt for Python (Socket Mode)
- **HTTP**: Requests + urllib3 with retry logic

## Security

- **Read-Only** — All Alation operations are GET requests
- **No Hallucination** — Returns "unknown" for missing data, never guesses
- **Token Exchange** — Refresh Token auto-exchanged for API Access Token
- **Access Control** — Respects Alation's permission model
- **Audit Trail** — All API calls and tool executions logged

---

Internal use only. Proprietary.
