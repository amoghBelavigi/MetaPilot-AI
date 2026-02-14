# MCP-based Metadata Assistant - Documentation

> Complete guide for setup, usage, and development

## Table of Contents
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [MCP Tools Reference](#mcp-tools-reference)
- [Usage Examples](#usage-examples)
- [Troubleshooting](#troubleshooting)
- [Development](#development)

---

## Quick Start

### Prerequisites
- Python 3.10+
- Alation instance with API access
- AWS Account with Bedrock access (Claude 4.5 Sonnet)
- Slack App with Socket Mode enabled

### 1. Setup Environment

```bash
# Create virtual environment
python -m venv venv

# Activate (Windows)
.\venv\Scripts\Activate.ps1

# Activate (Unix/macOS)
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure `.env`

Create a `.env` file in the project root:

```env
# Alation
ALATION_BASE_URL=https://your-company.alation.com
ALATION_API_TOKEN=your_api_token_here
ALATION_USER_ID=your_user_id  # Optional

# Slack
SLACK_BOT_TOKEN=xoxb-...
SLACK_APP_TOKEN=xapp-...
SLACK_SIGNING_SECRET=...

# AWS (for Bedrock)
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-west-2
```

### 3. Get Alation API Token

1. Log into Alation
2. Go to **User Settings** → **API Access Tokens**
3. Click **Create Token**
4. Copy token to `.env`

> **Note**: The token inherits your user permissions. The bot only sees metadata you have access to.

### 4. Start the Bot

```bash
python -m app.socket_mode
```

This starts both:
- Alation MCP server (port 8000)
- Slack bot (Socket Mode)

You should see:
```
INFO - Starting Alation MCP Server on port 8000
INFO - Connected to Alation instance: https://your-company.alation.com
INFO - Starting MCP-based Metadata Assistant in Socket Mode...
```

---

## Configuration

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `ALATION_BASE_URL` | Yes | Alation instance URL |
| `ALATION_API_TOKEN` | Yes | Alation API access token |
| `ALATION_USER_ID` | No | User ID for context operations |
| `SLACK_BOT_TOKEN` | Yes | Slack Bot OAuth Token (`xoxb-...`) |
| `SLACK_APP_TOKEN` | Yes | Slack App Token (`xapp-...`) |
| `SLACK_SIGNING_SECRET` | Yes | Request verification secret |
| `AWS_ACCESS_KEY_ID` | Yes | AWS credentials for Bedrock |
| `AWS_SECRET_ACCESS_KEY` | Yes | AWS credentials for Bedrock |
| `AWS_DEFAULT_REGION` | Yes | AWS region (e.g., `us-west-2`) |

### Caching

The Alation adapter uses in-memory caching:
- **TTL**: 5 minutes (configurable in `alation_adapter.py`)
- **Scope**: Process-level
- First call hits Alation API, subsequent calls within TTL use cache

---

## MCP Tools Reference

The bot exposes 6 tools to query Alation metadata:

### 1. `list_data_sources`
Lists all accessible data sources.

```json
// Response
[
  {"data_source_id": 123, "name": "Production Snowflake", "type": "snowflake", "description": "Main warehouse"}
]
```

### 2. `list_schemas`
Lists schemas in a data source.

**Input**: `data_source_id`

```json
// Response
[
  {"schema_name": "analytics", "schema_description": "Transformed data for analytics"}
]
```

### 3. `list_tables`
Lists tables with metadata.

**Input**: `data_source_id`, `schema_name`

```json
// Response
[
  {"table_name": "customers", "table_type": "TABLE", "row_count": 2500000, "popularity": 92}
]
```

### 4. `get_table_metadata`
Gets detailed table information.

**Input**: `data_source_id`, `schema_name`, `table_name`

```json
// Response
{
  "table_name": "customers",
  "table_description": "Customer master data",
  "owner": "john.doe@company.com",
  "steward": "data-governance-team",
  "certification": "CERTIFIED",
  "trust_status": "ENDORSED",
  "last_updated": "2026-01-15T10:30:00Z"
}
```

### 5. `get_column_metadata`
Gets column definitions and classifications.

**Input**: `data_source_id`, `schema_name`, `table_name`

```json
// Response
[
  {"column_name": "email", "data_type": "VARCHAR", "nullable": true, "classification": ["PII"]}
]
```

### 6. `get_lineage`
Gets upstream and downstream dependencies.

**Input**: `data_source_id`, `schema_name`, `table_name`

```json
// Response
{
  "upstream_tables": ["raw_data.crm_customers"],
  "downstream_tables": ["reporting.customer_summary"],
  "transformation_context": "SQL transformation logic..."
}
```

---

## Usage Examples

### In Slack

**Direct Message**: Send a DM to the bot
```
What data sources are available?
```

**Channel Mention**: @mention the bot
```
@YourBot tell me about the customers table
```

**Thread Conversations**: Bot maintains context
```
You: List tables in analytics schema
Bot: [lists tables]
You: Tell me more about the first one
Bot: [provides details]
```

### Example Queries

| Query Type | Examples |
|------------|----------|
| **Discovery** | "What databases can I query?" |
| **Schemas** | "Show me schemas in Snowflake" |
| **Tables** | "List tables in analytics schema" |
| **Details** | "Describe the customers table" |
| **Ownership** | "Who owns the orders table?" |
| **Columns** | "What columns are in customers?" |
| **PII** | "Which tables contain PII?" |
| **Lineage** | "Where does customer data come from?" |

### Sample Multi-Step Flow

```
User: "I need to find customer data"

Bot: "You have access to 3 data sources:
      1. Production Snowflake
      2. Analytics PostgreSQL
      3. S3 Data Lake"

User: "Show me tables in Snowflake analytics schema"

Bot: "The analytics schema has 3 tables:
      - customers (2.5M rows) - CERTIFIED
      - orders (15M rows)
      - customer_360 (view)"

User: "Tell me about customers"

Bot: "The customers table:
      - Owner: sarah.chen@acme-corp.com
      - Status: CERTIFIED ✓
      - Columns: customer_id, email (PII), first_name (PII)...
      
      ⚠️ Contains PII - ensure proper access"
```

---

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| **Alation 403 Forbidden** | Check API token is valid and not expired |
| **Empty results** | Verify user has access to data sources in Alation |
| **Port 8000 in use** | Kill other process or change port in `alation_server.py` |
| **Slack timeout** | Check VPN/internet connection |
| **AWS credentials error** | Verify `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` |
| **Bedrock access denied** | Enable Claude model access in AWS Bedrock console |

### Test Alation Connection

```bash
# Test API token directly
curl -H "TOKEN: your_token_here" \
     https://your-company.alation.com/integration/v1/datasource/
```

### Check Port Availability

```bash
# Windows
netstat -ano | findstr :8000

# Unix/macOS
lsof -i :8000
```

### Enable Debug Logging

Edit `app/core/config.py`:
```python
logging.basicConfig(level=logging.DEBUG)
```

### "Unknown" Results

When the bot returns "unknown" for metadata fields, this is expected behavior when data is missing in Alation. Solutions:
- Verify data exists in Alation UI
- Check user permissions
- Contact data stewards to update metadata

---

## Development

### Test Individual Components

**Test Alation Adapter:**
```python
from app.services.rag.alation_adapter import AlationAPIAdapter

adapter = AlationAPIAdapter(
    base_url="https://your-company.alation.com",
    api_token="your_token"
)
sources = adapter.list_data_sources()
print(sources)
```

**Test MCP Server:**
```bash
# Terminal 1 - Start server
python app/services/rag/alation_server.py

# Terminal 2 - Test client
python app/services/rag/alation_client.py
```

**Test Metadata Assistant:**
```python
from app.services.rag.engine import metadata_assistant

response = metadata_assistant.answer("What data sources are available?")
print(response.answer)
```

### Adding New MCP Tools

1. Add method to `alation_adapter.py`:
```python
def get_query_history(self, table_name: str) -> List[Dict]:
    # Implementation
    pass
```

2. Add tool to `alation_server.py`:
```python
@mcp.tool()
def get_query_history(table_name: str) -> str:
    """Get query history for a table."""
    return format_response(alation.get_query_history(table_name))
```

3. Add client method to `alation_client.py`:
```python
async def get_query_history(self, table_name: str) -> str:
    return await self._call_tool("get_query_history", {"table_name": table_name})
```

### Production Checklist

- [ ] API token with least privilege permissions
- [ ] Token rotation schedule (quarterly recommended)
- [ ] Structured logging enabled
- [ ] Monitoring for API call rates and errors
- [ ] Alerts for MCP server crashes and API failures
- [ ] Load testing completed
- [ ] Security review completed

---

## Architecture Overview

```
User Message → Slack WebSocket → handlers.py → engine.py → generator.py
                                                              ↓
                                                    AWS Bedrock Claude
                                                              ↓
                                              (Tool Use Loop if needed)
                                                              ↓
                                    alation_client.py → alation_server.py
                                                              ↓
                                                    alation_adapter.py
                                                              ↓
                                                    Alation REST API
                                                              ↓
                                              Response → User in Slack
```

### Key Principles

1. **Alation is the single source of truth** - No secondary catalogs or data dumps
2. **No hallucination** - Missing data returns "unknown", never guessed
3. **Read-only operations** - All API calls are GET requests
4. **Governance-aware** - Highlights PII, certification, ownership

---

## Stopping the Bot

Press `Ctrl+C` in the terminal. The bot will:
1. Stop accepting new Slack events
2. Gracefully shutdown the MCP server
3. Close all connections
