# Architecture

> Technical architecture, design decisions, and system internals for MetaPilot-AI

## System Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                        Slack User                           │
└──────────────────────────┬──────────────────────────────────┘
                           │ Question
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    Slack Bot (Socket Mode)                   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  handlers.py - Extract question + thread context     │   │
│  └──────────────────┬───────────────────────────────────┘   │
└─────────────────────┼───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                   Metadata Assistant                        │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  engine.py - Orchestrate tool discovery + generation │   │
│  └──────────────────┬───────────────────────────────────┘   │
└─────────────────────┼───────────────────────────────────────┘
                      │
         ┌────────────┴────────────┐
         │                         │
         ▼                         ▼
┌──────────────────┐    ┌──────────────────────────┐
│ Get MCP Tools    │    │  generator.py            │
│ (alation_client) │    │  Invoke Claude + Tools   │
└──────────────────┘    └──────────┬───────────────┘
                                   │
                                   ▼
                        ┌─────────────────────┐
                        │   AWS Bedrock       │
                        │   Claude 4.5 Sonnet │
                        └──────────┬──────────┘
                                   │
                    ┌──────────────┴───────────────┐
                    │                              │
              Tool Request                    Final Answer
                    │                              │
                    ▼                              ▼
      ┌──────────────────────────┐         ┌────────────┐
      │  Alation MCP Server      │         │   Return   │
      │  (alation_server.py)     │         │  to User   │
      │  9 tools, pre-formatted  │         └────────────┘
      └──────────┬───────────────┘
                 │
                 ▼
      ┌──────────────────────────┐
      │  Alation API Adapter     │
      │  (alation_adapter.py)    │
      │  - Auth + token exchange │
      │  - Caching (5-min TTL)   │
      │  - Retry logic           │
      │  - HTML stripping        │
      └──────────┬───────────────┘
                 │ HTTPS + API Token
                 ▼
      ┌──────────────────────────┐
      │   Alation REST API       │
      │   (v1 + v2 endpoints)    │
      └──────────────────────────┘
```

## Process Lifecycle

1. **Startup**: MCP server starts on port 8000 → Slack Socket Mode connects
2. **Message**: User message → Handler extracts question + thread context → Metadata Assistant invoked
3. **Tool Loop**: Claude requests tool → MCP client calls server via SSE → Alation API queried → Result returned → Repeat until answer ready
4. **Soft Limit**: At round 25, Claude is nudged to wrap up with what it has
5. **Hard Limit**: At round 50, one final call without tools forces a summary
6. **Response**: Answer split into multiple Slack messages if long, posted to thread
7. **Shutdown**: MCP subprocess terminated → Connections closed

---

## Design Decisions

### Why Tool-Augmented Generation (not RAG)?
- No vector store, no embeddings, no pre-ingestion
- LLM actively decides which API calls to make (agentic retrieval)
- Every answer comes from real-time Alation data
- Simpler architecture, always-fresh data

### Why Alation as Single Source of Truth?
- Enterprise-grade metadata catalog with governance
- Rich metadata: ownership, lineage, certification, classifications
- Access control enforced at the source
- No data duplication or drift

### Why MCP (Model Context Protocol)?
- Standardized protocol for tool execution
- Dynamic tool discovery
- Clean separation between bot and data layer
- Easy to add new tools without modifying core logic

### Why Socket Mode?
- No public endpoint required
- Works behind firewalls and VPN
- Real-time bidirectional communication
- Suitable for enterprise environments

### Why Search-First Strategy?
- `search_table`, `search_schema`, `search_columns` find resources instantly
- Avoids exhaustive browsing through data sources one by one
- Reduces tool calls from 10+ to 1-2 for most queries

---

## Error Handling (4 Layers)

| Layer | Component | Strategy |
|-------|-----------|----------|
| **1** | Alation Adapter | HTTP retries, 403 → re-authenticate, 404 → return None |
| **2** | MCP Server | None from adapter → descriptive error with user guidance |
| **3** | Generator | Tool errors passed to Claude in context. Claude explains naturally |
| **4** | Slack Handler | Unhandled exceptions → friendly error message. Full error logged |

---

## Security

| Aspect | Implementation |
|--------|----------------|
| **API Token** | Refresh Token in `.env`, auto-exchanged for API Access Token |
| **Permissions** | Token inherits user permissions (least privilege) |
| **Operations** | All Alation API calls are read-only GET requests |
| **Access Control** | Enforced by Alation — bot only sees what user can access |
| **Audit Trail** | All API calls and tool executions logged |

---

## Performance

### Caching
- **TTL**: 5 minutes (configurable in `alation_adapter.py`)
- **Scope**: Process-level in-memory cache + dedicated table ID cache
- **Impact**: Reduces API load significantly for repeated queries

### Tool Execution
- Parallel tool execution via `asyncio.gather` when Claude requests multiple tools
- Fresh SSE connections per operation to avoid stale sessions

### Response Delivery
- Long responses split into multiple Slack messages at natural boundaries
- Never cuts inside code blocks or mid-sentence
