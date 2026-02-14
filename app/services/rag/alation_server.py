"""
Alation MCP Server

Model Context Protocol (MCP) server that exposes Alation metadata to LLMs.
Provides clean, stable tools for querying enterprise metadata without
exposing Alation API complexity.

Core Principles:
- Alation is the single source of truth
- Read-only operations
- No hallucination - explicit "unknown" for missing data
- Graceful error handling
- No data persistence (in-memory cache only)

Tools Provided:
  Search:  search_table, search_schema, search_columns
  Browse:  list_data_sources, list_schemas, list_tables
  Detail:  get_table_metadata, get_column_metadata, get_lineage
"""

import os
import logging

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

from app.services.rag.alation_adapter import AlationAPIAdapter

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastMCP service
mcp = FastMCP("Alation Metadata")

# Initialize Alation API adapter
alation = AlationAPIAdapter(
    base_url=os.getenv("ALATION_BASE_URL"),
    api_token=os.getenv("ALATION_API_TOKEN"),
    user_id=os.getenv("ALATION_USER_ID"),
    cache_enabled=True
)


def _err(msg: str) -> str:
    """Format an error message for tool output."""
    return f"Error: {msg}"


def _trunc(text: str, limit: int = 120) -> str:
    """Truncate text to first sentence or limit, whichever is shorter."""
    if not text or text == "unknown":
        return text or "unknown"
    text = str(text)
    if '. ' in text:
        text = text[:text.index('. ') + 1]
    if len(text) > limit:
        text = text[:limit - 3] + "..."
    return text


# =========================================================================
# Formatters -- pre-format tool output so Claude passes it through as-is.
# Every Alation name/identifier is wrapped in backticks for Slack rendering.
# =========================================================================

def fmt_data_sources(data_sources: list) -> str:
    """Format data sources as a readable list with backticked names."""
    lines = [f"Found {len(data_sources)} data source(s):\n"]
    for ds in data_sources:
        name = ds.get('name', 'unknown')
        ds_id = ds.get('data_source_id', '?')
        dbtype = ds.get('type', 'unknown')
        desc = _trunc(ds.get('description', ''))
        lines.append(f"• `{name}` (ID: {ds_id}) — Type: `{dbtype}` — {desc}")
    return "\n".join(lines)


def fmt_schemas(schemas: list, ds_id: int) -> str:
    """Format schemas as a readable list with backticked names."""
    lines = [f"Found {len(schemas)} schema(s) in data source {ds_id}:\n"]
    for s in schemas:
        name = s.get('schema_name', 'unknown')
        desc = _trunc(s.get('schema_description', ''))
        lines.append(f"• `{name}` — {desc}")
    return "\n".join(lines)


def fmt_tables(tables: list, schema_name: str) -> str:
    """Format tables as a bullet list with backticked names."""
    lines = [f"Found {len(tables)} table(s) in `{schema_name}`:\n"]
    for t in tables:
        name = t.get('table_name', 'unknown')
        ttype = t.get('table_type', '')
        suffix = f" ({ttype})" if ttype and ttype != "unknown" else ""
        lines.append(f"• `{name}`{suffix}")
    return "\n".join(lines)


def fmt_table_detail(meta: dict) -> str:
    """Format single table metadata as bullet points with backticked values."""
    name = meta.get('table_name', 'unknown')
    lines = [
        f"*Table:* `{name}`\n",
        f"• *Description:* {meta.get('table_description', 'unknown')}",
        f"• *Owner:* `{meta.get('owner', 'unknown')}`",
        f"• *Steward:* `{meta.get('steward', 'unknown')}`",
        f"• *Certification:* {meta.get('certification', 'unknown')}",
        f"• *Trust Status:* {meta.get('trust_status', 'unknown')}",
        f"• *Last Updated:* {meta.get('last_updated', 'unknown')}",
    ]
    return "\n".join(lines)


def fmt_columns(columns: list, context: str = "") -> str:
    """Format column data as vertical cards -- one entry per column.

    Each column gets its own line with backticked name and type,
    plus a description on the next line. This renders cleanly in
    Slack on all screen sizes.
    """
    if not columns:
        return "No columns found."

    lines = [context, ""] if context else []
    for col in columns:
        name = col.get('column_name', 'unknown')
        dtype = col.get('data_type', 'unknown')
        desc = _trunc(col.get('description', ''), 150)

        entry = f"• `{name}` — `{dtype}`"
        if desc and desc != "unknown":
            entry += f"\n  _{desc}_"
        lines.append(entry)

    lines.append(f"\n_{len(columns)} column(s) total_")
    return "\n".join(lines)


def fmt_lineage(lineage: dict, table_name: str) -> str:
    """Format lineage as readable list with backticked table names."""
    lines = [f"*Lineage for* `{table_name}`:\n"]

    upstream = lineage.get('upstream_tables', 'unknown')
    lines.append("*Upstream Tables:*")
    if isinstance(upstream, list):
        for t in upstream:
            lines.append(f"  • `{t}`")
    else:
        lines.append(f"  {upstream}")

    downstream = lineage.get('downstream_tables', 'unknown')
    lines.append("\n*Downstream Tables:*")
    if isinstance(downstream, list):
        for t in downstream:
            lines.append(f"  • `{t}`")
    else:
        lines.append(f"  {downstream}")

    ctx = lineage.get('transformation_context', 'unknown')
    lines.append(f"\n*Transformation Context:* {_trunc(ctx, 200)}")
    return "\n".join(lines)


def fmt_search_tables(results: list) -> str:
    """Format search_table results with backticked names."""
    lines = [f"Found {len(results)} table(s):\n"]
    for t in results:
        name = t.get('table_name', 'unknown')
        ds_id = t.get('data_source_id', '?')
        schema = t.get('schema_name', 'unknown')
        desc = _trunc(t.get('description', ''))

        entry = f"• `{name}` — Schema: `{schema}` (DS ID: {ds_id})"
        if desc and desc != "unknown":
            entry += f"\n  _{desc}_"
        lines.append(entry)
    return "\n".join(lines)


def fmt_search_schemas(results: list) -> str:
    """Format search_schema results with backticked names."""
    lines = [f"Found {len(results)} schema(s):\n"]
    for s in results:
        name = s.get('schema_name', 'unknown')
        ds_name = s.get('data_source_name', 'unknown')
        ds_id = s.get('data_source_id', '?')
        desc = _trunc(s.get('description', ''))
        lines.append(
            f"• `{name}` in `{ds_name}` (DS ID: {ds_id}) — {desc}"
        )
    return "\n".join(lines)


# =============================================================================
# Tool 1: list_data_sources
# =============================================================================

@mcp.tool()
def list_data_sources() -> str:
    """
    List all available data sources in Alation.

    Returns structured information about each data source including:
    - data_source_id: Unique identifier
    - name: Display name
    - type: Database type (snowflake, redshift, postgresql, etc.)
    - description: Human-readable description

    Returns:
        JSON formatted list of data sources
    """
    try:
        logger.info("Tool invoked: list_data_sources")
        data_sources = alation.list_data_sources()

        if not data_sources:
            return _err("No data sources found or access denied")

        return fmt_data_sources(data_sources)

    except Exception as e:
        logger.error(f"list_data_sources failed: {e}")
        return _err(f"Failed to retrieve data sources: {str(e)}")


# =============================================================================
# Tool 2: list_schemas
# =============================================================================

@mcp.tool()
def list_schemas(data_source_id: int) -> str:
    """
    List all schemas in a specific data source.

    Args:
        data_source_id: The Alation data source ID (from list_data_sources)

    Returns:
        JSON formatted list of schemas with:
        - schema_name: Name of the schema
        - schema_description: Description if available, "unknown" otherwise

    Example:
        list_schemas(data_source_id=123)
    """
    try:
        logger.info(f"Tool invoked: list_schemas(data_source_id={data_source_id})")

        if not isinstance(data_source_id, int):
            return _err("data_source_id must be an integer")

        schemas = alation.list_schemas(data_source_id)

        if not schemas:
            return _err(
                f"No schemas found for data source {data_source_id} or access denied"
            )

        return fmt_schemas(schemas, data_source_id)

    except Exception as e:
        logger.error(f"list_schemas failed: {e}")
        return _err(f"Failed to retrieve schemas: {str(e)}")


# =============================================================================
# Tool 3: list_tables
# =============================================================================

@mcp.tool()
def list_tables(data_source_id: int, schema_name: str) -> str:
    """
    List all tables in a specific schema.

    Args:
        data_source_id: The Alation data source ID
        schema_name: Name of the schema

    Returns:
        JSON formatted list of tables with:
        - table_name: Name of the table
        - table_type: Type (TABLE, VIEW, etc.)
        - row_count: Number of rows if available, "unknown" otherwise
        - popularity: Usage signals if available, "unknown" otherwise

    Example:
        list_tables(data_source_id=123, schema_name="public")
    """
    try:
        logger.info(
            f"Tool invoked: list_tables(data_source_id={data_source_id}, "
            f"schema_name={schema_name})"
        )

        if not isinstance(data_source_id, int):
            return _err("data_source_id must be an integer")

        if not schema_name:
            return _err("schema_name is required")

        tables = alation.list_tables(data_source_id, schema_name)

        if not tables:
            return _err(
                f"No tables found in `{schema_name}` or access denied"
            )

        return fmt_tables(tables, schema_name)

    except Exception as e:
        logger.error(f"list_tables failed: {e}")
        return _err(f"Failed to retrieve tables: {str(e)}")


# =============================================================================
# Tool 4: get_table_metadata
# =============================================================================

@mcp.tool()
def get_table_metadata(
    data_source_id: int,
    schema_name: str,
    table_name: str
) -> str:
    """
    Get detailed metadata for a specific table.

    Provides comprehensive table information including ownership,
    governance status, and usage context.

    Args:
        data_source_id: The Alation data source ID
        schema_name: Name of the schema
        table_name: Name of the table

    Returns:
        JSON formatted metadata including:
        - table_name: Name of the table
        - table_description: Business description or "unknown"
        - owner: Table owner or "unknown"
        - steward: Data steward or "unknown"
        - certification: Trust certification status or "unknown"
        - trust_status: Endorsement status or "unknown"
        - last_updated: Last modification timestamp or "unknown"
        - sample_queries: Example queries or "unknown"

    Example:
        get_table_metadata(
            data_source_id=123,
            schema_name="public",
            table_name="customers"
        )
    """
    try:
        logger.info(
            f"Tool invoked: get_table_metadata(data_source_id={data_source_id}, "
            f"schema_name={schema_name}, table_name={table_name})"
        )

        if not isinstance(data_source_id, int):
            return _err("data_source_id must be an integer")

        if not schema_name or not table_name:
            return _err("schema_name and table_name are required")

        metadata = alation.get_table_metadata(data_source_id, schema_name, table_name)

        if not metadata:
            return _err(
                f"Table `{schema_name}`.`{table_name}` not found or access denied"
            )

        return fmt_table_detail(metadata)

    except Exception as e:
        logger.error(f"get_table_metadata failed: {e}")
        return _err(f"Failed to retrieve table metadata: {str(e)}")


# =============================================================================
# Tool 5: get_column_metadata
# =============================================================================

@mcp.tool()
def get_column_metadata(
    data_source_id: int,
    schema_name: str,
    table_name: str
) -> str:
    """
    Get column definitions and metadata for a table.

    Provides detailed column information including data types,
    descriptions, and data classifications.

    Args:
        data_source_id: The Alation data source ID
        schema_name: Name of the schema
        table_name: Name of the table

    Returns:
        JSON formatted list of columns with:
        - column_name: Name of the column
        - data_type: SQL data type
        - description: Column description or "unknown"
        - nullable: Whether column accepts NULL values or "unknown"
        - classification: Data classification tags (PII, PHI, etc.) or "unknown"

    Example:
        get_column_metadata(
            data_source_id=123,
            schema_name="public",
            table_name="customers"
        )
    """
    try:
        logger.info(
            f"Tool invoked: get_column_metadata(data_source_id={data_source_id}, "
            f"schema_name={schema_name}, table_name={table_name})"
        )

        if not isinstance(data_source_id, int):
            return _err("data_source_id must be an integer")

        if not schema_name or not table_name:
            return _err("schema_name and table_name are required")

        columns = alation.get_column_metadata(data_source_id, schema_name, table_name)

        if not columns:
            return _err(
                f"No columns found for `{schema_name}`.`{table_name}`. "
                f"The table may not exist in this schema, or access is denied. "
                f"Ask the user to verify the exact schema and table name."
            )

        return fmt_columns(
            columns,
            context=f"Columns for `{schema_name}`.`{table_name}`:"
        )

    except Exception as e:
        logger.error(f"get_column_metadata failed: {e}")
        return _err(f"Failed to retrieve column metadata: {str(e)}")


# =============================================================================
# Tool 6: get_lineage
# =============================================================================

@mcp.tool()
def get_lineage(
    data_source_id: int,
    schema_name: str,
    table_name: str
) -> str:
    """
    Get data lineage for a table.

    Provides upstream and downstream dependencies to understand
    data flow and transformation context.

    Args:
        data_source_id: The Alation data source ID
        schema_name: Name of the schema
        table_name: Name of the table

    Returns:
        JSON formatted lineage information with:
        - upstream_tables: List of source tables or "unknown"
        - downstream_tables: List of dependent tables or "unknown"
        - transformation_context: SQL or transformation logic if available, "unknown" otherwise

    Note:
        Lineage data may not be available for all tables. Missing lineage
        is explicitly marked as "unknown" rather than inferred.

    Example:
        get_lineage(
            data_source_id=123,
            schema_name="public",
            table_name="customer_summary"
        )
    """
    try:
        logger.info(
            f"Tool invoked: get_lineage(data_source_id={data_source_id}, "
            f"schema_name={schema_name}, table_name={table_name})"
        )

        if not isinstance(data_source_id, int):
            return _err("data_source_id must be an integer")

        if not schema_name or not table_name:
            return _err("schema_name and table_name are required")

        lineage = alation.get_lineage(data_source_id, schema_name, table_name)

        if not lineage:
            return _err(
                f"Lineage not available for `{schema_name}`.`{table_name}`"
            )

        return fmt_lineage(lineage, table_name)

    except Exception as e:
        logger.error(f"get_lineage failed: {e}")
        return _err(f"Failed to retrieve lineage: {str(e)}")


# =============================================================================
# Tool 7: search_table
# =============================================================================

@mcp.tool()
def search_table(table_name: str) -> str:
    """
    Search for a table by name across ALL data sources.

    This is the FASTEST way to find a table when you know the table name
    but don't know which data source or schema it belongs to.
    Use this BEFORE browsing data sources manually.

    Args:
        table_name: The table name to search for (e.g. "ACCT_CONFORMED_SPEND_1ST_PARTY")

    Returns:
        JSON list of matching tables with data_source_id, schema_name,
        table_id, description, and URL for each match.

    Example:
        search_table(table_name="FCT_STORE_TRANSACTION_ITEM")
    """
    try:
        logger.info(f"Tool invoked: search_table(table_name={table_name})")
        results = alation.search_table(table_name)

        if not results:
            return _err(
                f"No tables matching `{table_name}` found across any data source. "
                f"The table may exist under a different name, or the API token "
                f"may not have access to it. Ask the user for the exact table "
                f"name as it appears in Alation, the data source name, or the "
                f"Alation URL (e.g. /table/12345/)."
            )

        return fmt_search_tables(results)

    except Exception as e:
        logger.error(f"search_table failed: {e}")
        return _err(f"Failed to search for table: {str(e)}")


# =============================================================================
# Tool 8: search_schema
# =============================================================================

@mcp.tool()
def search_schema(keyword: str) -> str:
    """
    Search for schemas matching a keyword across ALL data sources.

    Use this when you know a database/schema name (e.g. "PS_PRD_01_USERFP")
    but don't know which Alation data source it belongs to.

    Args:
        keyword: Keyword to search for in schema names (case-insensitive)

    Returns:
        JSON list of matching schemas with data_source_id, data_source_name,
        and description for each match.

    Example:
        search_schema(keyword="USERFP")
    """
    try:
        logger.info(f"Tool invoked: search_schema(keyword={keyword})")
        results = alation.search_schema(keyword)

        if not results:
            return _err(
                f"No schemas matching `{keyword}` found across any data source. "
                f"Ask the user for the exact schema or database name, or which "
                f"data source it belongs to."
            )

        return fmt_search_schemas(results)

    except Exception as e:
        logger.error(f"search_schema failed: {e}")
        return _err(f"Failed to search for schema: {str(e)}")


# =============================================================================
# Tool 9: search_columns
# =============================================================================

@mcp.tool()
def search_columns(column_name: str, table_name: str = "") -> str:
    """
    Search for columns by name across the catalog.

    Useful when the user asks about a specific column and you need
    to find its definition, data type, or which tables contain it.

    Args:
        column_name: The column name to search for (e.g. "TXN_DTTM")
        table_name: Optional table name to filter results (e.g. "ACCT_CONFORMED_SPEND")

    Returns:
        JSON list of matching columns with data_type, description,
        and the table they belong to.

    Example:
        search_columns(column_name="TIMESTAMP", table_name="ACCT_CONFORMED_SPEND")
    """
    try:
        logger.info(
            f"Tool invoked: search_columns(column_name={column_name}, "
            f"table_name={table_name})"
        )
        results = alation.search_columns(
            column_name, table_name if table_name else None
        )

        if not results:
            return _err(f"No columns matching `{column_name}` found")

        return fmt_columns(
            results,
            context=f"Columns matching `{column_name}`:"
        )

    except Exception as e:
        logger.error(f"search_columns failed: {e}")
        return _err(f"Failed to search for columns: {str(e)}")


# =============================================================================
# Server Entry Point
# =============================================================================

if __name__ == "__main__":
    import uvicorn

    logger.info("Starting Alation MCP Server on port 8000")
    logger.info(f"Connected to Alation instance: {os.getenv('ALATION_BASE_URL')}")

    # Run the SSE server on port 8000
    # This is started automatically by socket_mode.py
    uvicorn.run(mcp.sse_app, host="0.0.0.0", port=8000)
