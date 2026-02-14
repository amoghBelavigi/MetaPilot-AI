"""
Alation MCP Client

Client for communicating with the Alation MCP server via Server-Sent Events (SSE).
Provides convenient access to Alation metadata tools for the assistant.

This client abstracts the MCP communication layer and provides clean,
typed methods for accessing Alation metadata.

ARCHITECTURE: Uses fresh SSE connections per operation to avoid stale-session
issues when bridging sync/async code via run_until_complete().
"""

import logging
from typing import Dict, List, Any, Optional

from mcp import ClientSession
from mcp.client.sse import sse_client

logger = logging.getLogger(__name__)


class AlationMCPClient:
    """
    Client for the Alation MCP server.

    Connects to the MCP server running on localhost:8000 via SSE
    to access Alation metadata tools.

    Each operation (get_tools, call_tool) creates a fresh SSE connection,
    uses it, and closes it cleanly. This avoids stale-session issues that
    occur when persistent SSE connections are held across separate
    run_until_complete() calls.

    Usage:
        client = AlationMCPClient()
        tools = await client.get_tools()
        result = await client.call_tool("list_data_sources", {})
    """

    # Server endpoint (started by socket_mode.py)
    SERVER_URL = "http://localhost:8000/sse"

    def __init__(self, server_url: Optional[str] = None):
        """
        Initialize the Alation MCP client.

        Args:
            server_url: Optional custom server URL (defaults to localhost:8000)
        """
        self.server_url = server_url or self.SERVER_URL
        self.tools_cache = None
        logger.info(f"Initialized AlationMCPClient with server: {self.server_url}")

    async def get_tools(self) -> List[Any]:
        """
        Fetch available tools from the MCP server.

        Tools are cached after first successful fetch for performance.
        Each call creates a fresh SSE connection to ensure reliability.

        Returns:
            List of available tool definitions
        """
        if self.tools_cache:
            return self.tools_cache

        try:
            async with sse_client(self.server_url) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    result = await session.list_tools()
                    self.tools_cache = result.tools
                    logger.info(
                        f"Loaded {len(result.tools)} tools from Alation MCP server"
                    )
                    return result.tools

        except Exception as e:
            logger.error(f"Failed to connect to Alation MCP server: {e}")
            return []

    async def call_tool(self, tool_name: str, tool_args: Optional[Dict] = None) -> str:
        """
        Execute a tool on the MCP server.

        Creates a fresh SSE connection for each call to ensure the
        connection is healthy. This is critical because the SSE transport
        requires an active event loop, which is not guaranteed between
        separate run_until_complete() invocations.

        Args:
            tool_name: Name of the tool to execute
            tool_args: Arguments to pass to the tool

        Returns:
            The tool's text output

        Raises:
            Exception: If tool execution fails after retry
        """
        tool_args = tool_args or {}

        # Try up to 2 times (initial + 1 retry) to handle transient failures
        last_error = None
        for attempt in range(2):
            try:
                async with sse_client(self.server_url) as (read, write):
                    async with ClientSession(read, write) as session:
                        await session.initialize()
                        result = await session.call_tool(tool_name, tool_args)
                        return result.content[0].text

            except Exception as e:
                last_error = e
                if attempt == 0:
                    logger.warning(
                        f"Tool {tool_name} failed (attempt 1), retrying: {e}"
                    )
                else:
                    logger.error(
                        f"Tool {tool_name} failed after retry: {e}"
                    )

        raise last_error

    async def close(self):
        """Clear caches. No persistent connections to close."""
        self.tools_cache = None
        logger.info("AlationMCPClient closed")
