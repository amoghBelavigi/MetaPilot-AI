"""
MCP-based Metadata Assistant - Socket Mode Entry Point

This is the main entry point for running the Slack bot in Socket Mode.
Socket Mode allows the bot to receive events via WebSocket instead of HTTP webhooks.

Usage:
    python -m app.socket_mode
"""

import logging
import socket
import subprocess
import sys
import time

from app.core.config import app, settings
from slack_bolt.adapter.socket_mode import SocketModeHandler
from app.slack.handlers import register_slack_handlers

logger = logging.getLogger(__name__)


def start_mcp_server():
    """Start the MCP (Model Context Protocol) server as a subprocess.

    The MCP server provides Alation metadata tools that the assistant can use
    to fetch enterprise metadata from the Alation catalog.

    Returns:
        subprocess.Popen: The running MCP server process
    """
    logger.info("Starting Alation MCP Server as module")
    return subprocess.Popen([sys.executable, "-m", "app.services.rag.alation_server"])


def wait_for_mcp_server(host="localhost", port=8000, timeout=30, interval=2):
    """Wait for the MCP server to be ready by checking if the port is open.

    Polls the server port until it accepts connections or the timeout is reached.
    This prevents the bot from accepting Slack messages before the MCP server
    is ready to handle tool calls.

    Args:
        host: The MCP server host
        port: The MCP server port
        timeout: Maximum seconds to wait
        interval: Seconds between retry attempts

    Raises:
        RuntimeError: If the server does not start within the timeout
    """
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=5):
                logger.info(f"MCP server is ready on {host}:{port}")
                return
        except (ConnectionRefusedError, socket.timeout, OSError):
            elapsed = int(time.time() - start)
            logger.info(f"Waiting for MCP server on {host}:{port}... ({elapsed}s)")
            time.sleep(interval)

    raise RuntimeError(
        f"MCP server did not become ready within {timeout}s on {host}:{port}. "
        "Check the MCP server logs for errors."
    )


def main():
    """Main entry point for the MCP-based Metadata Assistant."""
    # Start the MCP Server (Alation) as a subprocess
    mcp_process = start_mcp_server()

    try:
        # Wait for MCP server to be ready before accepting Slack messages
        wait_for_mcp_server()

        # Register Slack event handlers
        register_slack_handlers()

        logger.info("Starting MCP-based Metadata Assistant in Socket Mode...")
        handler = SocketModeHandler(app, settings.SLACK_APP_TOKEN)
        handler.start()
    finally:
        # Ensure MCP server is stopped when bot exits
        logger.info("Stopping MCP Server...")
        mcp_process.terminate()
        mcp_process.wait()


if __name__ == "__main__":
    main()
