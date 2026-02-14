"""
Metadata Assistant Engine

Orchestrates the assistant using Alation MCP tools.
No vector store - all metadata comes from live Alation queries.

OPTIMIZED: Uses shared event loop to avoid repeated loop creation.
"""

import logging
import time
from typing import List

from app.services.rag.generator import BedrockGenerator, get_event_loop
from app.services.rag.alation_client import AlationMCPClient
from app.models.schemas import AssistantResponse

logger = logging.getLogger(__name__)

# Error message when MCP tools are unavailable
MCP_UNAVAILABLE_MSG = (
    "I'm unable to connect to the metadata catalog right now, "
    "so I can't look up accurate information. "
    "Please try again in a moment. If the problem persists, "
    "check that the MCP server is running."
)


class MetadataAssistant:
    """Metadata assistant using Alation MCP tools.

    This engine relies entirely on MCP tools for metadata access.
    The LLM uses tools to query Alation for enterprise metadata,
    including table descriptions, ownership, lineage, and governance.
    """

    def __init__(self):
        """Initialize the metadata assistant."""
        self.generator = BedrockGenerator()
        self.mcp_client = AlationMCPClient()

    def answer(self, question: str, history: str = "") -> AssistantResponse:
        """Generate an answer to a user's question.
        
        Args:
            question: The user's question
            history: Optional chat history for context
            
        Returns:
            AssistantResponse containing the answer
        """
        logger.info(f"Answering question: {question}")
        
        # Get available tools from MCP server
        tools = self._get_tools()

        # CRITICAL: Refuse to answer without tools -- prevents hallucination.
        # Without MCP tools Claude has no access to Alation and will make up data.
        if not tools:
            logger.error(
                "No MCP tools available. Refusing to answer to prevent hallucination."
            )
            return AssistantResponse(
                answer=MCP_UNAVAILABLE_MSG,
                sources=[],
                question=question,
            )

        # Generate answer using Claude with Alation metadata tools
        answer_text = self.generator.generate(
            question=question,
            history=history,
            tools=tools,
            tool_executor=self.mcp_client
        )
        
        return AssistantResponse(
            answer=answer_text,
            sources=[],
            question=question
        )
    
    def _get_tools(self, max_retries: int = 2) -> List:
        """Fetch available tools from the MCP server with retry.
        
        Retries on failure to handle transient connection issues
        (e.g. MCP server briefly restarting).
        
        Args:
            max_retries: Number of retry attempts after the first failure
        
        Returns:
            List of available tools, or empty list if all attempts fail
        """
        for attempt in range(max_retries + 1):
            try:
                loop = get_event_loop()
                tools = loop.run_until_complete(self.mcp_client.get_tools())

                if tools:
                    tool_names = [t.name for t in tools]
                    logger.info(
                        f"Loaded {len(tools)} MCP tools: {tool_names}"
                    )
                    return tools

                logger.warning(
                    f"MCP server returned empty tools list "
                    f"(attempt {attempt + 1}/{max_retries + 1})"
                )

            except Exception as e:
                logger.error(
                    f"Failed to fetch MCP tools "
                    f"(attempt {attempt + 1}/{max_retries + 1}): {e}"
                )

            # Brief pause before retry (skip on last attempt)
            if attempt < max_retries:
                time.sleep(1)

        logger.error("All attempts to fetch MCP tools failed")
        return []


# Global singleton instance
metadata_assistant = MetadataAssistant()
