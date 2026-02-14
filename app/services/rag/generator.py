"""
Bedrock LLM Generator

Handles text generation using AWS Bedrock with Claude.
Supports tool use for fetching Alation enterprise metadata.

OPTIMIZED: Supports parallel tool execution for multiple tool calls.
"""

import json
import asyncio
import logging
from typing import List, Optional, Tuple

from app.core.config import bedrock_runtime
from app.services.rag.prompts import SYSTEM_PROMPT

logger = logging.getLogger(__name__)


# Shared event loop for async operations
_event_loop = None

def get_event_loop():
    """Get or create a shared event loop for async operations."""
    global _event_loop
    if _event_loop is None or _event_loop.is_closed():
        _event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_event_loop)
    return _event_loop


class BedrockGenerator:
    """Generate responses using AWS Bedrock Claude model.

    Uses MCP tools to fetch Alation enterprise metadata and answer questions
    about data assets, governance, lineage, and classifications.
    """
    
    def __init__(self, model_id: str = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"):
        """Initialize the generator.
        
        Args:
            model_id: The Bedrock model ID to use
        """
        self.model_id = model_id
        self.client = bedrock_runtime

    # Maximum tool-use rounds (high enough to let Claude work complex queries)
    MAX_TOOL_ROUNDS = 50
    # Round at which we inject a "wrap up" nudge so Claude gives a partial answer
    SOFT_LIMIT_ROUND = 25

    def generate(
        self, 
        question: str, 
        history: str = "", 
        tools: Optional[List] = None, 
        tool_executor = None
    ) -> str:
        """Generate a response to a question.
        
        Args:
            question: The user's question
            history: Optional chat history
            tools: Optional list of MCP tools available
            tool_executor: Optional executor for calling tools
            
        Returns:
            The generated text response
        """
        # Build the prompt
        prompt = SYSTEM_PROMPT.format(history=history, question=question)
        messages = [{"role": "user", "content": prompt}]
        
        # Convert MCP tools to Claude tool format
        if tools:
            system_tools = self._format_tools(tools)
            logger.info(f"Sending {len(system_tools)} tools to Claude")
        else:
            system_tools = []
            logger.warning("No tools provided - Claude will answer without Alation data")

        # Tool use loop - continues until model returns text (not tool call)
        tool_round = 0
        tools_were_used = False
        while True:
            tool_round += 1
            if tool_round > self.MAX_TOOL_ROUNDS:
                logger.error(
                    f"Tool use loop exceeded {self.MAX_TOOL_ROUNDS} rounds, forcing stop"
                )
                # One final call WITHOUT tools so Claude must produce a text answer
                # using whatever data it has gathered so far
                try:
                    logger.info("Making final call without tools to get partial answer")
                    final_body = self._invoke_model(messages, tools=None)
                    final_text = self._extract_text(final_body["content"])
                    if final_text:
                        return final_text
                except Exception:
                    pass
                return "I encountered an issue processing your request (too many tool calls). Please try rephrasing your question."

            # On the FIRST round, force Claude to use a tool (tool_choice="any").
            # This prevents Claude from skipping tools and hallucinating data.
            # On subsequent rounds (after tool results), let Claude decide ("auto").
            if tool_round == 1 and system_tools:
                tool_choice = {"type": "any"}
                logger.info("Forcing tool use on first round (tool_choice=any)")
            elif tool_round == self.SOFT_LIMIT_ROUND:
                # Inject a nudge telling Claude to wrap up with what it has
                tool_choice = {"type": "auto"}
                messages.append({
                    "role": "user",
                    "content": (
                        "You are running low on tool calls. "
                        "Please provide your best answer NOW using the data "
                        "you have already gathered. If you could not find the "
                        "exact resource, explain what you found and what is missing."
                    )
                })
                logger.info(f"[Round {tool_round}] Injected soft-limit nudge")
            else:
                tool_choice = {"type": "auto"}

            response_body = self._invoke_model(
                messages, system_tools, tool_choice=tool_choice
            )
            content = response_body["content"]
            
            # Add assistant response to message history
            messages.append({"role": "assistant", "content": content})

            # Check if model wants to use tools (may be multiple for parallel execution)
            tool_use_blocks = [c for c in content if c["type"] == "tool_use"]
            
            if tool_use_blocks and tool_executor:
                tools_were_used = True
                logger.info(
                    f"[Round {tool_round}] Claude requested {len(tool_use_blocks)} tool(s): "
                    f"{[b['name'] for b in tool_use_blocks]}"
                )
                # Execute tools (in parallel if multiple) and add results to messages
                self._handle_tool_use_parallel(tool_use_blocks, tool_executor, messages)
            else:
                # No tool use - return the text response
                text_block = next(
                    (c for c in content if c["type"] == "text"), 
                    None
                )
                answer = text_block["text"] if text_block else ""

                # SAFEGUARD: If tools were available but Claude never used
                # any of them, the response is almost certainly hallucinated.
                if system_tools and tool_executor and not tools_were_used:
                    logger.error(
                        "HALLUCINATION GUARD: Claude answered without using "
                        "any tools despite tools being available and "
                        "tool_choice=any. Blocking response."
                    )
                    return (
                        "I was unable to look up the requested information "
                        "from the data catalog. Please try again."
                    )

                logger.info(
                    f"Claude responded after {tool_round} round(s), "
                    f"answer length: {len(answer)} chars"
                )
                return answer
    
    @staticmethod
    def _extract_text(content: List[dict]) -> str:
        """Extract text from Claude response content blocks."""
        text_block = next((c for c in content if c["type"] == "text"), None)
        return text_block["text"] if text_block else ""

    def _format_tools(self, tools: List) -> List[dict]:
        """Convert MCP tools to Claude API format."""
        return [
            {
                "name": tool.name,
                "description": tool.description,
                "input_schema": tool.inputSchema
            }
            for tool in tools
        ]
    
    def _invoke_model(
        self,
        messages: List[dict],
        tools: Optional[List[dict]] = None,
        tool_choice: Optional[dict] = None,
    ) -> dict:
        """Invoke the Bedrock model.

        Args:
            messages: Conversation messages
            tools: Tool definitions in Claude format
            tool_choice: Tool selection strategy. Use {"type": "any"} to force
                         tool use, {"type": "auto"} to let the model decide.
        """
        body = {
            "messages": messages,
            "max_tokens": 4096,
            "anthropic_version": "bedrock-2023-05-31"
        }
        if tools:
            body["tools"] = tools
            if tool_choice:
                body["tool_choice"] = tool_choice

        try:
            response = self.client.invoke_model(
                modelId=self.model_id,
                body=json.dumps(body).encode("utf-8")
            )
            return json.loads(response.get("body").read())
        except Exception as e:
            logger.error(f"Bedrock invoke_model failed: {e}")
            raise
    
    def _handle_tool_use_parallel(
        self, 
        tool_use_blocks: List[dict], 
        tool_executor, 
        messages: List[dict]
    ) -> None:
        """Execute multiple tools in parallel and add results to messages.
        
        OPTIMIZED: Uses asyncio.gather for parallel tool execution when
        Claude requests multiple tools at once.
        """
        loop = get_event_loop()
        
        async def execute_single_tool(block: dict) -> Tuple[str, str, str]:
            """Execute a single tool and return (id, name, result)."""
            tool_name = block["name"]
            tool_input = block["input"]
            tool_use_id = block["id"]
            
            try:
                logger.info(f"Executing tool: {tool_name} with args: {tool_input}")
                result = await tool_executor.call_tool(tool_name, tool_input)
                result_str = str(result)
                logger.info(
                    f"Tool {tool_name} succeeded "
                    f"({len(result_str)} chars): {result_str[:300]}..."
                )
                return (tool_use_id, tool_name, result_str)
            except Exception as e:
                logger.error(f"Tool {tool_name} execution FAILED: {e}", exc_info=True)
                return (
                    tool_use_id,
                    tool_name,
                    f"ERROR: Tool '{tool_name}' failed: {str(e)}. "
                    f"Do NOT guess the answer. Tell the user the data "
                    f"could not be retrieved from the catalog."
                )
        
        async def execute_all_tools():
            """Execute all tools in parallel using asyncio.gather."""
            tasks = [execute_single_tool(block) for block in tool_use_blocks]
            return await asyncio.gather(*tasks)
        
        # Execute all tools in parallel
        if len(tool_use_blocks) > 1:
            logger.info(f"Executing {len(tool_use_blocks)} tools in parallel")
        
        results = loop.run_until_complete(execute_all_tools())
        
        # Build tool result content (all results in one message)
        tool_results_content = []
        for tool_use_id, tool_name, tool_result in results:
            tool_results_content.append({
                "type": "tool_result",
                "tool_use_id": tool_use_id,
                "content": tool_result
            })
        
        # Add instruction text at the end
        tool_results_content.append({
            "type": "text",
            "text": (
                "Above are the tool results. "
                "Only show the information that is RELEVANT to the user's question. "
                "Do NOT dump all tool output. Be concise."
            )
        })
        
        # Add all tool results to messages
        messages.append({
            "role": "user",
            "content": tool_results_content
        })
