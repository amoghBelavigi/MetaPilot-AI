"""
Slack Event Handlers

This module contains the Slack event handlers for the MCP-based Metadata Assistant.
It handles both @mentions and direct messages.
"""

import re
import logging
from typing import List
from app.core.config import app
from app.services.rag.engine import metadata_assistant

logger = logging.getLogger(__name__)


def handle_question(event: dict, client, say) -> None:
    """Process a user question from Slack.
    
    This function:
    1. Extracts the question from the message
    2. Retrieves thread history for context (if in a thread)
    3. Adds a reaction to show processing
    4. Calls the metadata assistant for an answer
    5. Replies in the thread
    
    Args:
        event: The Slack event dictionary
        client: The Slack WebClient instance
        say: Function to send messages back to Slack
    """
    question = event["text"]
    history = ""
    
    # Retrieve thread context if this message is in a thread
    if "thread_ts" in event:
        history = _get_thread_history(client, event)
    
    # Reply in thread if exists, or start new thread
    thread_ts = event.get("thread_ts") or event.get("ts")
    
    try:
        # React immediately to show we are working
        _add_processing_reaction(client, event)
        
        # Get answer from metadata assistant
        response = metadata_assistant.answer(question, history)
        answer_text = response.answer
        logger.info(f"Got answer from metadata assistant ({len(answer_text)} chars)")

        # Split long responses into multiple messages instead of truncating.
        # Slack can handle ~40k chars per message, but long messages render
        # poorly. We split at ~3800 chars on natural boundaries.
        chunks = _split_message(answer_text, max_chars=3800)
        logger.info(f"Sending response in {len(chunks)} message(s)")

        for i, chunk in enumerate(chunks):
            result = client.chat_postMessage(
                channel=event["channel"],
                text=chunk,
                thread_ts=thread_ts
            )
            logger.info(
                f"Posted message {i + 1}/{len(chunks)} to Slack "
                f"(ok={result.get('ok', '?')}, {len(chunk)} chars)"
            )
        
    except Exception as e:
        logger.error(f"Assistant error: {e}", exc_info=True)
        try:
            client.chat_postMessage(
                channel=event["channel"],
                text="Sorry, I encountered an error while processing your request.",
                thread_ts=thread_ts
            )
        except Exception as e2:
            logger.error(f"Failed to send error message to Slack: {e2}")


def _split_message(text: str, max_chars: int = 3800) -> List[str]:
    """Split a long message into chunks that fit within Slack's display limits.

    Splits on natural boundaries to avoid breaking formatting:
    1. Section headers (*bold* lines)
    2. Code block boundaries (```)
    3. Double newlines (paragraph breaks)
    4. Single newlines (last resort)

    Args:
        text: The full response text
        max_chars: Maximum characters per chunk

    Returns:
        List of message chunks, each under max_chars
    """
    if len(text) <= max_chars:
        return [text]

    chunks = []
    remaining = text

    while remaining:
        if len(remaining) <= max_chars:
            chunks.append(remaining)
            break

        # Find the best split point within the allowed length
        segment = remaining[:max_chars]

        # Never split inside a code block -- find the last complete block boundary
        # Count backtick-fences to determine if we're inside a code block
        fences = [m.start() for m in re.finditer(r'^```', segment, re.MULTILINE)]
        if len(fences) % 2 != 0:
            # Odd number of fences = we'd cut inside a code block.
            # Split just before the last opening fence instead.
            split_at = fences[-1]
            if split_at > 0:
                chunks.append(remaining[:split_at].rstrip())
                remaining = remaining[split_at:]
                continue

        # Try splitting at a section header (*bold* at start of line)
        split_at = _find_last_match(segment, r'\n\*[^*]+\*')
        # Try double newline (paragraph break)
        if split_at == -1:
            split_at = segment.rfind('\n\n')
        # Try single newline
        if split_at == -1:
            split_at = segment.rfind('\n')
        # Hard split as absolute last resort
        if split_at == -1:
            split_at = max_chars

        chunks.append(remaining[:split_at].rstrip())
        remaining = remaining[split_at:].lstrip('\n')

    return [c for c in chunks if c.strip()]


def _find_last_match(text: str, pattern: str) -> int:
    """Find the start position of the last regex match in text.

    Returns:
        Position of the last match, or -1 if not found
    """
    matches = list(re.finditer(pattern, text))
    return matches[-1].start() if matches else -1


def _get_thread_history(client, event: dict) -> str:
    """Retrieve and format thread history for context.
    
    Args:
        client: The Slack WebClient instance
        event: The Slack event dictionary
        
    Returns:
        Formatted string of the last 10 messages in the thread
    """
    try:
        replies = client.conversations_replies(
            channel=event["channel"],
            ts=event["thread_ts"]
        )
        messages = replies.get("messages", [])
        
        # Format history as User/Assistant dialogue
        history_lines = []
        for msg in messages:
            role = "Assistant" if "bot_id" in msg else "User"
            text = msg.get("text", "")
            history_lines.append(f"{role}: {text}")
        
        # Keep only last 10 messages to avoid context overflow
        history = "\n".join(history_lines[-10:])
        logger.info(f"Retrieved {len(messages)} messages from thread.")
        return history
        
    except Exception as e:
        logger.error(f"Error fetching thread history: {e}")
        return ""


def _add_processing_reaction(client, event: dict) -> None:
    """Add an 'eyes' reaction to show the bot is processing.
    
    Args:
        client: The Slack WebClient instance
        event: The Slack event dictionary
    """
    try:
        logger.info(f"Adding reaction 'eyes' to {event.get('ts')}")
        client.reactions_add(
            channel=event["channel"],
            timestamp=event["ts"],
            name="eyes"
        )
    except Exception as e:
        logger.error(f"Failed to add reaction: {e}")


@app.event("app_mention")
def handle_app_mention(event: dict, client, say) -> None:
    """Handle @mentions of the bot in channels."""
    handle_question(event, client, say)


@app.event("message")
def handle_message(event: dict, client, say) -> None:
    """Handle direct messages to the bot."""
    if event.get("channel_type") == "im":
        handle_question(event, client, say)


def register_slack_handlers() -> None:
    """Register all Slack event handlers.
    
    This function is called at startup to ensure handlers are registered.
    """
    logger.info("Slack event handlers registered.")
