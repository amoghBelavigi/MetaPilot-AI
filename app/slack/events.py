"""
Slack Events Router (HTTP Mode)

This module provides FastAPI routes for handling Slack events when
running in HTTP webhook mode (as opposed to Socket Mode).

Note: Socket Mode (socket_mode.py) is preferred for local development.
"""

from fastapi import APIRouter, Request
from app.services.rag.engine import metadata_assistant
from app.core.config import slack_client

router = APIRouter()


@router.post("/slack/events")
async def slack_events(req: Request) -> dict:
    """Handle incoming Slack events via HTTP webhook.
    
    This endpoint handles:
    - URL verification challenges from Slack
    - App mention events
    
    Args:
        req: The incoming FastAPI request
        
    Returns:
        Response dict with challenge or ok status
    """
    payload = await req.json()

    # Handle Slack URL verification challenge
    if payload.get("type") == "url_verification":
        return {"challenge": payload["challenge"]}

    event = payload.get("event", {})
    
    def say(text: str) -> None:
        """Send a message to the channel where the event occurred."""
        slack_client.chat_postMessage(channel=event["channel"], text=text)

    # Handle @mentions
    if event.get("type") == "app_mention":
        question = event["text"]
        try:
            response = metadata_assistant.answer(question)
            say(response.answer)
        except Exception:
            say("❌ Error processing your request.")

    return {"ok": True}
