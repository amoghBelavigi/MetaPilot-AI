"""
Pydantic Schemas

Data models for the metadata assistant.
"""

from typing import List

from pydantic import BaseModel, Field


class AssistantResponse(BaseModel):
    """Response from the metadata assistant.

    Attributes:
        answer: The generated answer text
        sources: List of source references (reserved for future use)
        question: The original question
    """
    answer: str
    sources: List[str] = Field(default_factory=list)
    question: str
