"""
Application Configuration

Centralizes all configuration, credentials, and client initialization.
Loads settings from environment variables.
"""

import os
import ssl
import logging

import boto3
from botocore.config import Config as BotoConfig
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_bolt import App

# =============================================================================
# Logging Configuration
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# Environment Variables
# =============================================================================

load_dotenv()


class Settings:
    """Application settings loaded from environment variables.
    
    All configuration is centralized here for easy access across the app.
    """
    
    # Slack credentials
    SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
    SLACK_APP_TOKEN = os.getenv("SLACK_APP_TOKEN")  # For Socket Mode
    SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET")

    # AWS credentials (for Bedrock)
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-west-2")

    # Alation credentials (for metadata catalog)
    ALATION_BASE_URL = os.getenv("ALATION_BASE_URL")
    ALATION_API_TOKEN = os.getenv("ALATION_API_TOKEN")
    ALATION_USER_ID = os.getenv("ALATION_USER_ID")


# Global settings instance
settings = Settings()

# =============================================================================
# AWS Bedrock Client
# =============================================================================

# SSL context for Slack client (handles certificate issues)
ssl_context = ssl._create_unverified_context()

# Bedrock runtime client for LLM
# Increased read_timeout (default 60s) to handle long tool-use conversations
bedrock_config = BotoConfig(
    read_timeout=120,
    connect_timeout=10,
    retries={"max_attempts": 2, "mode": "adaptive"},
)
bedrock_runtime = boto3.client(
    service_name="bedrock-runtime",
    region_name=settings.AWS_REGION,
    aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
    config=bedrock_config,
)

# =============================================================================
# Slack Client & App
# =============================================================================

# Slack Web API client
slack_client = WebClient(
    token=settings.SLACK_BOT_TOKEN,
    ssl=ssl_context
)

# Slack Bolt app instance (used for event handling)
app = App(
    client=slack_client, 
    signing_secret=settings.SLACK_SIGNING_SECRET
)
