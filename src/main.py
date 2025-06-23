#!/usr/bin/env python3
"""
OMOP-NLP-MCP Server Entry Point - Simplified for subprocess execution
"""

import sys
import logging
import asyncio
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from server import create_omop_server
from config.settings import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stderr)
    ]
)

logger = logging.getLogger(__name__)


async def main():
    """Main entry point for the OMOP MCP server."""
    try:
        logger.info("OMOP MCP Server starting via stdio...")
        logger.info(f"LLM Provider: {settings.llm_provider}")
        logger.info(f"Database: {settings.database_endpoint}/{settings.database_name}")
        
        # Create the server
        server = create_omop_server()
        
        # Run the server with stdio transport
        await server.run(transport="stdio")
        
    except KeyboardInterrupt:
        logger.info("Server shutdown requested")
    except Exception as error:
        logger.error(f"Server error: {error}")
        raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server stopped")
    except Exception as error:
        logger.error(f"Failed to start server: {error}")
        sys.exit(1)