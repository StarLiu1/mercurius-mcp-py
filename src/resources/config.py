from typing import Dict, Any
from config.settings import settings


async def config_resource() -> Dict[str, Any]:
    """Get current configuration resource."""
    return {
        "llm_provider": settings.llm_provider,
        "version": "1.0.0",
        "capabilities": ["nl-to-cql", "vsac-integration", "omop-mapping", "sql-generation"]
    }