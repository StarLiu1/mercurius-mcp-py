import logging
from typing import Dict, Any, List, Optional
from services.vsac_services import vsac_service
from config.settings import settings

logger = logging.getLogger(__name__)


async def fetch_multiple_vsac_tool(
    value_set_ids: List[str],
    username: Optional[str] = None,
    password: Optional[str] = None
) -> Dict[str, Any]:
    """
    Fetch multiple ValueSets from VSAC.
    
    Args:
        value_set_ids: List of ValueSet OIDs to fetch
        username: VSAC username (optional, uses env var if not provided)
        password: VSAC password (optional, uses env var if not provided)
        
    Returns:
        Dict with results and summary information
    """
    try:
        # Use environment variables as defaults
        username = username or settings.vsac_username
        password = password or settings.vsac_password
        
        # Validate credentials
        if not username or not password:
            return {
                "error": "VSAC credentials are required",
                "message": "Set VSAC_USERNAME and VSAC_PASSWORD environment variables, or pass them as parameters",
                "environment_variables": {
                    "VSAC_USERNAME": "SET" if settings.vsac_username else "NOT SET",
                    "VSAC_PASSWORD": "SET" if settings.vsac_password else "NOT SET"
                },
                "value_set_ids": value_set_ids
            }
        
        logger.info(f"Batch fetching {len(value_set_ids)} VSAC value sets")
        
        results = await vsac_service.retrieve_multiple_value_sets(
            value_set_ids,
            username,
            password
        )
        
        # Process results to match expected format
        processed_results = {}
        for oid, value_set in results.items():
            processed_results[oid] = {
                "metadata": value_set.metadata.model_dump(),
                "concepts": [concept.model_dump() for concept in value_set.concepts]
            }
        
        successful_retrievals = len([r for r in processed_results.values() 
                                   if r["concepts"] and len(r["concepts"]) > 0])
        total_concepts = sum(len(r["concepts"]) for r in processed_results.values())
        
        summary = {
            "total_requested": len(value_set_ids),
            "successful_retrievals": successful_retrievals,
            "total_concepts": total_concepts,
            "results": processed_results,
            "credentials_used": {
                "username": username,
                "from_environment": {
                    "username": username == settings.vsac_username,
                    "password": password == settings.vsac_password
                }
            },
            "retrieved_at": "2025-06-23T00:00:00Z"  # You might want to use actual timestamp
        }
        
        return summary
        
    except Exception as error:
        logger.error(f"Error in fetch_multiple_vsac_tool: {error}")
        
        return {
            "error": str(error),
            "value_set_ids": value_set_ids,
            "status": "batch_failed",
            "credentials_checked": {
                "username": "PROVIDED" if username else "MISSING",
                "password": "PROVIDED" if password else "MISSING"
            },
            "timestamp": "2025-06-23T00:00:00Z"
        }


async def vsac_cache_status_tool() -> Dict[str, Any]:
    """
    Get VSAC cache status.
    
    Returns:
        Dict with cache information and environment variables
    """
    stats = vsac_service.get_cache_stats()
    
    return {
        "cache_size": stats["size"],
        "cached_value_sets": stats["keys"],
        "environment_variables": {
            "VSAC_USERNAME": "SET" if settings.vsac_username else "NOT SET",
            "VSAC_PASSWORD": "SET" if settings.vsac_password else "NOT SET"
        },
        "status": "cache_info"
    }