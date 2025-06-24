import logging
from typing import Dict, Any, List, Optional
from services.vsac_services import vsac_service
from config.settings import settings
from datetime import datetime

logger = logging.getLogger()


async def fetch_multiple_vsac_tool(
    value_set_ids: List[str],
    username: Optional[str] = None,
    password: Optional[str] = None
) -> Dict[str, Any]:
    """
    Fetch multiple ValueSets from VSAC - matches JavaScript functionality.
    
    Args:
        value_set_ids: List of ValueSet OIDs to fetch
        username: VSAC username (optional, uses env var if not provided)
        password: VSAC password (optional, uses env var if not provided)
        
    Returns:
        Dict with results and summary information
    """
    try:
        # Use environment variables as defaults (like JavaScript)
        username = username or settings.vsac_username
        password = password or settings.vsac_password
        
        # Validate credentials (like JavaScript)
        if not username or not password:
            return {
                "error": "VSAC credentials are required",
                "message": "Set VSAC_USERNAME and VSAC_PASSWORD environment variables, or pass them as parameters",
                "environmentVariables": {
                    "VSAC_USERNAME": "SET" if settings.vsac_username else "NOT SET",
                    "VSAC_PASSWORD": "SET" if settings.vsac_password else "NOT SET"
                },
                "valueSetIds": value_set_ids
            }
        
        logger.info(f"Batch fetching {len(value_set_ids)} VSAC value sets")
        
        results = await vsac_service.retrieve_multiple_value_sets(
            value_set_ids,
            username,
            password
        )
        
        # Process results to match JavaScript format exactly
        processed_results = {}
        for oid, value_set in results.items():
            # Convert to dictionary format (like JavaScript)
            metadata_dict = {}
            concepts_list = []
            
            if hasattr(value_set, 'metadata') and value_set.metadata:
                metadata_dict = {
                    "id": value_set.metadata.id,
                    "displayName": value_set.metadata.display_name,
                    "version": value_set.metadata.version,
                    "source": value_set.metadata.source,
                    "type": value_set.metadata.type,
                    "binding": value_set.metadata.binding,
                    "status": value_set.metadata.status,
                    "revisionDate": value_set.metadata.revision_date,
                    "description": value_set.metadata.description,
                    "clinicalFocus": value_set.metadata.clinical_focus,
                    "dataElementScope": value_set.metadata.data_element_scope,
                    "inclusionCriteria": value_set.metadata.inclusion_criteria,
                    "exclusionCriteria": value_set.metadata.exclusion_criteria
                }
            
            if hasattr(value_set, 'concepts') and value_set.concepts:
                concepts_list = [
                    {
                        "code": concept.code,
                        "codeSystem": concept.code_system,
                        "codeSystemName": concept.code_system_name,
                        "codeSystemVersion": concept.code_system_version,
                        "displayName": concept.display_name
                    }
                    for concept in value_set.concepts
                ]
            
            processed_results[oid] = {
                "metadata": metadata_dict,
                "concepts": concepts_list
            }
        
        # Calculate summary statistics (like JavaScript)
        successful_retrievals = len([r for r in processed_results.values() 
                                   if r["concepts"] and len(r["concepts"]) > 0])
        total_concepts = sum(len(r["concepts"]) for r in processed_results.values())
        
        # Format response exactly like JavaScript version
        summary = {
            "totalRequested": len(value_set_ids),
            "successfulRetrievals": successful_retrievals,
            "totalConcepts": total_concepts,
            "results": processed_results,
            "credentialsUsed": {
                "username": username,
                "fromEnvironment": {
                    "username": username == settings.vsac_username,
                    "password": password == settings.vsac_password
                }
            },
            "retrievedAt": datetime.now().isoformat()
        }
        
        return summary
        
    except Exception as error:
        logger.error(f"Error in fetch_multiple_vsac_tool: {error}", exc_info=True)
        
        # Error response format (like JavaScript)
        error_response = {
            "error": str(error),
            "valueSetIds": value_set_ids,
            "status": "batch_failed",
            "credentialsChecked": {
                "username": "PROVIDED" if username else "MISSING",
                "password": "PROVIDED" if password else "MISSING"
            },
            "timestamp": datetime.now().isoformat()
        }
        
        # Add specific guidance for common errors (like JavaScript)
        if '401' in str(error) or 'authentication' in str(error).lower():
            error_response["guidance"] = [
                "Authentication failed during batch operation",
                "Verify VSAC_USERNAME and VSAC_PASSWORD environment variables",
                "Test credentials using debug-vsac-auth tool",
                "Check UMLS account status and VSAC access"
            ]
        
        return error_response


async def vsac_cache_status_tool() -> Dict[str, Any]:
    """
    Get VSAC cache status - matches JavaScript functionality.
    
    Returns:
        Dict with cache information and environment variables
    """
    stats = vsac_service.get_cache_stats()
    
    return {
        "cacheSize": stats["size"],
        "cachedValueSets": stats["keys"],
        "environmentVariables": {
            "VSAC_USERNAME": "SET" if settings.vsac_username else "NOT SET",
            "VSAC_PASSWORD": "SET" if settings.vsac_password else "NOT SET"
        },
        "status": "cache_info"
    }

import logging
from typing import Dict, Any, List, Optional
from services.vsac_services import vsac_service
from config.settings import settings
from datetime import datetime

logger = logging.getLogger(__name__)


async def fetch_multiple_vsac_tool(
    value_set_ids: List[str],
    username: Optional[str] = None,
    password: Optional[str] = None
) -> Dict[str, Any]:
    """
    Fetch multiple ValueSets from VSAC - matches JavaScript functionality.
    
    Args:
        value_set_ids: List of ValueSet OIDs to fetch
        username: VSAC username (optional, uses env var if not provided)
        password: VSAC password (optional, uses env var if not provided)
        
    Returns:
        Dict with results and summary information
    """
    try:
        # Use environment variables as defaults (like JavaScript)
        username = username or settings.vsac_username
        password = password or settings.vsac_password
        
        # Validate credentials (like JavaScript)
        if not username or not password:
            return {
                "error": "VSAC credentials are required",
                "message": "Set VSAC_USERNAME and VSAC_PASSWORD environment variables, or pass them as parameters",
                "environmentVariables": {
                    "VSAC_USERNAME": "SET" if settings.vsac_username else "NOT SET",
                    "VSAC_PASSWORD": "SET" if settings.vsac_password else "NOT SET"
                },
                "valueSetIds": value_set_ids
            }
        
        logger.info(f"Batch fetching {len(value_set_ids)} VSAC value sets")
        
        results = await vsac_service.retrieve_multiple_value_sets(
            value_set_ids,
            username,
            password
        )
        
        # Process results to match JavaScript format exactly
        processed_results = {}
        for oid, value_set in results.items():
            # Convert to dictionary format (like JavaScript)
            metadata_dict = {}
            concepts_list = []
            
            if hasattr(value_set, 'metadata') and value_set.metadata:
                metadata_dict = {
                    "id": value_set.metadata.id,
                    "displayName": value_set.metadata.display_name,
                    "version": value_set.metadata.version,
                    "source": value_set.metadata.source,
                    "type": value_set.metadata.type,
                    "binding": value_set.metadata.binding,
                    "status": value_set.metadata.status,
                    "revisionDate": value_set.metadata.revision_date,
                    "description": value_set.metadata.description,
                    "clinicalFocus": value_set.metadata.clinical_focus,
                    "dataElementScope": value_set.metadata.data_element_scope,
                    "inclusionCriteria": value_set.metadata.inclusion_criteria,
                    "exclusionCriteria": value_set.metadata.exclusion_criteria
                }
            
            if hasattr(value_set, 'concepts') and value_set.concepts:
                concepts_list = [
                    {
                        "code": concept.code,
                        "codeSystem": concept.code_system,
                        "codeSystemName": concept.code_system_name,
                        "codeSystemVersion": concept.code_system_version,
                        "displayName": concept.display_name
                    }
                    for concept in value_set.concepts
                ]
            
            processed_results[oid] = {
                "metadata": metadata_dict,
                "concepts": concepts_list
            }
        
        # Calculate summary statistics (like JavaScript)
        successful_retrievals = len([r for r in processed_results.values() 
                                   if r["concepts"] and len(r["concepts"]) > 0])
        total_concepts = sum(len(r["concepts"]) for r in processed_results.values())
        
        # Format response exactly like JavaScript version
        summary = {
            "totalRequested": len(value_set_ids),
            "successfulRetrievals": successful_retrievals,
            "totalConcepts": total_concepts,
            "results": processed_results,
            "credentialsUsed": {
                "username": username,
                "fromEnvironment": {
                    "username": username == settings.vsac_username,
                    "password": password == settings.vsac_password
                }
            },
            "retrievedAt": datetime.now().isoformat()
        }
        
        return summary
        
    except Exception as error:
        logger.error(f"Error in fetch_multiple_vsac_tool: {error}", exc_info=True)
        
        # Error response format (like JavaScript)
        error_response = {
            "error": str(error),
            "valueSetIds": value_set_ids,
            "status": "batch_failed",
            "credentialsChecked": {
                "username": "PROVIDED" if username else "MISSING",
                "password": "PROVIDED" if password else "MISSING"
            },
            "timestamp": datetime.now().isoformat()
        }
        
        # Add specific guidance for common errors (like JavaScript)
        if '401' in str(error) or 'authentication' in str(error).lower():
            error_response["guidance"] = [
                "Authentication failed during batch operation",
                "Verify VSAC_USERNAME and VSAC_PASSWORD environment variables",
                "Test credentials using debug-vsac-auth tool",
                "Check UMLS account status and VSAC access"
            ]
        
        return error_response


async def vsac_cache_status_tool() -> Dict[str, Any]:
    """
    Get VSAC cache status - matches JavaScript functionality.
    
    Returns:
        Dict with cache information and environment variables
    """
    stats = vsac_service.get_cache_stats()
    
    return {
        "cacheSize": stats["size"],
        "cachedValueSets": stats["keys"],
        "environmentVariables": {
            "VSAC_USERNAME": "SET" if settings.vsac_username else "NOT SET",
            "VSAC_PASSWORD": "SET" if settings.vsac_password else "NOT SET"
        },
        "status": "cache_info"
    }