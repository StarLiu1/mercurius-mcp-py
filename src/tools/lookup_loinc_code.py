# src/tools/lookup_loinc_code.py

import logging
from typing import Dict, Any, Optional
import httpx
import asyncpg
from config.settings import settings

logger = logging.getLogger(__name__)

# LOINC API configuration
LOINC_FHIR_BASE = "https://fhir.loinc.org"
NIH_LOINC_BASE = "https://clinicaltables.nlm.nih.gov/api/loinc_items/v3"


async def fetch_loinc_details(code: str) -> Dict[str, Any]:
    """Look up a LOINC code and retrieve its details from LOINC API."""
    
    # Try LOINC FHIR server first (requires credentials)
    if settings.loinc_username and settings.loinc_password:
        try:
            import base64
            auth = base64.b64encode(
                f"{settings.loinc_username}:{settings.loinc_password}".encode()
            ).decode()
            
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    f"{LOINC_FHIR_BASE}/CodeSystem/$lookup",
                    params={
                        "system": "http://loinc.org",
                        "code": code
                    },
                    headers={
                        "Authorization": f"Basic {auth}",
                        "Accept": "application/json"
                    }
                )
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get("parameter"):
                        display = next(
                            (p["valueString"] for p in data["parameter"] if p.get("name") == "display"),
                            code
                        )
                        return {
                            "code": code,
                            "system": "LOINC",
                            "display": display,
                            "source": "LOINC FHIR"
                        }
        except Exception as error:
            logger.warning(f"LOINC FHIR lookup failed for {code}: {error}")
    
    # Fallback to NIH Clinical Table Search (no auth required)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{NIH_LOINC_BASE}/search",
                params={
                    "terms": code,
                    "df": "LOINC_NUM"
                }
            )
            
            if response.status_code == 200:
                data = response.json()
                if data and len(data) > 3 and data[3] and len(data[3]) > 0:
                    result = data[3][0]
                    return {
                        "code": code,
                        "system": "LOINC",
                        "display": result[1] if len(result) > 1 else code,
                        "source": "NIH Clinical Tables"
                    }
    except Exception as error:
        logger.warning(f"NIH lookup also failed for {code}: {error}")
    
    # Fallback to basic info
    return {
        "code": code,
        "system": "LOINC",
        "display": code,
        "source": "default"
    }


async def map_loinc_to_omop(
    code: str,
    database_user: Optional[str] = None,
    database_endpoint: Optional[str] = None,
    database_name: Optional[str] = None,
    database_password: Optional[str] = None,
    omop_database_schema: Optional[str] = None
) -> Dict[str, Any]:
    """Map LOINC code to OMOP concept IDs."""
    
    # Use environment defaults
    db_user = database_user or settings.database_user
    db_endpoint = database_endpoint or settings.database_endpoint
    db_name = database_name or settings.database_name
    db_password = database_password or settings.database_password
    db_schema = omop_database_schema or settings.omop_database_schema
    
    if not db_password:
        return {
            "mapped": False,
            "error": "Database password required"
        }
    
    try:
        conn = await asyncpg.connect(
            user=db_user,
            host=db_endpoint,
            database=db_name,
            password=db_password,
            port=5432,
            timeout=30
        )
        
        try:
            # Query for 'Maps to' relationships
            query = f"""
                SELECT 
                    c2.concept_id,
                    c2.concept_name,
                    c2.domain_id,
                    c2.vocabulary_id,
                    c2.concept_class_id,
                    cr.relationship_id
                FROM {db_schema}.concept c1
                JOIN {db_schema}.concept_relationship cr ON c1.concept_id = cr.concept_id_1
                JOIN {db_schema}.concept c2 ON cr.concept_id_2 = c2.concept_id
                WHERE c1.vocabulary_id = 'LOINC'
                    AND c1.concept_code = $1
                    AND cr.relationship_id = 'Maps to'
                    AND c2.standard_concept = 'S'
                    AND cr.invalid_reason IS NULL
            """
            
            rows = await conn.fetch(query, code)
            
            if rows:
                return {
                    "mapped": True,
                    "conceptIds": [row["concept_id"] for row in rows],
                    "concepts": [
                        {
                            "id": row["concept_id"],
                            "name": row["concept_name"],
                            "domain": row["domain_id"],
                            "vocabulary": row["vocabulary_id"],
                            "conceptClass": row["concept_class_id"]
                        }
                        for row in rows
                    ]
                }
            
            # Try to find source concept
            source_query = f"""
                SELECT 
                    concept_id,
                    concept_name,
                    domain_id,
                    standard_concept
                FROM {db_schema}.concept
                WHERE vocabulary_id = 'LOINC'
                    AND concept_code = $1
            """
            
            source_rows = await conn.fetch(source_query, code)
            
            if source_rows:
                source = source_rows[0]
                
                # If already standard, use directly
                if source["standard_concept"] == "S":
                    return {
                        "mapped": True,
                        "conceptIds": [source["concept_id"]],
                        "concepts": [{
                            "id": source["concept_id"],
                            "name": source["concept_name"],
                            "domain": source["domain_id"],
                            "vocabulary": "LOINC",
                            "conceptClass": "LOINC Code"
                        }],
                        "message": "LOINC code is already a standard OMOP concept"
                    }
                
                return {
                    "mapped": False,
                    "sourceConceptId": source["concept_id"],
                    "sourceConcept": {
                        "id": source["concept_id"],
                        "name": source["concept_name"],
                        "domain": source["domain_id"],
                        "isStandard": False
                    },
                    "message": "LOINC code found in OMOP but is not a standard concept"
                }
            
            return {
                "mapped": False,
                "message": f"LOINC code {code} not found in OMOP vocabulary"
            }
            
        finally:
            await conn.close()
            
    except Exception as error:
        logger.error(f"Database error mapping LOINC to OMOP: {error}")
        return {
            "mapped": False,
            "error": str(error)
        }


async def lookup_loinc_code_tool(
    code: str,
    display: Optional[str] = None,
    database_user: Optional[str] = None,
    database_endpoint: Optional[str] = None,
    database_name: Optional[str] = None,
    database_password: Optional[str] = None,
    omop_database_schema: Optional[str] = None
) -> Dict[str, Any]:
    """
    MCP tool for looking up LOINC codes and mapping to OMOP.
    
    Args:
        code: LOINC code (e.g., '8462-4' for diastolic blood pressure)
        display: Optional display name for the code
        database_user: Database user (optional, uses env var)
        database_endpoint: Database endpoint (optional, uses env var)
        database_name: Database name (optional, uses env var)
        database_password: Database password (optional, uses env var)
        omop_database_schema: OMOP schema (optional, uses env var)
        
    Returns:
        Dict with LOINC details, OMOP mapping, and SQL snippet
    """
    logger.info(f"Looking up LOINC code: {code}")
    
    # Fetch LOINC details
    loinc_details = await fetch_loinc_details(code)
    
    # Use provided display name if available
    if display:
        loinc_details["display"] = display
    
    # Map to OMOP concepts
    omop_mapping = await map_loinc_to_omop(
        code,
        database_user,
        database_endpoint,
        database_name,
        database_password,
        omop_database_schema
    )
    
    # Construct response
    response = {
        "loinc": loinc_details,
        "omop": omop_mapping,
        "placeholder": f"{{{{DirectCode:LOINC:{code}:{loinc_details['display']}}}}}",
        "success": omop_mapping.get("mapped", False)
    }
    
    # Add suggested SQL if mapping successful
    if omop_mapping.get("mapped") and omop_mapping.get("conceptIds"):
        concept_ids = omop_mapping["conceptIds"]
        response["sql"] = {
            "conceptIds": concept_ids,
            "sqlSnippet": (
                f"measurement_concept_id = {concept_ids[0]}" if len(concept_ids) == 1
                else f"measurement_concept_id IN ({', '.join(map(str, concept_ids))})"
            )
        }
    
    logger.info(f"LOINC lookup complete: {'mapped' if response['success'] else 'not mapped'}")
    
    return response