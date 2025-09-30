# src/tools/lookup_snomed_code.py

import logging
from typing import Dict, Any, Optional
import httpx
import asyncpg
from config.settings import settings

logger = logging.getLogger(__name__)

# SNOMED CT API configuration
SNOMED_BROWSER_BASE = "http://browser.ihtsdotools.org/api/snomed"
SNOMED_EDITION = "en-edition"  # US Edition


async def fetch_snomed_details(code: str) -> Dict[str, Any]:
    """Look up a SNOMED code and retrieve its details from SNOMED API."""
    
    # Try SNOMED Browser API
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{SNOMED_BROWSER_BASE}/{SNOMED_EDITION}/v1/concepts/{code}",
                headers={"Accept": "application/json"}
            )
            
            if response.status_code == 200:
                data = response.json()
                return {
                    "code": code,
                    "system": "SNOMED",
                    "display": data.get("fsn", {}).get("term") or data.get("pt", {}).get("term") or code,
                    "conceptId": data.get("conceptId"),
                    "active": data.get("active"),
                    "source": "SNOMED Browser API"
                }
    except Exception as error:
        logger.warning(f"SNOMED Browser API lookup failed for {code}: {error}")
    
    # Try alternative endpoint
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{SNOMED_BROWSER_BASE}/browser/descriptions",
                params={
                    "term": code,
                    "limit": 1,
                    "searchMode": "exactMatch"
                },
                headers={"Accept": "application/json"}
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get("items") and len(data["items"]) > 0:
                    item = data["items"][0]
                    return {
                        "code": code,
                        "system": "SNOMED",
                        "display": item.get("term", code),
                        "conceptId": item.get("concept", {}).get("conceptId", code),
                        "source": "SNOMED Browser Search"
                    }
    except Exception as error:
        logger.warning(f"Alternative SNOMED lookup also failed for {code}: {error}")
    
    # Fallback to basic info
    return {
        "code": code,
        "system": "SNOMED",
        "display": code,
        "source": "default"
    }


async def map_snomed_to_omop(
    code: str,
    database_user: Optional[str] = None,
    database_endpoint: Optional[str] = None,
    database_name: Optional[str] = None,
    database_password: Optional[str] = None,
    omop_database_schema: Optional[str] = None
) -> Dict[str, Any]:
    """Map SNOMED code to OMOP concept IDs."""
    
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
                WHERE c1.vocabulary_id = 'SNOMED'
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
                    standard_concept,
                    concept_class_id
                FROM {db_schema}.concept
                WHERE vocabulary_id = 'SNOMED'
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
                            "vocabulary": "SNOMED",
                            "conceptClass": source["concept_class_id"]
                        }],
                        "message": "SNOMED code is already a standard concept"
                    }
                
                # Try any mapping relationship
                any_mapping_query = f"""
                    SELECT 
                        c2.concept_id,
                        c2.concept_name,
                        c2.domain_id,
                        c2.standard_concept,
                        cr.relationship_id
                    FROM {db_schema}.concept_relationship cr
                    JOIN {db_schema}.concept c2 ON cr.concept_id_2 = c2.concept_id
                    WHERE cr.concept_id_1 = $1
                        AND c2.standard_concept = 'S'
                        AND cr.invalid_reason IS NULL
                    ORDER BY 
                        CASE cr.relationship_id 
                            WHEN 'Maps to' THEN 1
                            WHEN 'Concept replaced by' THEN 2
                            ELSE 3
                        END
                    LIMIT 1
                """
                
                mapping_rows = await conn.fetch(any_mapping_query, source["concept_id"])
                
                if mapping_rows:
                    mapping = mapping_rows[0]
                    return {
                        "mapped": True,
                        "conceptIds": [mapping["concept_id"]],
                        "concepts": [{
                            "id": mapping["concept_id"],
                            "name": mapping["concept_name"],
                            "domain": mapping["domain_id"],
                            "vocabulary": "SNOMED",
                            "relationship": mapping["relationship_id"]
                        }],
                        "message": f"Mapped via {mapping['relationship_id']} relationship"
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
                    "message": "SNOMED code found in OMOP but no standard mapping available"
                }
            
            return {
                "mapped": False,
                "message": f"SNOMED code {code} not found in OMOP vocabulary"
            }
            
        finally:
            await conn.close()
            
    except Exception as error:
        logger.error(f"Database error mapping SNOMED to OMOP: {error}")
        return {
            "mapped": False,
            "error": str(error)
        }


def determine_omop_table(domain: str) -> str:
    """Determine the appropriate OMOP domain and table for a SNOMED concept."""
    domain_table_map = {
        "Condition": "condition_occurrence",
        "Procedure": "procedure_occurrence",
        "Measurement": "measurement",
        "Observation": "observation",
        "Drug": "drug_exposure",
        "Device": "device_exposure",
        "Visit": "visit_occurrence"
    }
    return domain_table_map.get(domain, "observation")


async def lookup_snomed_code_tool(
    code: str,
    display: Optional[str] = None,
    database_user: Optional[str] = None,
    database_endpoint: Optional[str] = None,
    database_name: Optional[str] = None,
    database_password: Optional[str] = None,
    omop_database_schema: Optional[str] = None
) -> Dict[str, Any]:
    """
    MCP tool for looking up SNOMED codes and mapping to OMOP.
    
    Args:
        code: SNOMED CT code (e.g., '428371000124100' for hospice discharge)
        display: Optional display name for the code
        database_user: Database user (optional, uses env var)
        database_endpoint: Database endpoint (optional, uses env var)
        database_name: Database name (optional, uses env var)
        database_password: Database password (optional, uses env var)
        omop_database_schema: OMOP schema (optional, uses env var)
        
    Returns:
        Dict with SNOMED details, OMOP mapping, and SQL snippet
    """
    logger.info(f"Looking up SNOMED code: {code}")
    
    # Fetch SNOMED details
    snomed_details = await fetch_snomed_details(code)
    
    # Use provided display name if available
    if display:
        snomed_details["display"] = display
    
    # Map to OMOP concepts
    omop_mapping = await map_snomed_to_omop(
        code,
        database_user,
        database_endpoint,
        database_name,
        database_password,
        omop_database_schema
    )
    
    # Construct response
    response = {
        "snomed": snomed_details,
        "omop": omop_mapping,
        "placeholder": f"{{{{DirectCode:SNOMEDCT:{code}:{snomed_details['display']}}}}}",
        "success": omop_mapping.get("mapped", False)
    }
    
    # Add suggested SQL if mapping successful
    if omop_mapping.get("mapped") and omop_mapping.get("conceptIds"):
        concept_ids = omop_mapping["conceptIds"]
        domain = omop_mapping["concepts"][0].get("domain") if omop_mapping.get("concepts") else None
        table = determine_omop_table(domain) if domain else "condition_occurrence"
        concept_column = f"{table.replace('_occurrence', '')}_concept_id"
        
        response["sql"] = {
            "conceptIds": concept_ids,
            "table": table,
            "column": concept_column,
            "sqlSnippet": (
                f"{concept_column} = {concept_ids[0]}" if len(concept_ids) == 1
                else f"{concept_column} IN ({', '.join(map(str, concept_ids))})"
            )
        }
    
    logger.info(f"SNOMED lookup complete: {'mapped' if response['success'] else 'not mapped'}")
    
    return response