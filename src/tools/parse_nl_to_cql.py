import logging
from typing import Dict, Any, Optional
from services.llm_services import llm_service
from utils.extractors import extract_valueset_identifiers_from_cql, validate_extracted_oids

logger = logging.getLogger(__name__)


async def parse_to_cql(query: str) -> str:
    """Convert natural language query to CQL using LLM service."""
    try:
        messages = [
            {
                "role": "system",
                "content": "You are a medical query parser. Convert the natural language medical query to a valid CQL (Clinical Quality Language) query. Return only the CQL code without any explanation."
            },
            {"role": "user", "content": query}
        ]
        
        response = await llm_service.create_completion(messages)
        return response["content"]
    except Exception as error:
        logger.error(f"Error parsing to CQL: {error}")
        raise Exception("Failed to parse natural language to CQL")


async def parse_nl_to_cql_tool(query: str, include_input: bool = False) -> Dict[str, Any]:
    """
    Parse natural language to CQL and extract ValueSet references.
    
    Args:
        query: Natural language query to convert
        include_input: Whether to include input in response
        
    Returns:
        Dict containing CQL, ValueSet references, and validation info
    """
    try:
        logger.info("Converting natural language to CQL...")
        cql = await parse_to_cql(query)
        
        logger.info("Extracting ValueSet OIDs using valueset declaration pattern...")
        oids, valuesets = extract_valueset_identifiers_from_cql(cql)
        
        # Validate extracted OIDs
        valid_oids = validate_extracted_oids(oids)
        invalid_oids = [oid for oid in oids if oid not in valid_oids]
        
        result = {
            "cql": cql,
            "value_set_references": oids,
            "valuesets": [{"name": vs.name, "oid": vs.oid} for vs in valuesets],
            "extraction_method": "valueset_declaration_regex",
            "validation": {
                "valid_oids": valid_oids,
                "invalid_oids": invalid_oids,
                "warnings": [],
                "total_found": len(oids),
                "valid_count": len(valid_oids)
            }
        }
        
        if include_input:
            result["input"] = query
        
        # Log validation results
        if invalid_oids:
            logger.error(f"Invalid OIDs found: {invalid_oids}")
        
        return result
        
    except Exception as error:
        logger.error(f"Error in parse_nl_to_cql_tool: {error}")
        raise


async def extract_valuesets_tool(cql_query: str, include_input: bool = False) -> Dict[str, Any]:
    """
    Extract ValueSets from CQL with minimal output.
    
    Args:
        cql_query: CQL query to extract from
        include_input: Whether to include input in response
        
    Returns:
        Dict with valuesets, oids, and count
    """
    try:
        oids, valuesets = extract_valueset_identifiers_from_cql(cql_query)
        
        result = {
            "valuesets": [{"name": vs.name, "oid": vs.oid} for vs in valuesets],
            "oids": oids,
            "count": len(oids)
        }
        
        if include_input:
            result["input"] = cql_query
            
        return result
        
    except Exception as error:
        logger.error(f"Error in extract_valuesets_tool: {error}")
        raise


async def valueset_regex_extraction_tool(
    cql_query: str, 
    show_details: bool = False, 
    include_input: bool = False
) -> Dict[str, Any]:
    """
    Test regex extraction patterns on CQL.
    
    Args:
        cql_query: CQL query to test
        show_details: Whether to show detailed regex test results
        include_input: Whether to include input in response
        
    Returns:
        Dict with extraction results and validation
    """
    try:
        logger.info("Testing regex extraction patterns...")
        
        oids, valuesets = extract_valueset_identifiers_from_cql(cql_query)
        valid_oids = validate_extracted_oids(oids)
        invalid_oids = [oid for oid in oids if oid not in valid_oids]
        
        # Import the fixed helper function
        from utils.helpers import format_list_with_double_quotes
        
        result = {
            "extracted_value_sets": [{"name": vs.name, "oid": vs.oid} for vs in valuesets],
            "valid_oids": valid_oids,
            "invalid_oids": invalid_oids,
            "summary": {
                "total_found": len(oids),
                "valid_oids": len(valid_oids),
                "invalid_oids": len(invalid_oids)
            },
            "copy_pastable_arrays": {
                "extracted_oids": oids,
                "valid_oids": valid_oids,
                "invalid_oids": invalid_oids,
                # Use the fixed helper - this will now properly show double quotes
                "extracted_oids_formatted": format_list_with_double_quotes(oids)
            }
        }
        
        if include_input:
            result["input"] = cql_query
        
        if show_details:
            import re
            # Test valueset declaration pattern
            pattern = r'(valueset\s")(.+)(":\s\')(urn:oid:)((\d+\.)*\d+)(\')' 
            matches = []
            
            for match in re.finditer(pattern, cql_query, re.IGNORECASE):
                matches.append({
                    "full_match": match.group(0),
                    "extracted_name": match.group(2),
                    "extracted_oid": match.group(5),
                    "index": match.start()
                })
            
            result["detailed_regex_tests"] = {
                "valueset_pattern": {
                    "pattern": r'(valueset\s")(.+)(":\s\')(urn:oid:)((\d+\.)*\d+)(\')' ,
                    "description": "Matches valueset declarations and extracts both name and OID",
                    "matches": matches
                }
            }
        
        return result
        
    except Exception as error:
        logger.error(f"Error in valueset_regex_extraction_tool: {error}")
        raise