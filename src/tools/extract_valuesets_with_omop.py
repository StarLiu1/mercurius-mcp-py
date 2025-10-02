"""
Tool 2: Extract valuesets from CQL and map to OMOP using existing MCP tool.
"""

import logging
from typing import Dict, Any, Optional

# Import the existing tool directly
from tools.map_vsac_to_omop import map_vsac_to_omop_tool

logger = logging.getLogger(__name__)


async def extract_valuesets_with_omop_tool(
    cql_content: str,
    library_files: Optional[Dict[str, str]] = None,
    parsed_structure: Optional[Dict[str, Any]] = None,
    vsac_username: Optional[str] = None,
    vsac_password: Optional[str] = None,
    database_user: Optional[str] = None,
    database_endpoint: Optional[str] = None,
    database_name: Optional[str] = None,
    database_password: Optional[str] = None,
    omop_database_schema: Optional[str] = None
) -> Dict[str, Any]:
    """
    Tool 2: Extract all valuesets and map to OMOP using existing map-vsac-to-omop tool.
    
    This tool leverages the existing MCP tool infrastructure:
    - Calls map_vsac_to_omop for main CQL
    - Calls map_vsac_to_omop for each library
    - Aggregates all results
    
    Args:
        cql_content: Main CQL content
        library_files: Dict of library_name -> library_content
        parsed_structure: Parsed CQL structure from Tool 1
        vsac_username: VSAC username (optional, uses env var)
        vsac_password: VSAC password (optional, uses env var)
        database_*: Database config (optional, uses env vars)
        
    Returns:
        Dict with:
        - all_valuesets: Combined valuesets from main + libraries
        - placeholder_mappings: OID -> concept_ids mappings
        - valueset_registry: Complete registry with sources
        - individual_codes: LOINC/SNOMED code mappings
        - statistics: Extraction statistics
    """
    try:
        logger.info("=" * 80)
        logger.info("TOOL 2: Extracting Valuesets via map-vsac-to-omop")
        logger.info("=" * 80)
        
        # Extract main CQL valuesets using existing tool
        logger.info("Calling map-vsac-to-omop for main CQL...")
        main_result = await map_vsac_to_omop_tool(
            cql_query=cql_content,
            vsac_username=vsac_username,
            vsac_password=vsac_password,
            database_user=database_user,
            database_endpoint=database_endpoint,
            database_name=database_name,
            database_password=database_password,
            omop_database_schema=omop_database_schema,
            include_verbatim=False,
            include_standard=False,
            include_mapped=True
        )
        logger.info(f"DEBUG: map_vsac_to_omop_tool success: {main_result.get('success')}")
        logger.info(f"DEBUG: map_vsac_to_omop_tool error: {main_result.get('error')}")

        if not main_result.get('success'):
            return {
                "success": False,
                "error": main_result.get('error', 'Unknown error'),
                "step": "extract_valuesets_with_omop"
            }
        
        # Extract results from main CQL
        main_pipeline = main_result.get('pipeline', {})
        main_extraction = main_pipeline.get('step1_extraction', {})
        main_omop = main_pipeline.get('step3_omop_mapping', {})
        
        # Build initial data structures
        all_valuesets = {}
        placeholder_mappings = {}
        individual_codes = {}
        
        # Process main CQL valuesets
        for vs_info in main_extraction.get('valuesets', []):
            oid = vs_info['oid']
            name = vs_info['name']
            
            # Get mapped concept IDs for this valueset
            concept_ids = []
            for concept in main_omop.get('mapped', []):
                if concept.get('concept_set_id') == oid:
                    concept_ids.append(str(concept['concept_id']))
            
            all_valuesets[oid] = {
                "name": name,
                "oid": oid,
                "omop_concept_ids": concept_ids,
                "concept_count": len(concept_ids),
                "source": "main"
            }
            
            # Create placeholders (just oid)
            placeholder_oid = f"PLACEHOLDER_{oid.replace('.', '_')}"
            placeholder_mappings[placeholder_oid] = concept_ids
            
            # placeholder_name = f"PLACEHOLDER_{name.upper().replace(' ', '_').replace('-', '_')}"
            # placeholder_mappings[placeholder_name] = concept_ids
        
        # Process individual codes from main CQL
        for code_info in main_extraction.get('codes', []):
            code = code_info.get('code', '')
            name = code_info.get('name', '')
            system = code_info.get('system', '')
            
            if code and system:
                clean_code = code.replace('-', '_').replace('.', '_')
                placeholder_key = f"PLACEHOLDER_{system.upper()}_{clean_code}"
                
                # Get concept IDs for this code
                concept_ids = []
                for concept in main_omop.get('mapped', []):
                    if concept.get('concept_set_id') == placeholder_key:
                        concept_ids.append(str(concept['concept_id']))
                
                individual_codes[f"{system}_{code}"] = {
                    "name": name,
                    "code": code,
                    "system": system,
                    "omop_concept_ids": concept_ids,
                    "placeholder": placeholder_key
                }
                
                if concept_ids:
                    placeholder_mappings[placeholder_key] = concept_ids
        
        logger.info(f"Main CQL: {len(all_valuesets)} valuesets, {len(individual_codes)} codes")
        
        # Process library files
        library_files = library_files or {}
        for lib_name, lib_content in library_files.items():
            logger.info(f"Calling map-vsac-to-omop for library: {lib_name}")
            
            try:
                lib_result = await map_vsac_to_omop_tool(
                    cql_query=lib_content,
                    vsac_username=vsac_username,
                    vsac_password=vsac_password,
                    database_user=database_user,
                    database_endpoint=database_endpoint,
                    database_name=database_name,
                    database_password=database_password,
                    omop_database_schema=omop_database_schema,
                    include_verbatim=False,
                    include_standard=False,
                    include_mapped=True
                )
                
                if not lib_result.get('success'):
                    logger.warning(f"Library {lib_name} extraction failed: {lib_result.get('error')}")
                    continue
                
                lib_pipeline = lib_result.get('pipeline', {})
                lib_extraction = lib_pipeline.get('step1_extraction', {})
                lib_omop = lib_pipeline.get('step3_omop_mapping', {})
                
                # Process library valuesets
                for vs_info in lib_extraction.get('valuesets', []):
                    oid = vs_info['oid']
                    name = vs_info['name']
                    
                    concept_ids = []
                    for concept in lib_omop.get('mapped', []):
                        if concept.get('concept_set_id') == oid:
                            concept_ids.append(str(concept['concept_id']))
                    
                    # Prefix to avoid conflicts
                    prefixed_key = f"{lib_name}_{oid}" if oid in all_valuesets else oid
                    
                    all_valuesets[prefixed_key] = {
                        "name": name,
                        "oid": oid,
                        "omop_concept_ids": concept_ids,
                        "concept_count": len(concept_ids),
                        "source": lib_name
                    }
                    
                    # Create placeholders
                    placeholder_oid = f"PLACEHOLDER_{oid.replace('.', '_')}"
                    placeholder_mappings[placeholder_oid] = concept_ids
                    
                    # placeholder_name = f"PLACEHOLDER_{lib_name.upper()}_{name.upper().replace(' ', '_').replace('-', '_')}"
                    # placeholder_mappings[placeholder_name] = concept_ids
                
                # Process library individual codes
                for code_info in lib_extraction.get('codes', []):
                    code = code_info.get('code', '')
                    name = code_info.get('name', '')
                    system = code_info.get('system', '')
                    
                    if code and system:
                        clean_code = code.replace('-', '_').replace('.', '_')
                        placeholder_key = f"PLACEHOLDER_{lib_name.upper()}_{system.upper()}_{clean_code}"
                        
                        concept_ids = []
                        for concept in lib_omop.get('mapped', []):
                            if concept.get('concept_set_id') == placeholder_key:
                                concept_ids.append(str(concept['concept_id']))
                        
                        individual_codes[f"{lib_name}_{system}_{code}"] = {
                            "name": name,
                            "code": code,
                            "system": system,
                            "omop_concept_ids": concept_ids,
                            "placeholder": placeholder_key
                        }
                        
                        if concept_ids:
                            placeholder_mappings[placeholder_key] = concept_ids
                
                logger.info(f"  ✓ Library {lib_name}: {len(lib_extraction.get('valuesets', []))} valuesets")
                
            except Exception as lib_error:
                logger.error(f"  ✗ Failed to process library {lib_name}: {lib_error}")
        
        # Build comprehensive valueset registry
        valueset_registry = {}
        if parsed_structure:
            for vs in parsed_structure.get('valuesets', []):
                oid = vs.get('oid', '')
                name = vs.get('name', '')
                if oid:
                    logger.info(f"DEBUG: Original OID from parser: {oid}")
                    clean_oid = oid.replace('urn:oid:', '')
                    logger.info(f"DEBUG: Clean OID from parser: {clean_oid}")

                    valueset_registry[clean_oid] = {
                        'name': name,
                        'oid': clean_oid,
                        'source': 'main'
                    }
            
            library_definitions = parsed_structure.get('library_definitions', {})
            for lib_name, lib_def in library_definitions.items():
                if isinstance(lib_def, dict) and 'valuesets' in lib_def:
                    for vs in lib_def.get('valuesets', []):
                        oid = vs.get('oid', '')
                        name = vs.get('name', '')
                        if oid:
                            clean_oid = oid.replace('urn:oid:', '')
                            valueset_registry[clean_oid] = {
                                'name': name,
                                'oid': clean_oid,
                                'source': lib_name
                            }
        
        # Compile statistics
        statistics = {
            "total_valuesets_extracted": len(all_valuesets),
            "total_individual_codes": len(individual_codes),
            "total_placeholders": len(placeholder_mappings),
            "total_concept_ids": sum(len(concepts) for concepts in placeholder_mappings.values()),
            "registry_valuesets": len(valueset_registry),
            "libraries_processed": len(library_files)
        }
        
        logger.info("=" * 80)
        logger.info("✓ Tool 2 Complete: Valuesets Extracted and Mapped")
        logger.info(f"  - Valuesets: {len(all_valuesets)}")
        logger.info(f"  - Individual codes: {len(individual_codes)}")
        logger.info(f"  - Placeholders: {len(placeholder_mappings)}")
        logger.info(f"  - Total concept IDs: {statistics['total_concept_ids']}")
        logger.info("=" * 80)
        
        return {
            "success": True,
            "all_valuesets": all_valuesets,
            "placeholder_mappings": placeholder_mappings,
            "valueset_registry": valueset_registry,
            "individual_codes": individual_codes,
            "statistics": statistics
        }
        
    except Exception as e:
        logger.error(f"Tool 2 failed: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e),
            "step": "extract_valuesets_with_omop"
        }