# Fixed src/tools/map_vsac_to_omop.py - Remove placeholder data and match JavaScript version

import logging
from typing import Dict, Any, List, Optional
import asyncpg
from utils.extractors import extract_valueset_identifiers_from_cql, map_vsac_to_omop_vocabulary, extract_individual_codes_from_cql
from services.vsac_services import vsac_service
from config.settings import settings
from datetime import datetime
from utils.helpers import format_list_with_double_quotes

logger = logging.getLogger(__name__)


def prepare_concepts_and_summary(vsac_results: Dict, valuesets: List) -> tuple:
    """
    Build the flattened concept list for OMOP mapping and a per-ValueSet summary.
    Matches the JavaScript prepareConceptsAndSummary function exactly.
    """
    concepts_for_mapping = []
    value_set_summary = {}
    
    for oid, vsac_set in vsac_results.items():
        # Always get an array (fallback to empty) - like JavaScript
        concepts = vsac_set.concepts if hasattr(vsac_set, 'concepts') and vsac_set.concepts else []
        
        if len(concepts) == 0:
            value_set_summary[oid] = {
                "conceptCount": 0,
                "codeSystemsFound": [],
                "status": "empty",
                "metadata": vsac_set.metadata.model_dump() if hasattr(vsac_set, 'metadata') else {},
                "description": vsac_set.metadata.description if hasattr(vsac_set, 'metadata') else None,
                "dataElementScope": vsac_set.metadata.data_element_scope if hasattr(vsac_set, 'metadata') else None,
                "clinicalFocus": vsac_set.metadata.clinical_focus if hasattr(vsac_set, 'metadata') else None,
                "inclusionCriteria": vsac_set.metadata.inclusion_criteria if hasattr(vsac_set, 'metadata') else None,
                "exclusionCriteria": vsac_set.metadata.exclusion_criteria if hasattr(vsac_set, 'metadata') else None,
            }
            continue
        
        # Friendly name from CQL extraction (if available) - like JavaScript
        vs_info = next((vs for vs in valuesets if vs.oid == oid), None)
        value_set_name = vs_info.name if vs_info else f"Unknown_{oid}"
        
        # Track summary stats - like JavaScript
        code_systems_found = list(set(c.code_system_name for c in concepts))
        value_set_summary[oid] = {
            "name": value_set_name,
            "conceptCount": len(concepts),
            "codeSystemsFound": code_systems_found,
            "status": "success",
            "description": vsac_set.metadata.description if hasattr(vsac_set, 'metadata') else None,
            "dataElementScope": vsac_set.metadata.data_element_scope if hasattr(vsac_set, 'metadata') else None,
            "clinicalFocus": vsac_set.metadata.clinical_focus if hasattr(vsac_set, 'metadata') else None,
            "inclusionCriteria": vsac_set.metadata.inclusion_criteria if hasattr(vsac_set, 'metadata') else None,
            "exclusionCriteria": vsac_set.metadata.exclusion_criteria if hasattr(vsac_set, 'metadata') else None,
        }
        
        # Flatten for OMOP mapping - like JavaScript
        for concept in concepts:
            concepts_for_mapping.append({
                "concept_set_id": oid,
                "concept_set_name": value_set_name,
                "concept_code": concept.code,
                "vocabulary_id": map_vsac_to_omop_vocabulary(concept.code_system_name),
                "original_vocabulary": concept.code_system_name,
                "display_name": concept.display_name,
                "code_system": concept.code_system,
            })
    
    return concepts_for_mapping, value_set_summary


def summarise_vsac_fetch(vsac_results: Dict) -> Dict:
    """
    Build a concise diagnostic object for the VSAC-fetch step.
    Matches the JavaScript summariseVsacFetch function exactly.
    """
    summary = {
        "totalRequested": len(vsac_results),
        "successfulRetrievals": 0,
        "totalConceptsRetrieved": 0,
        "results": vsac_results,
        "detailedSummary": [],
        "retrievedAt": datetime.now().isoformat(),
    }
    
    for oid, vsac_set in vsac_results.items():
        concepts = vsac_set.concepts if hasattr(vsac_set, 'concepts') and vsac_set.concepts else []
        
        if len(concepts) > 0:
            summary["successfulRetrievals"] += 1
        summary["totalConceptsRetrieved"] += len(concepts)
        
        summary["detailedSummary"].append({
            "oid": oid,
            "conceptCount": len(concepts),
            "codeSystemsFound": list(set(c.code_system_name for c in concepts)),
            "status": "success" if len(concepts) > 0 else "empty",
            "metadata": vsac_set.metadata.model_dump() if hasattr(vsac_set, 'metadata') else {},
            "sampleConcepts": [
                {
                    "code": c.code,
                    "displayName": c.display_name,
                    "codeSystemName": c.code_system_name,
                }
                for c in concepts[:3]
            ],
        })
    
    return summary


async def map_concepts_to_omop_database(
    concepts: List[Dict],
    cdm_database_schema: str,
    db_config: Dict,
    options: Dict,
    target_fact_tables: List[str]
) -> Dict[str, Any]:
    """
    Map concepts to OMOP using actual database queries.
    FIXED: Remove all placeholder/mock data and use real database like JavaScript version.
    """
    logger.info(f"Mapping {len(concepts)} concepts to OMOP using database...")
    logger.info(f"Database: {db_config['host']}/{db_config['database']}, Schema: {cdm_database_schema}")
    logger.info(f"Target fact tables: {', '.join(target_fact_tables)}")
    
    # FIXED: Use simple connection instead of pool to avoid pool size issues
    connection = None
    
    try:
        logger.info("Attempting to connect to database...")
        connection = await asyncpg.connect(
            user=db_config["user"],
            host=db_config["host"],
            database=db_config["database"],
            password=db_config["password"],
            port=db_config.get("port", 5432),
            command_timeout=30
        )
        logger.info("Successfully connected to database")
        
        # Test the connection with a simple query (like JavaScript)
        logger.info("Testing database connection...")
        test_result = await connection.fetchrow('SELECT version()')
        logger.info(f"Database version: {test_result['version'][:50]}...")
        
        # Test access to the OMOP schema (like JavaScript)
        logger.info(f"Testing access to schema '{cdm_database_schema}'...")
        schema_test_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = $1 
            AND table_name IN ('concept', 'concept_relationship_new')
            ORDER BY table_name
        """
        
        schema_test_result = await connection.fetch(schema_test_query, cdm_database_schema)
        found_tables = [row['table_name'] for row in schema_test_result]
        logger.info(f"Found OMOP tables in schema '{cdm_database_schema}': {found_tables}")
        
        if len(schema_test_result) == 0:
            # Try alternative schema names (like JavaScript)
            logger.info("No tables found in specified schema, trying alternative schemas...")
            alt_schemas = ['dbo', 'cdm', 'public', 'omop']
            found_schema = None
            
            for alt_schema in alt_schemas:
                if alt_schema == cdm_database_schema:
                    continue  # Already tried
                try:
                    alt_result = await connection.fetch(schema_test_query, alt_schema)
                    if len(alt_result) > 0:
                        found_schema = alt_schema
                        found_tables = [row['table_name'] for row in alt_result]
                        logger.info(f"Found OMOP tables in alternative schema '{alt_schema}': {found_tables}")
                        break
                except Exception as err:
                    logger.error(f"Schema '{alt_schema}' not accessible: {err}")
            
            if not found_schema:
                raise Exception(f"No OMOP tables found in schema '{cdm_database_schema}' or alternative schemas")
            else:
                # Update schema to the found one
                cdm_database_schema = found_schema
                logger.info(f"Using schema: {cdm_database_schema}")
        
        # Step 1: Create temporary table (like JavaScript)
        temp_table_name = f"temp_concepts_{int(datetime.now().timestamp() * 1000)}"
        logger.info(f"Creating temporary table: {temp_table_name}")
        
        await connection.execute(f"""
            CREATE TEMPORARY TABLE {temp_table_name} (
                concept_set_id varchar(255),
                concept_set_name varchar(255),
                concept_code varchar(50),
                vocabulary_id varchar(50),
                original_vocabulary varchar(50),
                display_name text
            )
        """)
        
        logger.info(f"Temporary table created, inserting {len(concepts)} concepts...")
        
        # Insert concepts one by one with better error handling (like JavaScript)
        inserted_count = 0
        for i, concept in enumerate(concepts):
            try:
                await connection.execute(f"""
                    INSERT INTO {temp_table_name} 
                    (concept_set_id, concept_set_name, concept_code, vocabulary_id, original_vocabulary, display_name) 
                    VALUES ($1, $2, $3, $4, $5, $6)
                """, 
                    concept.get("concept_set_id", ""),
                    concept.get("concept_set_name", ""),
                    concept.get("concept_code", ""),
                    concept.get("vocabulary_id", ""),
                    concept.get("original_vocabulary", ""),
                    concept.get("display_name", "")
                )
                inserted_count += 1
                
                if i == 0:
                    logger.info(f"First concept inserted successfully: {concept.get('concept_code')}")
                
            except Exception as insert_error:
                logger.error(f"Failed to insert concept {i + 1} ({concept.get('concept_code')}): {insert_error}")
                logger.debug(f"Concept data: {concept}")
                # Continue with other concepts
        
        logger.info(f"Successfully inserted {inserted_count}/{len(concepts)} concepts into temporary table")
        
        # Verify the insertion (like JavaScript)
        count_result = await connection.fetchrow(f"SELECT COUNT(*) as count FROM {temp_table_name}")
        logger.info(f"Verification: {count_result['count']} rows in temporary table")
        
        if count_result['count'] == 0:
            raise Exception("No concepts were successfully inserted into temporary table")
        
        results = {
            "tempConceptListSize": len(concepts),
            "insertedConceptCount": inserted_count,
            "conceptsByValueSet": group_concepts_by_value_set(concepts),
            "databaseInfo": {
                "version": test_result['version'][:100],
                "schema": cdm_database_schema,
                "tempTableName": temp_table_name,
                "conceptsInserted": inserted_count
            }
        }
        
        # Execute actual database queries for all mapping types (like JavaScript)
        if options.get("includeVerbatim", True):
            logger.info("Executing verbatim matching query...")
            try:
                results["verbatim"] = await execute_verbatim_query_real(connection, temp_table_name, cdm_database_schema)
            except Exception as verbatim_error:
                logger.error(f"Verbatim query failed: {verbatim_error}")
                results["verbatimError"] = str(verbatim_error)
                results["verbatim"] = []
        else:
            results["verbatim"] = []
        
        if options.get("includeStandard", True):
            logger.info("Executing standard concept query...")
            try:
                results["standard"] = await execute_standard_query_real(connection, temp_table_name, cdm_database_schema)
            except Exception as standard_error:
                logger.error(f"Standard query failed: {standard_error}")
                results["standardError"] = str(standard_error)
                results["standard"] = []
        else:
            results["standard"] = []
        
        if options.get("includeMapped", True):
            logger.info("Executing mapped concept query...")
            try:
                results["mapped"] = await execute_mapped_query_real(connection, temp_table_name, cdm_database_schema)
            except Exception as mapped_error:
                logger.error(f"Mapped query failed: {mapped_error}")
                results["mappedError"] = str(mapped_error)
                results["mapped"] = []
        else:
            results["mapped"] = []
        
        # Generate comprehensive summary based on actual results (like JavaScript)
        results["mappingSummary"] = generate_omop_mapping_summary(results, concepts)
        
        # Generate the actual SQL queries used (like JavaScript)
        results["sql_queries"] = {
            "verbatim": generate_verbatim_sql(cdm_database_schema, temp_table_name),
            "standard": generate_standard_sql(cdm_database_schema, temp_table_name),
            "mapped": generate_mapped_sql(cdm_database_schema, temp_table_name)
        }
        
        # Clean up temporary table (like JavaScript)
        try:
            await connection.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            logger.info(f"Cleaned up temporary table: {temp_table_name}")
        except Exception as cleanup_error:
            logger.error(f"Error cleaning up temporary table: {cleanup_error}")
        
        logger.info(f"OMOP mapping completed: {results['mappingSummary']['totalMappings']} total mappings found")
        
        return results
        
    except Exception as error:
        logger.error(f"Error in OMOP database mapping: {error}")
        logger.debug(f"Error details: {error}")
        raise Exception(f"OMOP database mapping failed: {str(error)}")
    finally:
        if connection:
            await connection.close()
            logger.info("Database connection closed")


async def execute_verbatim_query_real(connection, temp_table_name: str, cdm_schema: str) -> List[Dict]:
    """Execute verbatim matching query (exact concept_code and vocabulary_id)."""
    verbatim_query = f"""
        SELECT t.concept_set_id, c.concept_id, c.concept_code, c.vocabulary_id, 
               c.domain_id, c.concept_class_id, c.concept_name,
               t.concept_set_name, t.original_vocabulary
        FROM {cdm_schema}.concept c 
        INNER JOIN {temp_table_name} t
        ON c.concept_code = t.concept_code
        AND c.vocabulary_id = t.vocabulary_id
        ORDER BY t.concept_set_id, c.concept_id
    """
    
    result = await connection.fetch(verbatim_query)
    logger.info(f"Verbatim query returned {len(result)} matches")
    
    return [
        {
            "concept_set_id": row["concept_set_id"],
            "concept_set_name": row["concept_set_name"],
            "concept_id": int(row["concept_id"]),
            "concept_code": row["concept_code"],
            "vocabulary_id": row["vocabulary_id"],
            "domain_id": row["domain_id"],
            "concept_class_id": row["concept_class_id"],
            "concept_name": row["concept_name"],
            "source_vocabulary": row["original_vocabulary"],
            "mapping_type": "verbatim"
        }
        for row in result
    ]


async def execute_standard_query_real(connection, temp_table_name: str, cdm_schema: str) -> List[Dict]:
    """Execute standard concept matching query (standard_concept = 'S')."""
    standard_query = f"""
        SELECT t.concept_set_id, c.concept_id, c.concept_code, c.vocabulary_id,
               c.domain_id, c.concept_class_id, c.concept_name, c.standard_concept,
               t.concept_set_name, t.original_vocabulary
        FROM {cdm_schema}.concept c 
        INNER JOIN {temp_table_name} t
        ON c.concept_code = t.concept_code
        AND c.vocabulary_id = t.vocabulary_id
        AND c.standard_concept = 'S'
        ORDER BY t.concept_set_id, c.concept_id
    """
    
    result = await connection.fetch(standard_query)
    logger.info(f"Standard query returned {len(result)} matches")
    
    return [
        {
            "concept_set_id": row["concept_set_id"],
            "concept_set_name": row["concept_set_name"],
            "concept_id": int(row["concept_id"]),
            "concept_code": row["concept_code"],
            "vocabulary_id": row["vocabulary_id"],
            "domain_id": row["domain_id"],
            "concept_class_id": row["concept_class_id"],
            "concept_name": row["concept_name"],
            "standard_concept": row["standard_concept"],
            "source_vocabulary": row["original_vocabulary"],
            "mapping_type": "standard"
        }
        for row in result
    ]


async def execute_mapped_query_real(connection, temp_table_name: str, cdm_schema: str) -> List[Dict]:
    """Execute mapped concept query (via 'Maps to' relationships)."""
    # Note: Uses concept_relationship_new like JavaScript version
    mapped_query = f"""
        SELECT t.concept_set_id, cr.concept_id_2 AS concept_id, c.concept_code, c.vocabulary_id,
               c.concept_id as source_concept_id, cr.relationship_id,
               target_c.concept_name, target_c.domain_id, target_c.concept_class_id, target_c.standard_concept,
               t.concept_set_name, t.original_vocabulary
        FROM {cdm_schema}.concept c 
        INNER JOIN {temp_table_name} t
        ON c.concept_code = t.concept_code
        AND c.vocabulary_id = t.vocabulary_id
        INNER JOIN {cdm_schema}.concept_relationship_new cr
        ON c.concept_id = cr.concept_id_1
        AND cr.relationship_id = 'Maps to'
        INNER JOIN {cdm_schema}.concept target_c
        ON cr.concept_id_2 = target_c.concept_id
        ORDER BY t.concept_set_id, cr.concept_id_2
    """
    
    result = await connection.fetch(mapped_query)
    logger.info(f"Mapped query returned {len(result)} matches")
    
    return [
        {
            "concept_set_id": row["concept_set_id"],
            "concept_set_name": row["concept_set_name"],
            "concept_id": int(row["concept_id"]),
            "source_concept_id": int(row["source_concept_id"]),
            "concept_code": row["concept_code"],
            "vocabulary_id": row["vocabulary_id"],
            "domain_id": row["domain_id"],
            "concept_class_id": row["concept_class_id"],
            "concept_name": row["concept_name"],
            "standard_concept": row["standard_concept"],
            "relationship_id": row["relationship_id"],
            "source_vocabulary": row["original_vocabulary"],
            "mapping_type": "mapped"
        }
        for row in result
    ]


def group_concepts_by_value_set(concepts: List[Dict]) -> Dict:
    """Group concepts by ValueSet ID for easier processing."""
    result = {}
    for concept in concepts:
        concept_set_id = concept.get("concept_set_id")
        if concept_set_id not in result:
            result[concept_set_id] = []
        result[concept_set_id].append(concept)
    return result


def generate_omop_mapping_summary(results: Dict, temp_concept_list: List) -> Dict:
    """Generate mapping summary statistics."""
    total_source_concepts = len(temp_concept_list)
    verbatim_count = len(results.get("verbatim", []))
    standard_count = len(results.get("standard", []))
    mapped_count = len(results.get("mapped", []))
    
    # Calculate unique concept_ids across all mapping types
    all_concept_ids = set()
    for mapping_type in ["verbatim", "standard", "mapped"]:
        for mapping in results.get(mapping_type, []):
            all_concept_ids.add(mapping.get("concept_id"))
    
    # Group mappings by ValueSet
    mappings_by_value_set = {}
    for mapping_type in ["verbatim", "standard", "mapped"]:
        for mapping in results.get(mapping_type, []):
            concept_set_id = mapping.get("concept_set_id")
            if concept_set_id not in mappings_by_value_set:
                mappings_by_value_set[concept_set_id] = {
                    "verbatim": 0,
                    "standard": 0,
                    "mapped": 0,
                    "uniqueConceptIds": set()
                }
            mappings_by_value_set[concept_set_id][mapping.get("mapping_type")] += 1
            mappings_by_value_set[concept_set_id]["uniqueConceptIds"].add(mapping.get("concept_id"))
    
    # Convert Sets to lists for JSON serialization
    for value_set_id in mappings_by_value_set:
        mappings_by_value_set[value_set_id]["uniqueConceptIds"] = list(
            mappings_by_value_set[value_set_id]["uniqueConceptIds"]
        )
    
    return {
        "totalSourceConcepts": total_source_concepts,
        "totalMappings": verbatim_count + standard_count + mapped_count,
        "uniqueTargetConcepts": len(all_concept_ids),
        "mappingCounts": {
            "verbatim": verbatim_count,
            "standard": standard_count,
            "mapped": mapped_count
        },
        "mappingPercentages": {
            "verbatim": f"{(verbatim_count / total_source_concepts * 100):.1f}" if total_source_concepts > 0 else "0.0",
            "standard": f"{(standard_count / total_source_concepts * 100):.1f}" if total_source_concepts > 0 else "0.0",
            "mapped": f"{(mapped_count / total_source_concepts * 100):.1f}" if total_source_concepts > 0 else "0.0"
        },
        "mappingsByValueSet": [
            {
                "concept_set_id": value_set_id,
                "verbatim_mappings": stats["verbatim"],
                "standard_mappings": stats["standard"],
                "mapped_mappings": stats["mapped"],
                "unique_concept_ids": stats["uniqueConceptIds"],
                "total_mappings": stats["verbatim"] + stats["standard"] + stats["mapped"]
            }
            for value_set_id, stats in mappings_by_value_set.items()
        ]
    }


def generate_verbatim_sql(cdm_database_schema: str, temp_table_name: str = "#temp_hee_concept_list") -> str:
    """Generate SQL for verbatim concept matching."""
    return f"""
    SELECT t.concept_set_id, c.concept_id AS concept_id, c.concept_code, c.vocabulary_id,
           c.domain_id, c.concept_class_id, c.concept_name
    FROM {cdm_database_schema}.concept c 
    INNER JOIN {temp_table_name} t
    ON c.concept_code = t.concept_code
    AND c.vocabulary_id = t.vocabulary_id
    ORDER BY t.concept_set_id, c.concept_id"""


def generate_standard_sql(cdm_database_schema: str, temp_table_name: str = "#temp_hee_concept_list") -> str:
    """Generate SQL for standard concept matching."""
    return f"""
    SELECT t.concept_set_id, c.concept_id AS concept_id, c.concept_code, c.vocabulary_id,
           c.domain_id, c.concept_class_id, c.concept_name, c.standard_concept
    FROM {cdm_database_schema}.concept c 
    INNER JOIN {temp_table_name} t
    ON c.concept_code = t.concept_code
    AND c.vocabulary_id = t.vocabulary_id
    AND c.standard_concept = 'S'
    ORDER BY t.concept_set_id, c.concept_id"""


def generate_mapped_sql(cdm_database_schema: str, temp_table_name: str = "#temp_hee_concept_list") -> str:
    """Generate SQL for mapped concept matching."""
    return f"""
    SELECT t.concept_set_id, cr.concept_id_2 AS concept_id, c.concept_code, c.vocabulary_id,
           c.concept_id as source_concept_id, cr.relationship_id,
           target_c.concept_name, target_c.domain_id, target_c.concept_class_id, target_c.standard_concept
    FROM {cdm_database_schema}.concept c 
    INNER JOIN {temp_table_name} t
    ON c.concept_code = t.concept_code
    AND c.vocabulary_id = t.vocabulary_id
    INNER JOIN {cdm_database_schema}.concept_relationship_new cr
    ON c.concept_id = cr.concept_id_1
    AND cr.relationship_id = 'Maps to'
    INNER JOIN {cdm_database_schema}.concept target_c
    ON cr.concept_id_2 = target_c.concept_id
    ORDER BY t.concept_set_id, cr.concept_id_2"""


def generate_mapping_summary(extracted_oids, valuesets, value_set_summary, concepts_for_mapping, omop_mapping_results):
    """Generate comprehensive mapping summary like JavaScript version."""
    summary = {
        "pipeline_success": True,
        "total_valuesets_extracted": len(extracted_oids),
        "total_concepts_from_vsac": len(concepts_for_mapping),
        "total_omop_mappings": {
            "verbatim": len(omop_mapping_results.get("verbatim", [])),
            "standard": len(omop_mapping_results.get("standard", [])),
            "mapped": len(omop_mapping_results.get("mapped", []))
        },
        "valueset_breakdown": [
            {
                "oid": oid,
                "name": info.get("name", f"Unknown_{oid}"),
                "concept_count": info.get("conceptCount", 0),
                "code_systems": info.get("codeSystemsFound", []),
                "status": info.get("status", "unknown")
            }
            for oid, info in value_set_summary.items()
        ],
        "vocabulary_distribution": {},
        "mapping_coverage": {}
    }
    
    # Calculate vocabulary distribution
    vocab_counts = {}
    for concept in concepts_for_mapping:
        vocab_id = concept.get("vocabulary_id", "unknown")
        vocab_counts[vocab_id] = vocab_counts.get(vocab_id, 0) + 1
    summary["vocabulary_distribution"] = vocab_counts
    
    # Calculate mapping coverage
    total_concepts = len(concepts_for_mapping)
    summary["mapping_coverage"] = {
        "verbatim_percentage": f"{(len(omop_mapping_results.get('verbatim', [])) / total_concepts * 100):.1f}" if total_concepts > 0 else "0.0",
        "standard_percentage": f"{(len(omop_mapping_results.get('standard', [])) / total_concepts * 100):.1f}" if total_concepts > 0 else "0.0",
        "mapped_percentage": f"{(len(omop_mapping_results.get('mapped', [])) / total_concepts * 100):.1f}" if total_concepts > 0 else "0.0"
    }
    
    return summary


async def map_vsac_to_omop_tool(
    cql_query: str,
    vsac_username: Optional[str] = None,
    vsac_password: Optional[str] = None,
    database_user: Optional[str] = None,
    database_endpoint: Optional[str] = None,
    database_name: Optional[str] = None,
    database_password: Optional[str] = None,
    omop_database_schema: Optional[str] = None,
    include_verbatim: bool = True,
    include_standard: bool = True,
    include_mapped: bool = True,
    target_fact_tables: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Complete VSAC to OMOP mapping pipeline tool - matches JavaScript functionality exactly."""
    try:
        # Use environment variables as defaults (like JavaScript)
        vsac_username = vsac_username or settings.vsac_username
        vsac_password = vsac_password or settings.vsac_password
        database_user = database_user or settings.database_user
        database_endpoint = database_endpoint or settings.database_endpoint
        database_name = database_name or settings.database_name
        database_password = database_password or settings.database_password
        omop_database_schema = omop_database_schema or settings.omop_database_schema
        target_fact_tables = target_fact_tables or [
            "condition_occurrence",
            "procedure_occurrence", 
            "measurement",
            "drug_exposure"
        ]
        
        # Validate required credentials (like JavaScript)
        if not vsac_username or not vsac_password:
            return {
                "success": False,
                "error": "VSAC credentials are required",
                "message": "Set VSAC_USERNAME and VSAC_PASSWORD environment variables, or pass them as parameters",
                "environmentVariables": {
                    "VSAC_USERNAME": "SET" if settings.vsac_username else "NOT SET",
                    "VSAC_PASSWORD": "SET" if settings.vsac_password else "NOT SET"
                }
            }
        
        if not database_password:
            return {
                "success": False,
                "error": "Database password is required",
                "message": "Set DATABASE_PASSWORD environment variable, or pass it as a parameter",
                "environmentVariables": {
                    "DATABASE_PASSWORD": "SET" if settings.database_password else "NOT SET"
                }
            }
        
        logger.info("Starting VSAC to OMOP mapping pipeline with environment variable defaults...")
        logger.info(f"Using VSAC username: {vsac_username}")
        logger.info(f"Using database: {database_endpoint}/{database_name}")
        
        # Step 1: Extract ValueSet OIDs from CQL (like JavaScript)
        logger.info("Step 1: Extracting ValueSet OIDs from CQL...")
        extraction_result = extract_valueset_identifiers_from_cql(cql_query)
        extracted_oids = extraction_result[0]  # oids
        valuesets = extraction_result[1]       # valuesets

        # Also extract individual codes
        code_extraction_result = extract_individual_codes_from_cql(cql_query)
        individual_codes = code_extraction_result.get('codes', [])
        logger.info(f"Found {len(individual_codes)} individual codes")
        
        if len(extracted_oids) == 0:
            return {
                "success": False,
                "message": "No ValueSet OIDs found in CQL query",
                "cqlQuery": cql_query,
                "extractedOids": [],
                "valuesets": []
            }
        
        logger.info(f"Found {len(extracted_oids)} unique ValueSet OIDs")
        
        # Step 2: Fetch concepts from VSAC for all ValueSets (like JavaScript)
        logger.info("Step 2: Fetching concepts from VSAC...")
        vsac_results = await vsac_service.retrieve_multiple_value_sets(
            extracted_oids,
            vsac_username,
            vsac_password
        )
        
        # Step 3: Prepare concept data for OMOP mapping (like JavaScript)
        logger.info("Step 3: Preparing concept data for OMOP mapping...")
        concepts_for_mapping, value_set_summary = prepare_concepts_and_summary(
            vsac_results, valuesets
        )

        individual_code_mappings = []
        for code in individual_codes:
            clean_code = code['code'].replace('-', '_').replace('.', '_')
            placeholder_name = f"PLACEHOLDER_{code['system'].upper()}_{clean_code}"
            
            concepts_for_mapping.append({
                "concept_set_id": placeholder_name,
                "concept_set_name": code['name'],
                "concept_code": code['code'],
                "vocabulary_id": map_vsac_to_omop_vocabulary(code['system']),
                "original_vocabulary": code['system'],
                "display_name": code['name'],
                "code_system": code['system'],
                "is_individual_code": True
            })
            
            individual_code_mappings.append({
                "code": code['code'],
                "name": code['name'],
                "system": code['system'],
                "placeholder": placeholder_name
            })

        logger.info(f'THISSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS {individual_code_mappings}')
        print(f'THISSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS {individual_code_mappings}')
        
        logger.info(f"Prepared {len(concepts_for_mapping)} concepts for OMOP mapping")
        
        # Step 4: Map to OMOP concepts using real database (like JavaScript)
        logger.info("Step 4: Mapping to OMOP concepts using database...")
        db_config = {
            "user": database_user,
            "host": database_endpoint,
            "database": database_name,
            "password": database_password,
            "port": 5432,  # PostgreSQL default port
            "ssl": False
        }
        
        omop_mapping_results = await map_concepts_to_omop_database(
            concepts_for_mapping,
            omop_database_schema,
            db_config,
            {
                "includeVerbatim": include_verbatim,
                "includeStandard": include_standard,
                "includeMapped": include_mapped
            },
            target_fact_tables
        )
        
        # Step 5: Generate summary and statistics (like JavaScript)
        summary = generate_mapping_summary(
            extracted_oids,
            valuesets,
            value_set_summary,
            concepts_for_mapping,
            omop_mapping_results
        )
        
        return {
            "success": True,
            "message": "VSAC to OMOP mapping completed successfully using environment variables",
            "credentialsUsed": {
                "vsacUsername": vsac_username,
                "databaseEndpoint": database_endpoint,
                "databaseName": database_name,
                "omopSchema": omop_database_schema
            },
            "summary": summary,
            "pipeline": {
                "step1_extraction": {
                    "extractedOids": extracted_oids,
                    "valuesets": [{"name": vs.name, "oid": vs.oid} for vs in valuesets],
                    "totalValueSets": len(extracted_oids)
                },
                "step2_vsac_fetch": {
                    "valueSetSummary": value_set_summary,
                    "totalConceptsFromVsac": len(concepts_for_mapping)
                },
                "step3_omop_mapping": omop_mapping_results,
                "step4_final_concept_sets": {
                    "verbatim": omop_mapping_results.get("verbatim", []),
                    "standard": omop_mapping_results.get("standard", []),
                    "mapped": omop_mapping_results.get("mapped", []),
                },
                "step5_individual_code_mappings": individual_code_mappings
            },
            "metadata": {
                "processingTime": datetime.now().isoformat(),
                "totalValueSets": len(extracted_oids),
                "totalVsacConcepts": len(concepts_for_mapping),
                "totalOmopMappings": {
                    "verbatim": len(omop_mapping_results.get("verbatim", [])),
                    "standard": len(omop_mapping_results.get("standard", [])),
                    "mapped": len(omop_mapping_results.get("mapped", []))
                }
            }
        }
        
    except Exception as error:
        logger.error(f"VSAC to OMOP mapping error: {error}")
        return {
            "success": False,
            "error": str(error),
            "step": "Pipeline execution failed",
            "credentialsChecked": {
                "vsacUsername": "PROVIDED" if vsac_username else "MISSING",
                "databasePassword": "PROVIDED" if database_password else "MISSING"
            },
            "timestamp": datetime.now().isoformat()
        }


async def debug_vsac_omop_pipeline_tool(
    step: str,
    cql_query: str,
    vsac_username: Optional[str] = None,
    vsac_password: Optional[str] = None,
    test_oids: Optional[List[str]] = None,
    database_user: Optional[str] = None,
    database_endpoint: Optional[str] = None,
    database_name: Optional[str] = None,
    database_password: Optional[str] = None,
    omop_database_schema: Optional[str] = None
) -> Dict[str, Any]:
    """Diagnostic tool to test each step individually - matches JavaScript functionality exactly."""
    try:
        # Use environment variables as defaults (like JavaScript)
        vsac_username = vsac_username or settings.vsac_username
        vsac_password = vsac_password or settings.vsac_password
        database_user = database_user or settings.database_user
        database_endpoint = database_endpoint or settings.database_endpoint
        database_name = database_name or settings.database_name
        database_password = database_password or settings.database_password
        omop_database_schema = omop_database_schema or settings.omop_database_schema
        
        results = {
            "environmentVariables": {
                "VSAC_USERNAME": "SET" if settings.vsac_username else "NOT SET",
                "VSAC_PASSWORD": "SET" if settings.vsac_password else "NOT SET",
                "DATABASE_PASSWORD": "SET" if settings.database_password else "NOT SET",
                "DATABASE_USER": database_user,
                "DATABASE_ENDPOINT": database_endpoint,
                "DATABASE_NAME": database_name,
                "OMOP_DATABASE_SCHEMA": omop_database_schema
            },
            "credentialsUsed": {
                "vsacUsername": vsac_username or "NOT PROVIDED",
                "databaseEndpoint": database_endpoint,
                "databaseName": database_name
            }
        }
        
        if step in ["extract", "all"]:
            logger.info("Testing extraction step...")
            extracted_oids, valuesets = extract_valueset_identifiers_from_cql(cql_query)
            from utils.extractors import validate_extracted_oids
            
            results["extraction"] = {
                "extractedOids": extracted_oids,
                "valuesets": [{"name": vs.name, "oid": vs.oid} for vs in valuesets],
                "validation": validate_extracted_oids(extracted_oids),
                # FIXED: This will now show double quotes properly
                "arrayAsStr": format_list_with_double_quotes(extracted_oids)
            }
        
        if step in ["fetch", "all"]:
            logger.info("Testing VSAC fetch step...")
            oids_to_test = test_oids or results.get("extraction", {}).get("extractedOids", [])
            
            if len(oids_to_test) == 0:
                results["vsacFetch"] = {
                    "error": "No ValueSet OIDs available for testing",
                    "suggestion": "Run extraction step first or provide testOids parameter"
                }
            elif not vsac_username or not vsac_password:
                results["vsacFetch"] = {
                    "error": "VSAC credentials required for fetch step",
                    "suggestion": "Provide vsacUsername and vsacPassword parameters",
                    "oidsReadyForFetch": oids_to_test
                }
            else:
                logger.info(f"Fetching concept sets for {len(oids_to_test)} ValueSet OIDs...")
                
                vsac_results = await vsac_service.retrieve_multiple_value_sets(
                    oids_to_test,
                    vsac_username,
                    vsac_password
                )
                
                stats = summarise_vsac_fetch(vsac_results)
                results["vsacFetch"] = stats
                logger.info(f"VSAC fetch completed: {stats['successfulRetrievals']}/{stats['totalRequested']} ValueSets, {stats['totalConceptsRetrieved']} total concepts")
        
        if step in ["map", "all"]:
            logger.info("Testing OMOP mapping step...")
            
            # Use real concept data from VSAC fetch if available, or fetch using test_oids
            concepts_to_map = []
            
            # Check if we have VSAC results from the fetch step
            if "vsacFetch" in results and results["vsacFetch"].get("results"):
                # Convert VSAC results to concept mapping format (like JavaScript)
                logger.info("Using real VSAC concept data from fetch step for mapping test...")
                
                for oid, vsac_set in results["vsacFetch"]["results"].items():
                    if hasattr(vsac_set, 'concepts') and vsac_set.concepts:
                        # Find the ValueSet name from extraction results
                        valueset_info = None
                        if "extraction" in results and results["extraction"].get("valuesets"):
                            valueset_info = next((vs for vs in results["extraction"]["valuesets"] if vs["oid"] == oid), None)
                        valueset_name = valueset_info["name"] if valueset_info else f"ValueSet_{oid}"
                        
                        for concept in vsac_set.concepts:
                            concepts_to_map.append({
                                "concept_set_id": oid,
                                "concept_set_name": valueset_name,
                                "concept_code": concept.code,
                                "vocabulary_id": map_vsac_to_omop_vocabulary(concept.code_system_name),
                                "original_vocabulary": concept.code_system_name,
                                "display_name": concept.display_name,
                                "code_system": concept.code_system
                            })
                
                logger.info(f"Prepared {len(concepts_to_map)} real VSAC concepts for OMOP mapping")
            
            # If no VSAC data from fetch step but test_oids provided, fetch directly
            elif test_oids and len(test_oids) > 0 and vsac_username and vsac_password:
                logger.info(f"Fetching VSAC data directly for mapping test using provided test_oids: {test_oids}")
                
                try:
                    # Fetch VSAC data directly using the provided test_oids
                    direct_vsac_results = await vsac_service.retrieve_multiple_value_sets(
                        test_oids,
                        vsac_username,
                        vsac_password
                    )
                    
                    # Convert to concept mapping format
                    for oid, vsac_set in direct_vsac_results.items():
                        if hasattr(vsac_set, 'concepts') and vsac_set.concepts:
                            valueset_name = f"TestValueSet_{oid}"
                            
                            for concept in vsac_set.concepts:
                                concepts_to_map.append({
                                    "concept_set_id": oid,
                                    "concept_set_name": valueset_name,
                                    "concept_code": concept.code,
                                    "vocabulary_id": map_vsac_to_omop_vocabulary(concept.code_system_name),
                                    "original_vocabulary": concept.code_system_name,
                                    "display_name": concept.display_name,
                                    "code_system": concept.code_system
                                })
                    
                    logger.info(f"Successfully fetched and prepared {len(concepts_to_map)} concepts from test_oids for OMOP mapping")
                    
                    # Store the direct fetch results for reference
                    results["directVsacFetch"] = {
                        "source": "test_oids parameter",
                        "test_oids": test_oids,
                        "conceptsRetrieved": len(concepts_to_map),
                        "message": f"Fetched concepts directly from {len(test_oids)} ValueSet OIDs for mapping test"
                    }
                    
                except Exception as direct_fetch_error:
                    logger.error(f"Failed to fetch VSAC data using test_oids: {direct_fetch_error}")
                    concepts_to_map = []
                    results["directVsacFetch"] = {
                        "error": f"Failed to fetch test_oids: {str(direct_fetch_error)}",
                        "test_oids": test_oids
                    }
            
            else:
                # No VSAC data available
                logger.info("No VSAC concept data available for mapping test")
                concepts_to_map = []
            
            # Check if database connection parameters are provided
            if not database_password:
                results["omopMapping"] = {
                    "error": "Database password required for real OMOP mapping",
                    "suggestion": "Provide databasePassword parameter for database connection",
                    "inputConcepts": len(concepts_to_map),
                    "database": {
                        "user": database_user,
                        "endpoint": database_endpoint,
                        "database": database_name,
                        "schema": omop_database_schema
                    },
                    "conceptsAvailableForMapping": f"{len(concepts_to_map)} real concepts from VSAC" if concepts_to_map else "No concepts available",
                    "dataSource": results.get("directVsacFetch", {}).get("source", "fetch step results") if concepts_to_map else "none"
                }
            elif len(concepts_to_map) == 0:
                # Provide helpful suggestions based on what parameters were provided
                suggestions = []
                if not test_oids and "extraction" not in results:
                    suggestions.append("Run extract step first to get ValueSet OIDs from CQL")
                elif not test_oids and "vsacFetch" not in results:
                    suggestions.append("Run fetch step first, or provide test_oids parameter with specific ValueSet OIDs")
                elif test_oids and (not vsac_username or not vsac_password):
                    suggestions.append(f"VSAC credentials required to fetch concepts for test_oids: {test_oids}")
                else:
                    suggestions.append("Provide test_oids parameter like: ['2.16.840.1.114222.4.11.837', '2.16.840.1.113883.3.526.3.1240']")
                
                results["omopMapping"] = {
                    "error": "No concepts available for mapping",
                    "suggestions": suggestions,
                    "inputConcepts": 0,
                    "database": {
                        "user": database_user,
                        "endpoint": database_endpoint,
                        "database": database_name,
                        "schema": omop_database_schema
                    },
                    "availableOptions": {
                        "test_oids_provided": test_oids if test_oids else None,
                        "extraction_results_available": "extraction" in results,
                        "fetch_results_available": "vsacFetch" in results,
                        "vsac_credentials_available": bool(vsac_username and vsac_password)
                    },
                    "exampleUsage": {
                        "description": "To test OMOP mapping with specific ValueSets, provide test_oids parameter",
                        "example_test_oids": ["2.16.840.1.114222.4.11.837", "2.16.840.1.113883.3.526.3.1240"],
                        "note": "These OIDs will be fetched from VSAC and mapped to OMOP concepts"
                    }
                }
            else:
                # Execute the actual OMOP mapping logic with real database and real concepts
                try:
                    db_config = {
                        "user": database_user,
                        "host": database_endpoint,
                        "database": database_name,
                        "password": database_password,
                        "port": 5432,
                        "ssl": False
                    }
                    
                    omop_results = await map_concepts_to_omop_database(
                        concepts_to_map,
                        omop_database_schema,
                        db_config,
                        {
                            "includeVerbatim": True,
                            "includeStandard": True,
                            "includeMapped": True
                        },
                        ["condition_occurrence", "procedure_occurrence", "measurement", "drug_exposure"]
                    )
                    
                    results["omopMapping"] = {
                        "inputConcepts": len(concepts_to_map),
                        "mappingResults": omop_results,
                        "conceptsByValueSet": omop_results.get("conceptsByValueSet", {}),
                        "mappingSummary": omop_results.get("mappingSummary", {}),
                        "sqlQueries": omop_results.get("sql_queries", {}),
                        "database": {
                            "connected": True,
                            "user": database_user,
                            "endpoint": database_endpoint,
                            "database": database_name,
                            "schema": omop_database_schema
                        },
                        "dataSource": "Real VSAC concepts mapped to real OMOP database"
                    }
                    
                    logger.info(f"OMOP mapping test completed: {omop_results.get('mappingSummary', {}).get('totalMappings', 0)} total mappings found")
                    
                except Exception as db_error:
                    results["omopMapping"] = {
                        "error": f"Database connection failed: {str(db_error)}",
                        "inputConcepts": len(concepts_to_map),
                        "database": {
                            "user": database_user,
                            "endpoint": database_endpoint,
                            "database": database_name,
                            "schema": omop_database_schema,
                            "connectionAttempted": True
                        },
                        "suggestion": "Check database credentials and network connectivity"
                    }
        
        return {
            "step": step,
            "results": results,
            "status": "debug_complete",
            "database": {
                "endpoint": database_endpoint,
                "database": database_name,
                "user": database_user,
                "schema": omop_database_schema
            }
        }
        
    except Exception as error:
        logger.error(f"Error in debug_vsac_omop_pipeline_tool: {error}")
        return {
            "step": step,
            "error": str(error),
            "status": "debug_failed"
        }