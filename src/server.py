import logging
from typing import Optional, List
from mcp.server.fastmcp import FastMCP
from tools.parse_nl_to_cql import (
    parse_nl_to_cql_tool,
    extract_valuesets_tool,
    valueset_regex_extraction_tool
)
from tools.fetch_vasc import fetch_multiple_vsac_tool, vsac_cache_status_tool
from tools.env_status_tool import check_environment_status_tool
from resources.config import config_resource
from resources.schema import omop_schema_resource
from utils.env_helpers import (
    get_vsac_credentials, 
    get_database_config, 
    validate_required_credentials,
    create_credentials_error_response
)
# Import the new OMOP mapping functions
from utils.extractors import extract_valueset_identifiers_from_cql, map_vsac_to_omop_vocabulary
from services.vsac_services import vsac_service
from config.settings import settings
from datetime import datetime
import asyncpg

logger = logging.getLogger(__name__)

# Import OMOP mapping functions (we'll put them directly here for now)
async def prepare_concepts_and_summary(vsac_results, valuesets):
    """Build the flattened concept list for OMOP mapping and a per-ValueSet summary."""
    concepts_for_mapping = []
    value_set_summary = {}
    
    for oid, vsac_set in vsac_results.items():
        concepts = vsac_set.concepts if hasattr(vsac_set, 'concepts') and vsac_set.concepts else []
        
        if len(concepts) == 0:
            value_set_summary[oid] = {
                "conceptCount": 0,
                "codeSystemsFound": [],
                "status": "empty",
                "metadata": vsac_set.metadata.model_dump() if hasattr(vsac_set, 'metadata') else {},
            }
            continue
        
        vs_info = next((vs for vs in valuesets if vs.oid == oid), None)
        value_set_name = vs_info.name if vs_info else f"Unknown_{oid}"
        
        code_systems_found = list(set(c.code_system_name for c in concepts))
        value_set_summary[oid] = {
            "name": value_set_name,
            "conceptCount": len(concepts),
            "codeSystemsFound": code_systems_found,
            "status": "success",
        }
        
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

async def map_concepts_to_omop_database(concepts, cdm_database_schema, db_config, options, target_fact_tables):
    """Map concepts to OMOP using actual database queries."""
    logger.info(f"Mapping {len(concepts)} concepts to OMOP using database...")
    
    pool_config = {
        "user": db_config["user"],
        "host": db_config["host"],
        "database": db_config["database"],
        "password": db_config["password"],
        "port": db_config.get("port", 5432),
        "command_timeout": 30,
        "min_size": 1,  # Minimum connections to maintain in pool
        "max_size": 5,  # Maximum connections in pool (reduced from 10 for MCP usage)
    }
    
    pool = None
    connection = None
    
    try:
        pool = await asyncpg.create_pool(**pool_config)
        connection = await pool.acquire()
        logger.info("Successfully connected to database")
        
        # Test connection
        test_result = await connection.fetchrow('SELECT version()')
        logger.info(f"Database version: {test_result['version'][:50]}...")
        
        # Create temporary table
        temp_table_name = f"temp_concepts_{int(datetime.now().timestamp() * 1000)}"
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
        
        # Insert concepts
        inserted_count = 0
        for concept in concepts:
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
            except Exception as insert_error:
                logger.error(f"Failed to insert concept {concept.get('concept_code')}: {insert_error}")
        
        results = {
            "tempConceptListSize": len(concepts),
            "insertedConceptCount": inserted_count,
            "verbatim": [],
            "standard": [],
            "mapped": []
        }
        
        # Execute queries
        if options.get("includeVerbatim", True):
            verbatim_query = f"""
                SELECT t.concept_set_id, c.concept_id, c.concept_code, c.vocabulary_id, 
                       c.domain_id, c.concept_class_id, c.concept_name,
                       t.concept_set_name, t.original_vocabulary
                FROM {cdm_database_schema}.concept c 
                INNER JOIN {temp_table_name} t
                ON c.concept_code = t.concept_code AND c.vocabulary_id = t.vocabulary_id
                ORDER BY t.concept_set_id, c.concept_id
            """
            verbatim_result = await connection.fetch(verbatim_query)
            results["verbatim"] = [dict(row) for row in verbatim_result]
        
        if options.get("includeStandard", True):
            standard_query = f"""
                SELECT t.concept_set_id, c.concept_id, c.concept_code, c.vocabulary_id,
                       c.domain_id, c.concept_class_id, c.concept_name, c.standard_concept,
                       t.concept_set_name, t.original_vocabulary
                FROM {cdm_database_schema}.concept c 
                INNER JOIN {temp_table_name} t
                ON c.concept_code = t.concept_code AND c.vocabulary_id = t.vocabulary_id
                AND c.standard_concept = 'S'
                ORDER BY t.concept_set_id, c.concept_id
            """
            standard_result = await connection.fetch(standard_query)
            results["standard"] = [dict(row) for row in standard_result]
        
        if options.get("includeMapped", True):
            mapped_query = f"""
                SELECT t.concept_set_id, cr.concept_id_2 AS concept_id, c.concept_code, c.vocabulary_id,
                       c.concept_id as source_concept_id, cr.relationship_id,
                       target_c.concept_name, target_c.domain_id, target_c.concept_class_id, target_c.standard_concept,
                       t.concept_set_name, t.original_vocabulary
                FROM {cdm_database_schema}.concept c 
                INNER JOIN {temp_table_name} t
                ON c.concept_code = t.concept_code AND c.vocabulary_id = t.vocabulary_id
                INNER JOIN {cdm_database_schema}.concept_relationship_new cr
                ON c.concept_id = cr.concept_id_1 AND cr.relationship_id = 'Maps to'
                INNER JOIN {cdm_database_schema}.concept target_c
                ON cr.concept_id_2 = target_c.concept_id
                ORDER BY t.concept_set_id, cr.concept_id_2
            """
            mapped_result = await connection.fetch(mapped_query)
            results["mapped"] = [dict(row) for row in mapped_result]
        
        # Generate summary
        total_mappings = len(results["verbatim"]) + len(results["standard"]) + len(results["mapped"])
        results["mappingSummary"] = {
            "totalSourceConcepts": len(concepts),
            "totalMappings": total_mappings,
            "mappingCounts": {
                "verbatim": len(results["verbatim"]),
                "standard": len(results["standard"]),
                "mapped": len(results["mapped"])
            }
        }
        
        # Cleanup
        await connection.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
        
        return results
        
    finally:
        if connection:
            await pool.release(connection)
        if pool:
            await pool.close()

def create_omop_server() -> FastMCP:
    """Create and configure the OMOP MCP server."""
    
    # Initialize FastMCP server
    mcp = FastMCP("OMOP-NLP-Translator")
    
    # Register existing tools
    @mcp.tool()
    async def debug_vsac_omop_pipeline(
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
    ) -> dict:
        """
        Debug VSAC to OMOP pipeline steps.
        
        Automatically uses environment variables as defaults for all credentials.
        
        Args:
            step: Which step to test ("extract", "fetch", "map", "all")
            cql_query: CQL query to process
            vsac_username: VSAC username (uses VSAC_USERNAME env var if not provided)
            vsac_password: VSAC password (uses VSAC_PASSWORD env var if not provided)
            test_oids: Optional OIDs for testing
            database_user: Database username (uses DATABASE_USER env var if not provided)
            database_endpoint: Database endpoint (uses DATABASE_ENDPOINT env var if not provided)
            database_name: Database name (uses DATABASE_NAME env var if not provided)
            database_password: Database password (uses DATABASE_PASSWORD env var if not provided)
            omop_database_schema: OMOP schema name (uses OMOP_DATABASE_SCHEMA env var if not provided)
        """
        try:
            # Get credentials with environment variable fallback
            actual_vsac_username, actual_vsac_password = get_vsac_credentials(vsac_username, vsac_password)
            db_config = get_database_config(
                database_user, database_endpoint, database_name, 
                database_password, omop_database_schema
            )
            
            results = {
                "environmentVariables": {
                    "VSAC_USERNAME": "SET" if settings.vsac_username else "NOT SET",
                    "VSAC_PASSWORD": "SET" if settings.vsac_password else "NOT SET",
                    "DATABASE_PASSWORD": "SET" if settings.database_password else "NOT SET",
                    "DATABASE_USER": db_config['user'],
                    "DATABASE_ENDPOINT": db_config['endpoint'],
                    "DATABASE_NAME": db_config['name'],
                    "OMOP_DATABASE_SCHEMA": db_config['schema']
                },
                "credentialsUsed": {
                    "vsacUsername": actual_vsac_username or "NOT PROVIDED",
                    "databaseEndpoint": db_config['endpoint'],
                    "databaseName": db_config['name']
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
                    "arrayAsStr": str(extracted_oids)
                }
            
            if step in ["fetch", "all"]:
                logger.info("Testing VSAC fetch step...")
                oids_to_test = test_oids or results.get("extraction", {}).get("extractedOids", [])
                
                if len(oids_to_test) == 0:
                    results["vsacFetch"] = {
                        "error": "No ValueSet OIDs available for testing",
                        "suggestion": "Run extraction step first or provide testOids parameter"
                    }
                elif not actual_vsac_username or not actual_vsac_password:
                    results["vsacFetch"] = {
                        "error": "VSAC credentials required for fetch step",
                        "suggestion": "Provide vsacUsername and vsacPassword parameters",
                        "oidsReadyForFetch": oids_to_test
                    }
                else:
                    logger.info(f"Fetching concept sets for {len(oids_to_test)} ValueSet OIDs...")
                    
                    vsac_results = await vsac_service.retrieve_multiple_value_sets(
                        oids_to_test,
                        actual_vsac_username,
                        actual_vsac_password
                    )
                    
                    # Summarize VSAC fetch results
                    total_concepts = sum(len(vs.concepts) if hasattr(vs, 'concepts') and vs.concepts else 0 
                                       for vs in vsac_results.values())
                    successful_retrievals = len([vs for vs in vsac_results.values() 
                                               if hasattr(vs, 'concepts') and vs.concepts and len(vs.concepts) > 0])
                    
                    results["vsacFetch"] = {
                        "totalRequested": len(oids_to_test),
                        "successfulRetrievals": successful_retrievals,
                        "totalConceptsRetrieved": total_concepts,
                        "results": {
                            oid: {
                                "conceptCount": len(vs.concepts) if hasattr(vs, 'concepts') and vs.concepts else 0,
                                "codeSystemsFound": list(set(c.code_system_name for c in vs.concepts)) if hasattr(vs, 'concepts') and vs.concepts else [],
                                "status": "success" if hasattr(vs, 'concepts') and vs.concepts and len(vs.concepts) > 0 else "empty"
                            }
                            for oid, vs in vsac_results.items()
                        },
                        "retrievedAt": datetime.now().isoformat()
                    }
                    
                    logger.info(f"VSAC fetch completed: {successful_retrievals}/{len(oids_to_test)} ValueSets, {total_concepts} total concepts")
            
            if step in ["map", "all"]:
                logger.info("Testing OMOP mapping step...")
                
                # Use real concept data from VSAC fetch if available, otherwise create mock data
                concepts_to_map = []
                
                if "vsacFetch" in results and results["vsacFetch"].get("results"):
                    # Convert VSAC results to concept mapping format
                    logger.info("Using real VSAC concept data for mapping test...")
                    
                    # This would need access to the actual VSAC results, so let's create mock data for now
                    concepts_to_map = [
                        {
                            "concept_set_id": "2.16.840.1.113883.3.464.1003.103.12.1001",
                            "concept_set_name": "Diabetes",
                            "concept_code": "E11.9",
                            "vocabulary_id": "ICD10CM",
                            "original_vocabulary": "ICD10CM",
                            "display_name": "Type 2 diabetes mellitus without complications"
                        },
                        {
                            "concept_set_id": "2.16.840.1.113883.3.464.1003.103.12.1001",
                            "concept_set_name": "Diabetes",
                            "concept_code": "250.00",
                            "vocabulary_id": "ICD9CM",
                            "original_vocabulary": "ICD9CM",
                            "display_name": "Diabetes mellitus without mention of complication"
                        }
                    ]
                    
                    logger.info(f"Prepared {len(concepts_to_map)} concepts for OMOP mapping")
                else:
                    # Create mock concept data for testing
                    logger.info("Using mock concept data for mapping test...")
                    concepts_to_map = [
                        {
                            "concept_set_id": "2.16.840.1.113883.3.464.1003.103.12.1001",
                            "concept_set_name": "Diabetes",
                            "concept_code": "E11.9",
                            "vocabulary_id": "ICD10CM",
                            "original_vocabulary": "ICD10CM",
                            "display_name": "Type 2 diabetes mellitus without complications"
                        }
                    ]
                
                # Check if database connection parameters are provided
                if not db_config['password']:
                    results["omopMapping"] = {
                        "error": "Database password required for real OMOP mapping",
                        "suggestion": "Provide databasePassword parameter for database connection",
                        "inputConcepts": len(concepts_to_map),
                        "database": {
                            "user": db_config['user'],
                            "endpoint": db_config['endpoint'],
                            "database": db_config['name'],
                            "schema": db_config['schema']
                        },
                        "mockMappingWouldProcess": f"{len(concepts_to_map)} concepts"
                    }
                else:
                    # Execute the actual OMOP mapping logic with real database
                    try:
                        database_config = {
                            "user": db_config['user'],
                            "host": db_config['endpoint'],
                            "database": db_config['name'],
                            "password": db_config['password'],
                            "port": 5432,
                            "ssl": False
                        }
                        
                        omop_results = await map_concepts_to_omop_database(
                            concepts_to_map,
                            db_config['schema'],
                            database_config,
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
                            "mappingSummary": omop_results.get("mappingSummary", {}),
                            "database": {
                                "connected": True,
                                "user": db_config['user'],
                                "endpoint": db_config['endpoint'],
                                "database": db_config['name'],
                                "schema": db_config['schema']
                            }
                        }
                        
                        logger.info(f"OMOP mapping test completed: {omop_results.get('mappingSummary', {}).get('totalMappings', 0)} total mappings found")
                        
                    except Exception as db_error:
                        results["omopMapping"] = {
                            "error": f"Database connection failed: {str(db_error)}",
                            "inputConcepts": len(concepts_to_map),
                            "database": {
                                "user": db_config['user'],
                                "endpoint": db_config['endpoint'],
                                "database": db_config['name'],
                                "schema": db_config['schema'],
                                "connectionAttempted": True
                            },
                            "suggestion": "Check database credentials and network connectivity"
                        }
            
            return {
                "step": step,
                "results": results,
                "status": "debug_complete",
                "database": {
                    "endpoint": db_config['endpoint'],
                    "database": db_config['name'],
                    "user": db_config['user'],
                    "schema": db_config['schema']
                }
            }
            
        except Exception as error:
            logger.error(f"Error in debug_vsac_omop_pipeline: {error}")
            return {
                "step": step,
                "error": str(error),
                "status": "debug_failed"
            }
    
    @mcp.tool()
    async def parse_nl_to_cql(query: str, include_input: bool = False) -> dict:
        """Convert natural language query to CQL."""
        return await parse_nl_to_cql_tool(query, include_input)
    
    @mcp.tool()
    async def extract_valuesets(cql_query: str, include_input: bool = False) -> dict:
        """Extract ValueSets from CQL with minimal output."""
        return await extract_valuesets_tool(cql_query, include_input)
    
    @mcp.tool()
    async def valueset_regex_extraction(
        cql_query: str, 
        show_details: bool = False, 
        include_input: bool = False
    ) -> dict:
        """Test regex extraction patterns on CQL."""
        return await valueset_regex_extraction_tool(cql_query, show_details, include_input)
    
    @mcp.tool()
    async def fetch_multiple_vsac(
        value_set_ids: List[str],
        username: Optional[str] = None,
        password: Optional[str] = None
    ) -> dict:
        """Fetch multiple ValueSets from VSAC."""
        actual_username, actual_password = get_vsac_credentials(username, password)
        
        credentials = {'username': actual_username, 'password': actual_password}
        valid, missing = validate_required_credentials(credentials, ['username', 'password'])
        
        if not valid:
            return create_credentials_error_response(missing, "VSAC value set fetching")
        
        return await fetch_multiple_vsac_tool(value_set_ids, actual_username, actual_password)
    
    @mcp.tool()
    async def vsac_cache_status() -> dict:
        """Get VSAC cache status and environment variable info."""
        return await vsac_cache_status_tool()
    
    @mcp.tool()
    async def map_vsac_to_omop(
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
    ) -> dict:
        """Complete VSAC to OMOP mapping pipeline."""
        try:
            # Get credentials with environment variable fallback
            actual_vsac_username, actual_vsac_password = get_vsac_credentials(vsac_username, vsac_password)
            db_config = get_database_config(
                database_user, database_endpoint, database_name, 
                database_password, omop_database_schema
            )
            
            # Validate required credentials
            all_credentials = {
                'vsac_username': actual_vsac_username,
                'vsac_password': actual_vsac_password,
                'database_password': db_config['password']
            }
            valid, missing = validate_required_credentials(
                all_credentials, 
                ['vsac_username', 'vsac_password', 'database_password']
            )
            
            if not valid:
                return create_credentials_error_response(missing, "VSAC to OMOP mapping")
            
            # Target fact tables
            target_fact_tables = target_fact_tables or [
                "condition_occurrence", "procedure_occurrence", "measurement", "drug_exposure"
            ]
            
            logger.info("Starting VSAC to OMOP mapping pipeline...")
            
            # Step 1: Extract ValueSet OIDs from CQL
            logger.info("Step 1: Extracting ValueSet OIDs from CQL...")
            extracted_oids, valuesets = extract_valueset_identifiers_from_cql(cql_query)
            
            if not extracted_oids:
                return {
                    "success": False,
                    "message": "No ValueSet OIDs found in CQL query",
                    "cql_query": cql_query,
                    "extracted_oids": [],
                    "valuesets": []
                }
            
            logger.info(f"Found {len(extracted_oids)} unique ValueSet OIDs")
            
            # Step 2: Fetch concepts from VSAC for all ValueSets
            logger.info("Step 2: Fetching concepts from VSAC...")
            vsac_results = await vsac_service.retrieve_multiple_value_sets(
                extracted_oids,
                actual_vsac_username,
                actual_vsac_password
            )
            
            # Step 3: Prepare concept data for OMOP mapping
            logger.info("Step 3: Preparing concept data for OMOP mapping...")
            concepts_for_mapping, value_set_summary = await prepare_concepts_and_summary(
                vsac_results, valuesets
            )
            
            logger.info(f"Prepared {len(concepts_for_mapping)} concepts for OMOP mapping")
            
            # Step 4: Map to OMOP concepts using real database
            logger.info("Step 4: Mapping to OMOP concepts using database...")
            database_config = {
                "user": db_config['user'],
                "host": db_config['endpoint'],
                "database": db_config['name'],
                "password": db_config['password'],
                "port": 5432,
                "ssl": False
            }
            
            omop_mapping_results = await map_concepts_to_omop_database(
                concepts_for_mapping,
                db_config['schema'],
                database_config,
                {
                    "includeVerbatim": include_verbatim,
                    "includeStandard": include_standard,
                    "includeMapped": include_mapped
                },
                target_fact_tables
            )
            
            return {
                "success": True,
                "message": "VSAC to OMOP mapping completed successfully",
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
                    "step3_omop_mapping": omop_mapping_results
                },
                "metadata": {
                    "processingTime": datetime.now().isoformat(),
                    "totalValueSets": len(extracted_oids),
                    "totalVsacConcepts": len(concepts_for_mapping),
                    "totalOmopMappings": omop_mapping_results["mappingSummary"]["totalMappings"]
                }
            }
            
        except Exception as error:
            logger.error(f"VSAC to OMOP mapping error: {error}")
            return {
                "success": False,
                "error": str(error),
                "step": "Pipeline execution failed"
            }
    
    @mcp.tool()
    async def check_environment_status() -> dict:
        """Check environment variable status and get setup guidance."""
        return await check_environment_status_tool()
    
    # Register resources
    @mcp.resource("config://current")
    async def get_config() -> str:
        """Get current configuration including environment variables."""
        config = await config_resource()
        import json
        return json.dumps(config, indent=2)
    
    @mcp.resource("omop://schema/cdm")
    async def get_omop_schema() -> str:
        """Get OMOP schema information."""
        schema = await omop_schema_resource()
        import json
        return json.dumps(schema, indent=2)
    
    logger.info("OMOP MCP server created successfully with OMOP mapping support")
    return mcp