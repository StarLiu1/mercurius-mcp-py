import logging
from typing import Optional, List, Dict, Any

from mcp.server.fastmcp import FastMCP, Context
from tools.parse_nl_to_cql import (
    parse_nl_to_cql_tool,
    extract_valuesets_tool,
    valueset_regex_extraction_tool
)
from tools.fetch_vasc import fetch_multiple_vsac_tool, vsac_cache_status_tool
from tools.env_status_tool import check_environment_status_tool
from tools.map_vsac_to_omop import (
    map_vsac_to_omop_tool,
    debug_vsac_omop_pipeline_tool
)
from tools.lookup_loinc_code import lookup_loinc_code_tool
from tools.lookup_snomed_code import lookup_snomed_code_tool
from tools.parse_cql_structure import parse_cql_structure_tool
from tools.extract_valuesets_with_omop import extract_valuesets_with_omop_tool
from tools.generate_omop_sql import generate_omop_sql_tool
from tools.validate_generated_sql import validate_generated_sql_tool
from tools.correct_sql_errors import correct_sql_errors_tool
from tools.finalize_sql import finalize_sql_tool
from tools.translate_cql_to_sql_complete import translate_cql_to_sql_complete_tool


from rag_resources.config import config_resource
from rag_resources.schema import omop_schema_resource
from utils.env_helpers import (
    get_vsac_credentials, 
    get_database_config, 
    validate_required_credentials,
    create_credentials_error_response
)
from utils.extractors import extract_valueset_identifiers_from_cql
from services.vsac_services import vsac_service
from config.settings import settings
from datetime import datetime

logger = logging.getLogger(__name__)

from utils.config import load_config

# Load config once when server starts
CONFIG = load_config()

def create_omop_server() -> FastMCP:
    """Create and configure the OMOP MCP server."""
    
    # Initialize FastMCP server
    mcp = FastMCP("OMOP-NLP-Translator")
    
    # Register tools using imported functions
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
        return await map_vsac_to_omop_tool(
            cql_query,
            vsac_username,
            vsac_password,
            database_user,
            database_endpoint,
            database_name,
            database_password,
            omop_database_schema,
            include_verbatim,
            include_standard,
            include_mapped,
            target_fact_tables
        )
    
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
        """Debug VSAC to OMOP pipeline steps."""
        return await debug_vsac_omop_pipeline_tool(
            step,
            cql_query,
            vsac_username,
            vsac_password,
            test_oids,
            database_user,
            database_endpoint,
            database_name,
            database_password,
            omop_database_schema
        )
    
    @mcp.tool()
    async def lookup_loinc_code(
        code: str,
        display: Optional[str] = None,
        database_user: Optional[str] = None,
        database_endpoint: Optional[str] = None,
        database_name: Optional[str] = None,
        database_password: Optional[str] = None,
        omop_database_schema: Optional[str] = None
    ) -> dict:
        """Look up LOINC code and map to OMOP concepts."""
        return await lookup_loinc_code_tool(
            code, display, database_user, database_endpoint,
            database_name, database_password, omop_database_schema
        )
    
    @mcp.tool()
    async def lookup_snomed_code(
        code: str,
        display: Optional[str] = None,
        database_user: Optional[str] = None,
        database_endpoint: Optional[str] = None,
        database_name: Optional[str] = None,
        database_password: Optional[str] = None,
        omop_database_schema: Optional[str] = None
    ) -> dict:
        """Look up SNOMED code and map to OMOP concepts."""
        return await lookup_snomed_code_tool(
            code, display, database_user, database_endpoint,
            database_name, database_password, omop_database_schema
        )
    
    @mcp.tool()
    async def parse_cql_structure(
        cql_content: str,
        cql_file_path: Optional[str] = None
    ) -> dict:
        """Parse CQL structure and analyze dependencies using LLM."""
        return await parse_cql_structure_tool(
            cql_content, cql_file_path, CONFIG
        )
    
    @mcp.tool()
    async def extract_valuesets_with_omop(
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
    ) -> dict:
        """Extract ValueSets with OMOP mapping."""
        return await extract_valuesets_with_omop_tool(
            cql_content, library_files, parsed_structure,
            vsac_username, vsac_password, database_user,
            database_endpoint, database_name, database_password, omop_database_schema
        )
    
    @mcp.tool()
    async def generate_omop_sql(
        parsed_structure: Dict[str, Any],
        all_valuesets: Dict[str, Any],
        cql_content: str,
        placeholder_mappings: Optional[Dict[str, Any]] = None,
        dependency_analysis: Optional[Dict[str, Any]] = None,
        library_definitions: Optional[Dict[str, Any]] = None,
        valueset_registry: Optional[Dict[str, Any]] = None,
        individual_codes: Optional[Dict[str, Any]] = None,
        sql_dialect: str = "postgresql"
    ) -> dict:
        """Generate OMOP SQL from parsed CQL using LLM."""
        return await generate_omop_sql_tool(
            parsed_structure, all_valuesets, cql_content, dependency_analysis,
            library_definitions, valueset_registry, individual_codes,
            sql_dialect, CONFIG
        )   
    
    @mcp.tool()
    async def validate_generated_sql(
        sql_query: str,
        parsed_structure: Dict[str, Any],
        all_valuesets: Optional[Dict[str, Any]] = None,
        sql_dialect: str = "postgresql"
    ) -> dict:
        """Validate generated SQL semantically and syntactically using LLM."""
        return await validate_generated_sql_tool(
            sql_query, parsed_structure, all_valuesets, sql_dialect, CONFIG
        )
    
    @mcp.tool()
    async def correct_sql_errors(
        sql_query: str,
        validation_result: Dict[str, Any],
        parsed_structure: Optional[Dict[str, Any]] = None,
        sql_dialect: str = "postgresql"
    ) -> dict:
        """Correct SQL errors using LLM."""
        return await correct_sql_errors_tool(
            sql_query, validation_result, parsed_structure, sql_dialect, CONFIG
        )
    
    @mcp.tool()
    async def finalize_sql(
        sql_query: str,
        placeholder_mappings: Dict[str, List[str]],
        sql_dialect: str = "postgresql"
    ) -> dict:
        return await finalize_sql_tool(sql_query, placeholder_mappings, sql_dialect)
    
    @mcp.tool()
    async def translate_cql_to_sql_complete(
        cql_content: str,
        ctx: Context,
        cql_file_path: Optional[str] = None,
        sql_dialect: str = "postgresql",
        validate: bool = True,
        correct_errors: bool = True,
        vsac_username: Optional[str] = None,
        vsac_password: Optional[str] = None,
        database_user: Optional[str] = None,
        database_endpoint: Optional[str] = None,
        database_name: Optional[str] = None,
        database_password: Optional[str] = None,
        omop_database_schema: Optional[str] = None
    ) -> dict:
        """Complete CQL to SQL translation pipeline."""
        return await translate_cql_to_sql_complete_tool(
        cql_content, Context, cql_file_path, sql_dialect, validate, correct_errors,
        CONFIG, vsac_username, vsac_password, database_user, 
        database_endpoint, database_name, database_password, omop_database_schema
        )
    
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
    
    @mcp.prompt()
    async def translate_cql_measure_workflow() -> str:
        """Workflow for translating CQL measures to OMOP SQL."""
        return """
    # CQL to OMOP SQL Translation Workflow

    To translate a CQL measure to SQL, follow these steps in order, call the functions in sequence without manually passing the complex structures:

    ## Step 1: Parse CQL Structure
    Call: `parse_cql_structure(cql_content, cql_file_path?)`
    Returns: parsed_structure, library_files, dependency_analysis

    ## Step 2: Extract ValueSets with OMOP Mapping
    Call: `extract_valuesets_with_omop(cql_content, library_files, parsed_structure, credentials...)`
    Returns: all_valuesets, placeholder_mappings, valueset_registry

    ## Before you proceed to the next step, ask for permission.

    ## Step 3: Generate SQL
    Call: `generate_omop_sql(parsed_structure, all_valuesets, cql_content, placeholder_mappings, ...)`
    Returns: sql (with placeholders)

    ## Before you proceed to the next step, ask for permission.

    ## Step 4: Validate SQL (Optional)
    Call: `validate_generated_sql(sql_query, parsed_structure, all_valuesets, sql_dialect)`
    Returns: validation_result with issues

    ## Before you proceed to the next step, ask for permission.

    ## Step 5: Correct Errors (Optional)
    If validation found errors:
    Call: `correct_sql_errors(sql_query, validation_result, parsed_structure, sql_dialect)`
    Returns: corrected_sql

    ## Before you proceed to the next step, ask for permission.

    ## Step 6: Finalize SQL
    Call: `finalize_sql(sql_query, placeholder_mappings, sql_dialect)`
    Returns: final_sql (ready to execute)

    ## Step 7: Create an Artifact of the final SQL.
    """
    
    logger.info("OMOP MCP server created successfully with OMOP mapping support")
    return mcp