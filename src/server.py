import logging
from mcp.server.fastmcp import FastMCP
from tools.parse_nl_to_cql import (
    parse_nl_to_cql_tool,
    extract_valuesets_tool,
    valueset_regex_extraction_tool
)
from tools.fetch_vasc import fetch_multiple_vsac_tool, vsac_cache_status_tool
from tools.map_vsac_to_omop import map_vsac_to_omop_tool, debug_vsac_omop_pipeline_tool
from tools.env_status_tool import check_environment_status_tool
from resources.config import config_resource
from resources.schema import omop_schema_resource

logger = logging.getLogger(__name__)


def create_omop_server() -> FastMCP:
    """Create and configure the OMOP MCP server."""
    
    # Initialize FastMCP server
    mcp = FastMCP("OMOP-NLP-Translator")
    
    # Register tools with automatic environment variable defaults
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
        value_set_ids: list[str],
        username: str = None,
        password: str = None
    ) -> dict:
        """
        Fetch multiple ValueSets from VSAC.
        
        Args:
            value_set_ids: List of ValueSet OIDs to fetch
            username: VSAC username (auto-uses VSAC_USERNAME env var if not provided)
            password: VSAC password (auto-uses VSAC_PASSWORD env var if not provided)
        """
        return await fetch_multiple_vsac_tool(value_set_ids, username, password)
    
    @mcp.tool()
    async def vsac_cache_status() -> dict:
        """Get VSAC cache status and environment variable info."""
        return await vsac_cache_status_tool()
    
    @mcp.tool()
    async def map_vsac_to_omop(
        cql_query: str,
        vsac_username: str = None,
        vsac_password: str = None,
        database_user: str = None,
        database_endpoint: str = None,
        database_name: str = None,
        database_password: str = None,
        omop_database_schema: str = None,
        include_verbatim: bool = True,
        include_standard: bool = True,
        include_mapped: bool = True,
        target_fact_tables: list[str] = None
    ) -> dict:
        """
        Complete VSAC to OMOP mapping pipeline.
        
        Automatically uses environment variables as defaults:
        - vsac_username: Uses VSAC_USERNAME env var
        - vsac_password: Uses VSAC_PASSWORD env var  
        - database_user: Uses DATABASE_USER env var
        - database_endpoint: Uses DATABASE_ENDPOINT env var
        - database_name: Uses DATABASE_NAME env var
        - database_password: Uses DATABASE_PASSWORD env var
        - omop_database_schema: Uses OMOP_DATABASE_SCHEMA env var
        
        Args:
            cql_query: CQL query containing ValueSet references
            vsac_username: VSAC username (optional - uses env var)
            vsac_password: VSAC password (optional - uses env var)
            database_user: Database username (optional - uses env var)
            database_endpoint: Database endpoint (optional - uses env var)
            database_name: Database name (optional - uses env var)
            database_password: Database password (optional - uses env var)
            omop_database_schema: OMOP CDM schema name (optional - uses env var)
            include_verbatim: Include verbatim mappings
            include_standard: Include standard concept mappings
            include_mapped: Include mapped concept mappings
            target_fact_tables: Target OMOP fact tables
        """
        return await map_vsac_to_omop_tool(
            cql_query, vsac_username, vsac_password, database_user,
            database_endpoint, database_name, database_password,
            omop_database_schema, include_verbatim, include_standard,
            include_mapped, target_fact_tables
        )
    
    @mcp.tool()
    async def debug_vsac_omop_pipeline(
        step: str,
        cql_query: str,
        vsac_username: str = None,
        vsac_password: str = None,
        test_oids: list[str] = None,
        database_user: str = None,
        database_endpoint: str = None,
        database_name: str = None,
        database_password: str = None,
        omop_database_schema: str = None
    ) -> dict:
        """
        Debug VSAC to OMOP pipeline steps.
        
        Automatically uses environment variables as defaults for all credentials.
        
        Args:
            step: Which step to test ("extract", "fetch", "map", "all")
            cql_query: CQL query to process
            vsac_username: VSAC username (optional - uses env var)
            vsac_password: VSAC password (optional - uses env var)
            test_oids: Optional OIDs for testing
            database_user: Database username (optional - uses env var)
            database_endpoint: Database endpoint (optional - uses env var)
            database_name: Database name (optional - uses env var)
            database_password: Database password (optional - uses env var)
            omop_database_schema: OMOP schema name (optional - uses env var)
        """
        return await debug_vsac_omop_pipeline_tool(
            step, cql_query, vsac_username, vsac_password, test_oids,
            database_user, database_endpoint, database_name,
            database_password, omop_database_schema
        )
    
    @mcp.tool()
    async def check_environment_status() -> dict:
        """
        Check environment variable status and get setup guidance.
        
        Shows which credentials are configured and provides setup instructions
        for missing environment variables.
        """
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
    
    logger.info("OMOP MCP server created successfully with auto environment variable support")
    return mcp