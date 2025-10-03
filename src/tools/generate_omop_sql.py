"""
Tool 3: Generate OMOP SQL from parsed CQL using LLM.
"""

import logging
from typing import Dict, Any, Optional
import yaml

from services.sql_generator import SimpleSQLGenerator

logger = logging.getLogger(__name__)


# def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
#     """Load configuration from YAML file."""
#     try:
#         with open(config_path, 'r') as f:
#             config = yaml.safe_load(f)
        
#         # Expand environment variables
#         import os
#         def expand_env_vars(obj):
#             if isinstance(obj, dict):
#                 return {k: expand_env_vars(v) for k, v in obj.items()}
#             elif isinstance(obj, str) and obj.startswith('${') and obj.endswith('}'):
#                 env_var = obj[2:-1]
#                 return os.getenv(env_var, obj)
#             return obj
        
#         return expand_env_vars(config)
#     except Exception as e:
#         logger.error(f"Failed to load config: {e}")
#         raise


async def generate_omop_sql_tool(
    parsed_structure: Dict[str, Any],
    all_valuesets: Dict[str, Any],
    cql_content: str,
    placeholder_mappings: Optional[Dict[str, Any]] = None,  # ADD THIS
    dependency_analysis: Optional[Dict[str, Any]] = None,
    library_definitions: Optional[Dict[str, Any]] = None,
    valueset_registry: Optional[Dict[str, Any]] = None,
    individual_codes: Optional[Dict[str, Any]] = None,
    sql_dialect: str = "postgresql",
    config: Dict[str, Any] = None 
) -> Dict[str, Any]:
    """
    Tool 3: Generate OMOP SQL from parsed CQL structure using LLM.
    
    Uses SimpleSQLGenerator with full context:
    - Parsed CQL structure
    - All valuesets (main + libraries)
    - Library definitions
    - Dependency analysis
    - Individual LOINC/SNOMED codes
    
    Args:
        parsed_structure: Parsed CQL from Tool 1
        all_valuesets: Valuesets from Tool 2
        cql_content: Original CQL content
        placeholder_mappings: Placeholder -> concept_ids from Tool 2  # ADD THIS
        dependency_analysis: Library dependency info from Tool 1
        library_definitions: Parsed library structures from Tool 1
        valueset_registry: Complete valueset registry from Tool 2
        individual_codes: Individual code mappings from Tool 2
        sql_dialect: Target SQL dialect (postgresql, snowflake, bigquery, sqlserver)
        config_path: Path to config.yaml
        
    Returns:
        Dict with:
        - sql: Complete SQL query with placeholders
        - ctes: List of CTE names
        - main_query: Final SELECT statement
        - placeholders_used: List of placeholders in SQL
        - statistics: Generation statistics
    """
    try:
        logger.info("=" * 80)
        logger.info(f"TOOL 3: Generating OMOP SQL for {sql_dialect}")
        logger.info("=" * 80)
        
        # Load configuration
        if config is None:
            from utils.config import load_config
            config = load_config()
        config['sql_dialect'] = sql_dialect
        
        logger.info(f"Using LLM provider: {config.get('model_provider')}")
        logger.info(f"Target dialect: {sql_dialect}")
        
        # Initialize SQL generator
        generator = SimpleSQLGenerator(config)
        
        # Create valueset hints for OID-based placeholders
        valueset_hints = {}
        if valueset_registry:
            for oid, vs_data in valueset_registry.items():
                clean_oid = oid.replace(".", "_").replace("-", "_")
                placeholder = f"PLACEHOLDER_{clean_oid}"
                valueset_hints[oid] = {
                    'name': vs_data.get('name', ''),
                    'placeholder': placeholder  # Add explicit placeholder
                }
        
        logger.info(f"Context provided:")
        logger.info(f"  - Valuesets: {len(all_valuesets)}")
        logger.info(f"  - Registry entries: {len(valueset_registry or {})}")
        logger.info(f"  - Individual codes: {len(individual_codes or {})}")
        logger.info(f"  - Library definitions: {len(library_definitions or {})}")
        logger.info(f"  - Valueset hints: {len(valueset_hints)}")
        logger.info(f"  - Placeholder mappings: {len(placeholder_mappings or {})}")  # ADD THIS

        
        # Generate SQL with full context
        logger.info("Calling LLM to generate SQL...")
        result = generator.generate(
            parsed_cql=parsed_structure,
            valuesets=all_valuesets,
            cql_content=cql_content,
            dependency_analysis=dependency_analysis,
            library_definitions=library_definitions,
            valueset_registry=valueset_registry,
            individual_codes=individual_codes,
            dialect=sql_dialect,
            valueset_hints=valueset_hints
        )
        
        # Check for errors
        if result.get('error'):
            logger.error(f"SQL generation error: {result['error']}")
            return {
                "success": False,
                "error": result['error'],
                "step": "generate_omop_sql"
            }
        
        # Extract SQL components
        sql_query = result.get('sql', '')
        ctes = result.get('ctes', [])
        main_query = result.get('main_query', '')
        placeholders = result.get('placeholders_used', [])
        
        # Find placeholders in SQL if not provided
        if not placeholders and sql_query:
            import re
            placeholders = list(set(re.findall(r'PLACEHOLDER_[\w_]+', sql_query)))
        
        # Compile statistics
        statistics = {
            "sql_length": len(sql_query),
            "cte_count": len(ctes),
            "placeholder_count": len(placeholders),
            "sql_dialect": sql_dialect,
            "library_context_used": bool(library_definitions),
            "individual_codes_used": bool(individual_codes)
        }
        
        logger.info("=" * 80)
        logger.info("✓ Tool 3 Complete: SQL Generated")
        logger.info(f"  - SQL length: {len(sql_query):,} characters")
        logger.info(f"  - CTEs: {len(ctes)}")
        logger.info(f"  - Placeholders: {len(placeholders)}")
        logger.info(f"  - Dialect: {sql_dialect}")
        logger.info("=" * 80)
        
        return {
            "success": True,
            "sql": sql_query,
            "ctes": ctes,
            "main_query": main_query,
            "placeholders_used": placeholders,
            "placeholder_mappings": placeholder_mappings,  # ADD THIS - pass through for Tool 6
            "statistics": statistics
        }
        
    except Exception as e:
        logger.error(f"Tool 3 failed: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e),
            "step": "generate_omop_sql"
        }