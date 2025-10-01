"""
Tool 5: Correct SQL errors based on validation feedback using LLM.
"""

import logging
from typing import Dict, Any, Optional
import yaml

from services.sql_corrector import SQLCorrector

logger = logging.getLogger(__name__)


def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """Load configuration from YAML file."""
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Expand environment variables
        import os
        def expand_env_vars(obj):
            if isinstance(obj, dict):
                return {k: expand_env_vars(v) for k, v in obj.items()}
            elif isinstance(obj, str) and obj.startswith('${') and obj.endswith('}'):
                env_var = obj[2:-1]
                return os.getenv(env_var, obj)
            return obj
        
        return expand_env_vars(config)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        raise


async def correct_sql_errors_tool(
    sql_query: str,
    validation_result: Dict[str, Any],
    parsed_structure: Optional[Dict[str, Any]] = None,
    sql_dialect: str = "postgresql",
    config_path: str = "config.yaml"
) -> Dict[str, Any]:
    """
    Tool 5: Correct SQL errors based on validation feedback using LLM.
    
    Takes validation errors and uses LLM to fix:
    - Syntax errors for target dialect
    - Missing OMOP tables/columns
    - Invalid SQL constructs
    - Dialect-specific issues (e.g., QUALIFY clause in PostgreSQL)
    
    CRITICAL: Preserves all PLACEHOLDER_* patterns unchanged
    
    Args:
        sql_query: SQL query with validation errors
        validation_result: Validation result from Tool 4
        parsed_structure: Parsed CQL structure (optional, for context)
        sql_dialect: Target SQL dialect (postgresql, snowflake, bigquery, sqlserver)
        config_path: Path to config.yaml
        
    Returns:
        Dict with:
        - corrected_sql: Fixed SQL query
        - changes_made: List of changes applied
        - success: Boolean indicating if correction succeeded
        - original_sql: Original SQL for reference
    """
    try:
        logger.info("=" * 80)
        logger.info(f"TOOL 5: Correcting SQL Errors for {sql_dialect.upper()}")
        logger.info("=" * 80)
        
        # Check if there are actually errors to fix
        if validation_result.get('valid', True):
            logger.info("No errors to correct - SQL is valid")
            return {
                "success": True,
                "corrected_sql": sql_query,
                "changes_made": [],
                "original_sql": sql_query,
                "message": "No corrections needed - SQL passed validation"
            }
        
        # Extract error issues
        issues = validation_result.get('issues', [])
        errors = [i for i in issues if i.get('severity') == 'error']
        
        if not errors:
            logger.info("No error-level issues found")
            return {
                "success": True,
                "corrected_sql": sql_query,
                "changes_made": [],
                "original_sql": sql_query,
                "message": "No error-level issues to correct"
            }
        
        logger.info(f"Found {len(errors)} errors to correct:")
        for error in errors:
            logger.info(f"  - {error.get('message')}")
        
        # Load configuration
        config = load_config(config_path)
        logger.info(f"Using LLM provider: {config.get('model_provider')}")
        
        # Initialize SQL corrector
        corrector = SQLCorrector(config)
        
        logger.info("Calling LLM to correct SQL errors...")
        correction_result = corrector.correct_sql(
            sql_query=sql_query,
            validation_result=validation_result,
            dialect=sql_dialect,
            cql_structure=parsed_structure
        )
        
        # Extract results
        corrected_sql = correction_result.get('corrected_sql', sql_query)
        changes_made = correction_result.get('changes_made', [])
        success = correction_result.get('success', False)
        
        # Check if SQL actually changed
        sql_changed = corrected_sql != sql_query
        
        logger.info("=" * 80)
        if success:
            logger.info("✓ Tool 5 Complete: SQL Corrected")
            logger.info(f"  - Changes made: {len(changes_made)}")
            logger.info(f"  - SQL modified: {sql_changed}")
            
            if changes_made:
                logger.info("  Changes applied:")
                for change in changes_made:
                    logger.info(f"    - {change}")
        else:
            logger.error("✗ Tool 5 Failed: Could not correct SQL")
            logger.error(f"  - Error: {correction_result.get('error', 'Unknown error')}")
        
        logger.info("=" * 80)
        
        return {
            "success": success,
            "corrected_sql": corrected_sql,
            "changes_made": changes_made,
            "original_sql": sql_query,
            "sql_changed": sql_changed,
            "errors_addressed": len(errors)
        }
        
    except Exception as e:
        logger.error(f"Tool 5 failed: {e}", exc_info=True)
        return {
            "success": False,
            "corrected_sql": sql_query,
            "changes_made": [],
            "original_sql": sql_query,
            "error": str(e),
            "step": "correct_sql_errors"
        }