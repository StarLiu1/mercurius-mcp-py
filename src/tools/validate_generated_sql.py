"""
Tool 4: Validate generated SQL semantically and syntactically using LLM.
"""

import logging
from typing import Dict, Any, Optional, Union
import yaml
from utils.parameter_normalizer import normalize_dict_param, normalize_string_param, log_parameter_types

from services.sql_validator import SQLValidator

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


async def validate_generated_sql_tool(
    sql_query: str,
    parsed_structure: Union[Dict[str, Any], str],
    all_valuesets: Optional[Union[Dict[str, Any], str]] = None,
    sql_dialect: str = "postgresql",
    config: Union[Dict[str, Any], str] = None
) -> Dict[str, Any]:
    """
    Tool 4: Validate generated SQL semantically and syntactically using LLM.
    
    Performs comprehensive validation:
    - Syntax validation for target SQL dialect
    - Semantic validation against CQL intent
    - Completeness checks (all populations, valuesets, etc.)
    - OMOP CDM compliance
    - Performance considerations
    
    Args:
        sql_query: Generated SQL to validate
        parsed_structure: Parsed CQL structure from Tool 1
        all_valuesets: Valuesets from Tool 2 (optional)
        sql_dialect: Target SQL dialect (postgresql, snowflake, bigquery, sqlserver)
        config_path: Path to config.yaml
        
    Returns:
        Dict with:
        - valid: Boolean indicating if SQL is valid
        - issues: List of validation issues (errors, warnings, info)
        - statistics: Query statistics
        - improvements: Suggested improvements
        - dialect: Target dialect validated against
    """
    try:

        parsed_structure = normalize_dict_param(parsed_structure, "parsed_structure", required=True)
        all_valuesets = normalize_dict_param(all_valuesets, "all_valuesets", required=True)
        sql_dialect = normalize_string_param(sql_dialect, "sql_dialect", default="postgresql")
        sql_dialect = sql_dialect.lower().strip()
        
        logger.info("=" * 80)
        logger.info(f"TOOL 4: Validating SQL for {sql_dialect.upper()}")
        logger.info("=" * 80)
        
        if not sql_query:
            return {
                "success": False,
                "valid": False,
                "issues": [{
                    "severity": "error",
                    "category": "completeness",
                    "message": "No SQL query provided for validation"
                }],
                "step": "validate_generated_sql"
            }
        
        # Load configuration
        if config is None:
            from utils.config import load_config
            config = load_config()
        logger.info(f"Using LLM provider: {config.get('model_provider')}")
        
        # Initialize SQL validator
        validator = SQLValidator(config)
        
        logger.info(f"SQL length: {len(sql_query):,} characters")
        logger.info(f"Target dialect: {sql_dialect}")
        
        # Validate with LLM
        logger.info("Calling LLM to validate SQL...")
        validation_result = validator.validate(
            sql_query=sql_query,
            cql_structure=parsed_structure,
            dialect=sql_dialect,
            valuesets=all_valuesets
        )
        
        # Extract validation components
        valid = validation_result.valid
        issues = [issue.dict() for issue in validation_result.issues]
        statistics = validation_result.statistics
        improvements = validation_result.improvements
        
        # Categorize issues by severity
        errors = [i for i in issues if i.get('severity') == 'error']
        warnings = [i for i in issues if i.get('severity') == 'warning']
        info = [i for i in issues if i.get('severity') == 'info']
        
        logger.info("=" * 80)
        logger.info(f"✓ Tool 4 Complete: SQL Validation {'PASSED' if valid else 'FAILED'}")
        logger.info(f"  - Valid: {valid}")
        logger.info(f"  - Errors: {len(errors)}")
        logger.info(f"  - Warnings: {len(warnings)}")
        logger.info(f"  - Info: {len(info)}")
        logger.info(f"  - Improvements suggested: {len(improvements)}")
        
        if errors:
            logger.error("  Validation Errors:")
            for error in errors:
                logger.error(f"    - {error.get('message')}")
        
        if warnings:
            logger.warning("  Validation Warnings:")
            for warning in warnings:
                logger.warning(f"    - {warning.get('message')}")
        
        logger.info("=" * 80)
        
        return {
            "success": True,
            "valid": valid,
            "dialect": sql_dialect,
            "issues": issues,
            "statistics": statistics,
            "improvements": improvements,
            "issue_counts": {
                "errors": len(errors),
                "warnings": len(warnings),
                "info": len(info)
            }
        }
        
    except Exception as e:
        logger.error(f"Tool 4 failed: {e}", exc_info=True)
        return {
            "success": False,
            "valid": False,
            "error": str(e),
            "step": "validate_generated_sql"
        }