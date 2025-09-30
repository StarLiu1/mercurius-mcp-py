"""
SQL corrector that fixes SQL based on validation feedback.
"""

import json
import logging
from typing import Dict, Any, List
from src.services.llm_factory import LLMFactory
from src.services. import unwrap_json_response

logger = logging.getLogger(__name__)


class SQLCorrector:
    """LLM-based SQL corrector that fixes issues found during validation."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize SQL corrector with LLM configuration."""
        self.config = config
        self.component_name = 'sql_corrector'

        # Use LLMFactory to create client
        self.client, self.model = LLMFactory.create_component_client(config, self.component_name)
        logger.info(f"SQLCorrector initialized with model: {self.model}")
    
    def correct_sql(
        self,
        sql_query: str,
        validation_result: Dict[str, Any],
        dialect: str = "postgresql",
        cql_structure: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Correct SQL based on validation feedback.
        
        Args:
            sql_query: The SQL query with validation issues
            validation_result: Validation result with issues to fix
            dialect: Target SQL dialect
            cql_structure: Original CQL structure for reference
            
        Returns:
            Dict with corrected SQL and changes made
        """
        logger.info(f"Correcting SQL for {dialect} dialect based on validation feedback")
        
        # Extract error issues from validation
        errors = [
            issue for issue in validation_result.get("issues", [])
            if issue.get("severity") == "error"
        ]
        
        if not errors:
            logger.info("No errors to correct")
            return {
                "corrected_sql": sql_query,
                "changes_made": [],
                "success": True
            }
        
        # Build correction prompt
        prompt = self._build_correction_prompt(
            sql_query, errors, dialect, validation_result
        )
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": self._get_system_prompt(dialect)
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)

            # Use universal unwrapper to handle any wrapper format
            result = unwrap_json_response(result)

            # Log corrections made
            if result.get("changes_made"):
                logger.info(f"Made {len(result['changes_made'])} corrections:")
                for change in result["changes_made"]:
                    logger.info(f"  - {change}")
            
            return result
            
        except Exception as e:
            logger.error(f"SQL correction failed: {e}")
            return {
                "corrected_sql": sql_query,
                "changes_made": [],
                "success": False,
                "error": str(e)
            }
    
    def _get_system_prompt(self, dialect: str) -> str:
        """Get system prompt for SQL correction."""
        return f"""You are an expert SQL developer specializing in {dialect.upper()} and OMOP CDM.
Your task is to correct SQL queries based on validation feedback while preserving all PLACEHOLDER_* patterns.

Key Rules:
1. Fix ONLY the specific errors mentioned in the validation feedback
2. NEVER replace or modify PLACEHOLDER_* patterns - they must be preserved exactly
3. Maintain the overall query structure and logic
4. Ensure the corrected SQL is valid for {dialect} dialect
5. Preserve all CTEs and their relationships
6. Keep all OMOP table and column references accurate

CRITICAL PLACEHOLDER RULES:
- NEVER modify placeholder patterns: Keep all PLACEHOLDER_* tokens EXACTLY as they appear
- Keep placeholders in their SIMPLEST form: IN (PLACEHOLDER_NAME)
- NEVER transform placeholders into subquery patterns like IN (SELECT value FROM PLACEHOLDER_NAME)
- NEVER create SELECT value FROM patterns with placeholders
- Placeholders will be replaced with simple lists of concept IDs later
- The goal is syntactically valid SQL that preserves placeholders for later replacement
- Modifying placeholders will break the pipeline - they must remain unchanged

Example:
- CORRECT: WHERE concept_id IN (PLACEHOLDER_DIABETES)
- WRONG: WHERE concept_id IN (SELECT value FROM PLACEHOLDER_DIABETES)
- WRONG: WHERE concept_id IN (SELECT value FROM (PLACEHOLDER_DIABETES))

Return JSON with this structure:
{{
    "corrected_sql": "the complete corrected SQL query",
    "changes_made": ["list of specific changes made"],
    "success": true/false
}}"""
    
    def _build_correction_prompt(
        self,
        sql_query: str,
        errors: List[Dict[str, Any]],
        dialect: str,
        validation_result: Dict[str, Any]
    ) -> str:
        """Build the correction prompt."""
        
        # Format errors for clarity
        error_descriptions = []
        for error in errors:
            desc = f"- {error['message']}"
            if error.get('location'):
                desc += f" (Location: {error['location']})"
            if error.get('suggestion'):
                desc += f"\n  Suggestion: {error['suggestion']}"
            error_descriptions.append(desc)
        
        return f"""Fix the following SQL query based on the validation errors found.

Target Dialect: {dialect.upper()}

ORIGINAL SQL:
{sql_query}

VALIDATION ERRORS TO FIX:
{chr(10).join(error_descriptions)}

IMPORTANT REQUIREMENTS:
1. Fix ALL the errors listed above
2. PRESERVE all PLACEHOLDER_* patterns exactly as they are (do not replace them)
3. Ensure the corrected SQL is valid for {dialect} dialect
4. Maintain the semantic meaning and structure of the query
5. If a QUALIFY clause is present and not supported in {dialect}, rewrite using window functions with CTEs

Specific {dialect} considerations:
- PostgreSQL: No QUALIFY clause, use window functions with CTEs instead
- PostgreSQL: Use INTERVAL for date arithmetic (e.g., date + INTERVAL '6 months')
- PostgreSQL: Use DATE_PART for date extraction
- Snowflake: QUALIFY clause is supported
- BigQuery: QUALIFY clause is supported, use DATE_ADD/DATE_DIFF
- SQL Server: No QUALIFY, use TOP instead of LIMIT

Return the complete corrected SQL query with all issues fixed."""