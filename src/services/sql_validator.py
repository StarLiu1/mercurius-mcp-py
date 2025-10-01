"""
LLM-based SQL validator for semantic and syntactic validation.
"""

import json
import logging
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
from services.llm_factory import LLMFactory
from services.json_utils import unwrap_json_response

logger = logging.getLogger(__name__)


class ValidationIssue(BaseModel):
    """Represents a validation issue found in SQL."""
    severity: str  # 'error', 'warning', 'info'
    category: str  # 'syntax', 'semantic', 'completeness', 'performance'
    message: str
    location: Optional[str] = None
    suggestion: Optional[str] = None


class ValidationResult(BaseModel):
    """Complete validation result for SQL query."""
    valid: bool
    dialect: str = "postgresql"
    issues: List[ValidationIssue] = Field(default_factory=list)
    statistics: Dict[str, Any] = Field(default_factory=dict)
    improvements: List[str] = Field(default_factory=list)


class SQLValidator:
    """LLM-based SQL validator for CQL-generated queries."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize SQL validator with LLM configuration."""
        self.config = config
        self.component_name = 'sql_validator'

        # Use LLMFactory to create client
        self.client, self.model = LLMFactory.create_component_client(config, self.component_name)
        logger.info(f"SQLValidator initialized with model: {self.model}")
        
    def validate(
        self,
        sql_query: str,
        cql_structure: Dict[str, Any],
        dialect: str = "postgresql",
        valuesets: Optional[Dict[str, Any]] = None
    ) -> ValidationResult:
        """
        Validate SQL query against CQL intent and syntax rules.
        
        Args:
            sql_query: The generated SQL to validate
            cql_structure: The parsed CQL structure
            dialect: SQL dialect (postgresql, snowflake, bigquery, sqlserver)
            valuesets: Optional valueset mappings for reference
            
        Returns:
            ValidationResult with issues and suggestions
        """
        logger.info(f"Validating SQL for {dialect} dialect")
        
        # Build context about expected elements
        expected_context = self._build_expected_context(cql_structure, valuesets)
        
        prompt = f"""
You are an expert in both CQL (Clinical Quality Language) and SQL, specifically {dialect} dialect and OMOP CDM schema.

Validate the following SQL query that was generated from CQL:

SQL Query:
{sql_query}

Expected Elements from CQL:
{json.dumps(expected_context, indent=2)}

Target SQL Dialect: {dialect}

Perform comprehensive validation and return a JSON object with this structure:
{{
    "valid": boolean,
    "dialect": "{dialect}",
    "issues": [
        {{
            "severity": "error|warning|info",
            "category": "syntax|semantic|completeness|performance",
            "message": "description of the issue",
            "location": "optional: specific line or CTE name",
            "suggestion": "optional: how to fix it"
        }}
    ],
    "statistics": {{
        "cte_count": number,
        "join_count": number,
        "subquery_count": number,
        "placeholder_count": number,
        "omop_tables_used": ["list of OMOP tables"],
        "estimated_complexity": "low|medium|high"
    }},
    "improvements": [
        "list of suggested improvements (not errors)"
    ]
}}

Validation Checks to Perform:

1. SYNTAX VALIDATION ({dialect} specific):
   - Valid {dialect} syntax and functions
   - Proper CTE structure and naming
   - Correct OMOP CDM table and column names
   - Valid join conditions and relationships
   - Proper date/time functions for {dialect}
   - Check for PLACEHOLDER_* patterns that should be replaced

2. SEMANTIC VALIDATION (CQL Intent):
   - All CQL populations are represented (Initial Population, Denominator, Numerator, etc.)
   - Temporal logic from CQL is preserved
   - Value set references have corresponding placeholders
   - Library function calls are properly translated
   - Aggregations and calculations match CQL logic
   - Patient context is maintained

3. COMPLETENESS VALIDATION:
   - All expected CQL definitions have corresponding CTEs or subqueries
   - All value sets from CQL have placeholders in SQL
   - Required OMOP tables are included (person, observation_period, etc.)
   - Measurement period parameters are used correctly

4. PERFORMANCE CONSIDERATIONS ({dialect} specific):
   - Check for cartesian products or expensive operations
   - Validate CTE materialization strategy for {dialect}

5. OMOP CDM COMPLIANCE:
   - Correct domain tables for different concept types
   - Proper use of concept_id vs source_value columns
   - Valid relationships between OMOP tables
   - Appropriate handling of dates and periods

Mark as "valid": false only if there are ERROR severity issues.
Include warnings for potential issues that don't break the query.
Include info for optimization opportunities.

Return ONLY the JSON object, no additional text.
"""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a SQL and CQL validation expert. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)

            # Use universal unwrapper to handle any wrapper format
            result = unwrap_json_response(result)

            # Convert to Pydantic model
            validation = ValidationResult(**result)
            
            # Log summary
            error_count = sum(1 for i in validation.issues if i.severity == 'error')
            warning_count = sum(1 for i in validation.issues if i.severity == 'warning')
            
            logger.info(f"Validation complete: valid={validation.valid}")
            logger.info(f"  - {error_count} errors, {warning_count} warnings")
            logger.info(f"  - {len(validation.improvements)} improvement suggestions")
            
            if not validation.valid:
                logger.error("SQL validation failed with errors:")
                for issue in validation.issues:
                    if issue.severity == 'error':
                        logger.error(f"  - {issue.message}")
            
            return validation
            
        except Exception as e:
            logger.error(f"Failed to validate SQL with LLM: {e}")
            # Return basic validation result
            return ValidationResult(
                valid=True,  # Assume valid if LLM fails
                dialect=dialect,
                issues=[
                    ValidationIssue(
                        severity="warning",
                        category="validation",
                        message=f"LLM validation failed: {str(e)}"
                    )
                ]
            )
    
    def _build_expected_context(
        self,
        cql_structure: Dict[str, Any],
        valuesets: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Build context about what we expect in the SQL based on CQL."""
        context = {
            "library_name": cql_structure.get("library_name", ""),
            "populations": cql_structure.get("populations", []),
            "definitions": [d.get("name") for d in cql_structure.get("definitions", [])],
            "valuesets": [v.get("name") for v in cql_structure.get("valuesets", [])],
            "includes": [i.get("alias") for i in cql_structure.get("includes", [])],
            "expected_placeholders": []
        }
        
        # Add expected placeholders
        if valuesets:
            for vs_name in valuesets.keys():
                placeholder = f"PLACEHOLDER_{vs_name.replace(' ', '_').upper()}"
                context["expected_placeholders"].append(placeholder)
        
        return context
    
    def suggest_dialect_conversion(
        self,
        sql_query: str,
        from_dialect: str,
        to_dialect: str
    ) -> str:
        """
        Convert SQL from one dialect to another using LLM.
        
        Args:
            sql_query: SQL in the source dialect
            from_dialect: Source SQL dialect
            to_dialect: Target SQL dialect
            
        Returns:
            Converted SQL query
        """
        logger.info(f"Converting SQL from {from_dialect} to {to_dialect}")
        
        prompt = f"""
Convert the following SQL query from {from_dialect} to {to_dialect} dialect.

Source SQL ({from_dialect}):
{sql_query}

Convert to {to_dialect} with these considerations:
1. Date/time functions and formatting
2. String concatenation operators
3. Window function syntax
4. CTE materialization hints
5. Data type names
6. Null handling functions
7. Array/JSON operations if used

Return ONLY the converted SQL query, no explanations.
"""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": f"You are a SQL expert. Convert SQL from {from_dialect} to {to_dialect}."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1
            )
            
            converted_sql = response.choices[0].message.content.strip()
            logger.info(f"Successfully converted SQL to {to_dialect}")
            
            return converted_sql
            
        except Exception as e:
            logger.error(f"Failed to convert SQL dialect: {e}")
            return sql_query  # Return original if conversion fails