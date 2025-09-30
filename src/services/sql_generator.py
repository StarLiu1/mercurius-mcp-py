"""
Minimal SQL generation agent for CQL to SQL translation
"""

import json
import logging
from typing import Dict, Any, Optional
from src.services.llm_factory import LLMFactory
from src.services.json_utils import unwrap_json_response

logger = logging.getLogger(__name__)


class SimpleSQLGenerator:
    """
    Minimal SQL generator using LLM.
    Focuses on translating CQL to OMOP CDM SQL with placeholders.
    Supports multiple SQL dialects.
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize SQL generator."""
        self.config = config
        self.component_name = 'sql_generator'

        # Use LLMFactory to create client
        self.client, self.model = LLMFactory.create_component_client(config, self.component_name)
        logger.info(f"SimpleSQLGenerator initialized with model: {self.model}")

        self.dialect = config.get('sql_dialect', 'postgresql')  # Default to PostgreSQL
        
    def generate(self, parsed_cql: Dict[str, Any], valuesets: Dict[str, Any],
                 cql_content: str, dependency_analysis: Optional[Dict[str, Any]] = None,
                 library_definitions: Optional[Dict[str, Any]] = None,
                 valueset_registry: Optional[Dict[str, Any]] = None,
                 individual_codes: Optional[Dict[str, Any]] = None,
                 dialect: Optional[str] = None,
                 valueset_hints: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Generate SQL from CQL using OpenAI.
        
        Args:
            parsed_cql: Parsed CQL structure
            valuesets: Value set information (includes library valuesets)
            cql_content: Original CQL content
            dependency_analysis: Optional library dependency analysis
            library_definitions: Optional library definition structures
            valueset_registry: Optional comprehensive registry of all valuesets
            individual_codes: Optional dict of individual LOINC/SNOMED codes
            dialect: Optional SQL dialect override (postgresql, snowflake, bigquery, sqlserver)
            valueset_hints: Optional dict of OID to name mappings for consistent placeholders
            
        Returns:
            Dict with SQL query and components
        """
        # Use provided dialect or default from config
        sql_dialect = dialect or self.dialect
        logger.info(f"Generating SQL with OpenAI for {sql_dialect} dialect")
        
        # Build prompt with dependency analysis and library definitions if available
        prompt = self._build_prompt(parsed_cql, valuesets, cql_content, dependency_analysis,
                                   library_definitions, valueset_registry, individual_codes, sql_dialect,
                                   valueset_hints)
        
        try:
            # Call OpenAI
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": self._get_system_prompt(sql_dialect)
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                response_format={"type": "json_object"},
                temperature=0.1
            )
            
            # Parse response
            result = json.loads(response.choices[0].message.content)

            # Use universal unwrapper to handle any wrapper format
            result = unwrap_json_response(result)

            logger.info(f"Generated SQL with {len(result.get('ctes', []))} CTEs")
            
            return result
            
        except Exception as e:
            logger.error(f"SQL generation failed: {e}")
            # Return minimal structure
            return {
                "sql": "",
                "ctes": [],
                "main_query": "",
                "error": str(e)
            }
    
    def _get_system_prompt(self, dialect: str = "postgresql") -> str:
        """Get the system prompt for SQL generation for a specific dialect."""
        
        # Dialect-specific information
        dialect_info = self._get_dialect_info(dialect)
        
        return f"""You are a CQL to OMOP CDM SQL translator specializing in {dialect.upper()} SQL dialect.

## CRITICAL PLACEHOLDER RULES - READ FIRST
1. ALL placeholders MUST use ONLY alphanumeric characters and underscores
2. NEVER use dots (.) in placeholder names - they are INVALID SQL identifiers
3. When creating placeholders from OIDs, ALWAYS replace ALL dots with underscores
4. Example: OID "2.16.840.1.113883.3.464" becomes PLACEHOLDER_2_16_840_1_113883_3_464
5. This is MANDATORY - placeholders with dots will cause SQL syntax errors

## Your Task
Translate Clinical Quality Language (CQL) expressions to COMPLETE OMOP CDM-compatible SQL for {dialect.upper()}.
IMPORTANT: Generate COMPLETE SQL without any omissions, abbreviations, or shortcuts. Long queries are expected and preferred.

CRITICAL: PRESERVE ALL CQL LOGIC EXACTLY AS SPECIFIED
- If CQL has 7 valuesets UNIONed together, create 7 separate placeholders and UNION them
- NEVER simplify or optimize away CQL logic to "avoid creating placeholders"
- NEVER use NOT IN exclusion when CQL specifies explicit UNION of valuesets
- Every valueset reference in CQL must get its own placeholder
- Correctness and completeness take priority over optimization

## OMOP CDM Context
The OMOP (Observational Medical Outcomes Partnership) Common Data Model uses these main tables:
- PERSON: Patient demographics
- OBSERVATION_PERIOD: Time spans when patient data is available
- VISIT_OCCURRENCE: Healthcare visits/encounters
- CONDITION_OCCURRENCE: Diagnoses/conditions (domain_id = 'Condition')
- PROCEDURE_OCCURRENCE: Procedures performed (domain_id = 'Procedure')
- DRUG_EXPOSURE: Medications (domain_id = 'Drug')
- MEASUREMENT: Quantitative lab results, vital signs, and other measurements (domain_id = 'Measurement')
- OBSERVATION: Qualitative clinical observations, social history, family history, etc. (domain_id = 'Observation')

CRITICAL: OMOP concepts have a domain_id that determines which table they belong to:
- Concepts with domain_id = 'Measurement' → MEASUREMENT table (e.g., blood pressure, lab values)
- Concepts with domain_id = 'Observation' → OBSERVATION table (e.g., smoking status, family history)
- Concepts with domain_id = 'Condition' → CONDITION_OCCURRENCE table
- Concepts with domain_id = 'Procedure' → PROCEDURE_OCCURRENCE table
- Concepts with domain_id = 'Drug' → DRUG_EXPOSURE table

When querying for LOINC codes or other concepts, you MUST use the correct table based on the concept's domain:
- Use UNION ALL to combine results if concepts might be in multiple tables
- The concept_id column name varies by table:
  * MEASUREMENT: measurement_concept_id
  * OBSERVATION: observation_concept_id
  * CONDITION_OCCURRENCE: condition_concept_id
  * PROCEDURE_OCCURRENCE: procedure_concept_id
  * DRUG_EXPOSURE: drug_concept_id

## Key Rules
1. Use CTEs (Common Table Expressions) for each CQL definition
2. CRITICAL - Use PLACEHOLDERS for value sets: PLACEHOLDER_VALUESET_NAME
   - MANDATORY: Replace ALL dots with underscores in OIDs
   - CORRECT: PLACEHOLDER_2_16_840_1_113883_3_464_1003_104_12_1011
   - WRONG: PLACEHOLDER_2.16.840.1.113883.3.464.1003.104.12.1011 (INVALID SQL - will cause errors)
   - NEVER use dots in any placeholder name - SQL identifiers cannot contain dots
3. Map CQL context to OMOP:
   - Patient → PERSON table
   - Encounter → VISIT_OCCURRENCE table
4. Map CQL populations to SQL:
   - Initial Population → Base patient query
   - Denominator → Subset of initial population
   - Numerator → Subset meeting quality measure
5. Handle temporal logic:
   - "during" → date BETWEEN start AND end
   - "overlaps" → date ranges intersect

## Output Format
Return JSON with this structure:
{{
  "sql": "Complete SQL query with CTEs",
  "ctes": ["cte1", "cte2"],
  "main_query": "Final SELECT statement",
  "placeholders_used": ["PLACEHOLDER_NAME1", "PLACEHOLDER_NAME2"]
}}

## Example Patterns

### Example 1: Single domain query (conditions)
WITH has_condition AS (
  SELECT DISTINCT co.person_id, co.condition_start_date
  FROM condition_occurrence co
  WHERE co.condition_concept_id IN (PLACEHOLDER_DIABETES)
)

### Example 1b: Multiple valuesets UNIONed (PRESERVE EXACT CQL LOGIC)
-- CQL: union ["Encounter": "Office Visit"] union ["Encounter": "Home Healthcare"]
WITH qualifying_encounters AS (
  SELECT DISTINCT vo.person_id, vo.visit_occurrence_id, vo.visit_start_date
  FROM visit_occurrence vo
  WHERE vo.visit_concept_id IN (PLACEHOLDER_OFFICE_VISIT)

  UNION

  SELECT DISTINCT vo.person_id, vo.visit_occurrence_id, vo.visit_start_date
  FROM visit_occurrence vo
  WHERE vo.visit_concept_id IN (PLACEHOLDER_HOME_HEALTHCARE)
)
-- CORRECT: Each valueset gets its own placeholder and UNION
-- WRONG: Using NOT IN exclusion to "simplify" the logic

### Example 2: Individual LOINC codes MUST use placeholders
-- CORRECT: Using placeholder for individual LOINC code
WITH diastolic_bp_readings AS (
  SELECT person_id,
         COALESCE(measurement_datetime, measurement_date) as reading_date,
         value_as_number
  FROM measurement
  WHERE measurement_concept_id IN (PLACEHOLDER_LOINC_8462_4)  -- Diastolic BP
)

-- WRONG: Never use subqueries for codes
-- WHERE measurement_concept_id IN (SELECT concept_id FROM concept WHERE concept_code = '8462-4')

### Example 3: Multi-domain query with individual codes
WITH blood_pressure_readings AS (
  SELECT person_id,
         COALESCE(measurement_datetime, measurement_date) as reading_date,
         value_as_number,
         measurement_concept_id as concept_id
  FROM measurement
  WHERE measurement_concept_id IN (PLACEHOLDER_LOINC_8480_6)  -- Systolic BP

  UNION ALL

  SELECT person_id,
         COALESCE(observation_datetime, observation_date) as reading_date,
         value_as_number,
         observation_concept_id as concept_id
  FROM observation
  WHERE observation_concept_id IN (PLACEHOLDER_LOINC_8480_6)  -- Systolic BP
)

{dialect_info}"""
    
    def _build_prompt(self, parsed_cql: Dict[str, Any], valuesets: Dict[str, Any],
                      cql_content: str, dependency_analysis: Optional[Dict[str, Any]] = None,
                      library_definitions: Optional[Dict[str, Any]] = None,
                      valueset_registry: Optional[Dict[str, Any]] = None,
                      individual_codes: Optional[Dict[str, Any]] = None,
                      dialect: str = "postgresql",
                      valueset_hints: Optional[Dict[str, str]] = None) -> str:
        """Build the prompt for SQL generation."""
        # Create placeholder mapping using comprehensive registry if available
        placeholder_map = {}
        oid_to_placeholder = {}
        
        if valueset_registry:
            # Use the comprehensive registry that includes library valuesets
            for oid, vs_info in valueset_registry.items():
                name = vs_info.get('name', oid)
                # Create placeholder from OID to ensure uniqueness
                # CRITICAL: Replace ALL dots with underscores for valid SQL identifiers
                clean_oid = oid.replace(".", "_").replace("-", "_").upper()
                placeholder = f"PLACEHOLDER_{clean_oid}"
                placeholder_map[name] = placeholder
                oid_to_placeholder[oid] = placeholder
                
                # Also map by name for easy reference
                if name != oid:
                    placeholder_map[name] = placeholder
        else:
            # Fallback to original mapping from valuesets
            for vs_name, vs_data in valuesets.items():
                # CRITICAL: Replace ALL special characters including dots for valid SQL identifiers
                clean_name = vs_name.replace(" ", "_").replace("-", "_").replace(".", "_").upper()
                placeholder = f"PLACEHOLDER_{clean_name}"
                placeholder_map[vs_name] = placeholder
        
        # Build dependency context if available
        dependency_context = ""
        if dependency_analysis:
            dependency_context = f"""
## Library Dependencies
{json.dumps(dependency_analysis.get('dependencies', []), indent=2)}

## Library Usage in Main CQL
{json.dumps(dependency_analysis.get('library_usage', {}), indent=2)}

## SQL Structure Hints from Libraries
{json.dumps(dependency_analysis.get('sql_structure_hints', []), indent=2)}
"""
        
        # Build library definitions context if available
        library_context = ""
        if library_definitions:
            library_context = "\n## Library Definitions\n"
            for lib_name, lib_structure in library_definitions.items():
                library_context += f"\n### Library: {lib_name}\n"
                if hasattr(lib_structure, 'definitions'):
                    for definition in lib_structure.definitions:
                        library_context += f"\n**{definition.name}**:\n```cql\n{definition.logic}\n```\n"
                elif isinstance(lib_structure, dict) and 'definitions' in lib_structure:
                    for definition in lib_structure.get('definitions', []):
                        library_context += f"\n**{definition.get('name', 'Unknown')}**:\n```cql\n{definition.get('logic', '')}\n```\n"

        # Add valueset hints section if provided
        valueset_hints_section = ""
        if valueset_hints:
            valueset_hints_section = f"""
## Valueset Reference (OID to Name Mapping)
{json.dumps(valueset_hints, indent=2)}

CRITICAL: Use ONLY OID-based placeholders for ALL valuesets:
- Format: PLACEHOLDER_[OID with dots replaced by underscores]
- Example: OID "2.16.840.1.113883.3.464.1003.101.12.1080" → PLACEHOLDER_2_16_840_1_113883_3_464_1003_101_12_1080
- NEVER create name-based placeholders like PLACEHOLDER_TELEPHONE_VISIT
- This ensures 100% match with MCP server results
"""

        prompt = f"""Translate this CQL to OMOP CDM SQL for {dialect.upper()} dialect.

⚠️ CRITICAL REMINDER: All placeholders MUST have dots replaced with underscores. SQL identifiers CANNOT contain dots.
Example: "2.16.840.1.113883" → PLACEHOLDER_2_16_840_1_113883

## CQL Content
{cql_content}

## Parsed Structure
Library: {parsed_cql.get('library_name', 'Unknown')}
Context: {parsed_cql.get('context', 'Patient')}
Populations: {', '.join(parsed_cql.get('populations', []))}
Definitions: {len(parsed_cql.get('definitions', {}))}
{dependency_context}
{library_context}
{valueset_hints_section}
## Value Sets and Codes
IMPORTANT: Use these EXACT placeholders for value sets. DO NOT create new OIDs or placeholders.
⚠️ CRITICAL: Use OID-based placeholders ONLY. The placeholders below already have dots replaced with underscores. Use them EXACTLY as shown.
NEVER add dots to placeholders - they are INVALID in SQL and will cause syntax errors:

### Value Set Placeholders (use for value set references):
{json.dumps(placeholder_map, indent=2)}

### OID to Placeholder Mapping (for reference):
{json.dumps(oid_to_placeholder, indent=2) if valueset_registry else "Not available"}

### Individual Code Placeholders (use these for individual LOINC/SNOMED codes):
{self._format_individual_codes(individual_codes) if individual_codes else "None"}

CRITICAL: Individual LOINC/SNOMED codes MUST use their placeholders (e.g., PLACEHOLDER_LOINC_8462_4) exactly like valuesets.
NEVER use subqueries like "SELECT concept_id FROM concept WHERE concept_code = '8462-4'"
ALWAYS use the placeholder: "WHERE measurement_concept_id IN PLACEHOLDER_LOINC_8462_4"

## Requirements
1. Create a CTE for EVERY CQL definition - do NOT omit any for brevity
2. Generate COMPLETE SQL - no placeholders like "-- other CTEs here" or similar
3. Include ALL logic from the CQL - do not simplify or skip any conditions
4. Use ONLY the exact placeholder names provided above - DO NOT invent new OIDs
5. CRITICAL: Verify ALL placeholders use underscores, not dots - dots make invalid SQL identifiers
6. CRITICAL: For ALL codes (valuesets AND individual codes), use placeholders ONLY - never use subqueries or hardcoded concept lookups
7. Map to appropriate OMOP tables based on context
8. Handle ALL temporal logic present in the CQL
9. When you see references like AdultOutpatientEncounters."Qualifying Encounters", create a COMPLETE CTE for that library definition
10. Library definitions should be translated as their own COMPLETE CTEs that can be referenced by the main query
11. For individual LOINC/SNOMED codes, use their placeholders EXACTLY as provided (e.g., WHERE measurement_concept_id IN PLACEHOLDER_LOINC_8462_4)
12. Return valid JSON with the specified structure
13. The SQL can be as long as needed - completeness is more important than brevity
14. CRITICAL: When CQL uses UNION of multiple valuesets, translate each valueset to its own placeholder and UNION the results
15. NEVER simplify multiple valuesets into a single NOT IN exclusion - preserve the exact CQL logic

IMPORTANT: 
- Generate COMPLETE SQL without any omissions or abbreviations
- Library definitions referenced in the main CQL (like AdultOutpatientEncounters."Qualifying Encounters") must be FULLY translated into SQL CTEs
- Do NOT use comments like "-- additional logic here" or similar shortcuts
- Include ALL conditions, joins, and logic from the original CQL

Generate the COMPLETE SQL translation now."""
        
        return prompt
    
    def _get_dialect_info(self, dialect: str) -> str:
        """Get dialect-specific SQL information."""
        
        dialect_guides = {
            "postgresql": """
## PostgreSQL Specific Rules
- Use DATEADD for date arithmetic: DATEADD(month, 6, date_column)
- Use DATEDIFF for date differences: DATEDIFF(year, birth_date, current_date)
- String concatenation: || operator
- Use COALESCE for null handling
- CTEs are supported without hints
- Use INTERVAL for date arithmetic: date + INTERVAL '6 months'
- QUALIFY clause not supported, use subqueries or window functions with CTEs""",
            
            "snowflake": """
## Snowflake Specific Rules
- Use DATEADD for date arithmetic: DATEADD(month, 6, date_column)
- Use DATEDIFF for date differences: DATEDIFF(year, birth_date, current_date)
- String concatenation: || or CONCAT
- Use COALESCE or IFNULL for null handling
- CTEs are supported and can be materialized
- QUALIFY clause is supported for window functions
- Use VARIANT type for JSON operations""",
            
            "bigquery": """
## BigQuery Specific Rules
- Use DATE_ADD for date arithmetic: DATE_ADD(date_column, INTERVAL 6 MONTH)
- Use DATE_DIFF for date differences: DATE_DIFF(current_date, birth_date, YEAR)
- String concatenation: || or CONCAT
- Use COALESCE for null handling
- CTEs are supported
- QUALIFY clause is supported
- Use STRUCT for nested data
- Table references: project.dataset.table""",
            
            "sqlserver": """
## SQL Server Specific Rules  
- Use DATEADD for date arithmetic: DATEADD(month, 6, date_column)
- Use DATEDIFF for date differences: DATEDIFF(year, birth_date, GETDATE())
- String concatenation: + operator or CONCAT
- Use COALESCE or ISNULL for null handling
- CTEs are supported
- No QUALIFY clause, use subqueries
- Use GETDATE() for current timestamp
- TOP instead of LIMIT"""
        }
        
        return dialect_guides.get(dialect, dialect_guides["postgresql"])
    
    def _format_individual_codes(self, individual_codes: Dict[str, Any]) -> str:
        """Format individual codes for the prompt with their placeholders."""
        if not individual_codes:
            return "None"
        
        formatted = {}
        for code_key, code_data in individual_codes.items():
            # Get the placeholder name from the code data
            placeholder = code_data.get('placeholder', '')
            if not placeholder:
                # Generate placeholder if not provided
                code = code_data.get('code', '')
                system = code_data.get('system', '')
                clean_code = code.replace('-', '_').replace('.', '_')
                placeholder = f"PLACEHOLDER_{system.upper()}_{clean_code}"
            
            formatted[f"{code_data.get('name', code_key)}"] = {
                "code": code_data.get('code', ''),
                "system": code_data.get('system', ''),
                "placeholder": placeholder,
                "description": f"Use {placeholder} for {code_data.get('system', '')} code {code_data.get('code', '')}"
            }
        
        return json.dumps(formatted, indent=2)