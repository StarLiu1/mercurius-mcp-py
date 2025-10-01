"""
LLM-Driven CQL to SQL workflow with 5 simplified steps.
Maximizes LLM intelligence while keeping critical operations programmatic.
"""

import logging
from typing import Dict, Any, List, Optional, TypedDict
from pathlib import Path

from langgraph.graph import StateGraph, END

# Import new LLM-based components
from services.cql_parser import CQLParser
from services.sql_generator import SimpleSQLGenerator  
from services.sql_validator import SQLValidator
from services.sql_corrector import SQLCorrector
from services.mcp_client_simplified import SimplifiedMCPClient
from services.library_resolver import LibraryResolver

logger = logging.getLogger(__name__)


class WorkflowState(TypedDict, total=False):
    """State schema for LLM-driven workflow."""
    # Input
    cql_content: str
    cql_file_path: str
    config: Dict[str, Any]
    sql_dialect: str
    
    # Step 1: Parse & Analyze
    parsed_structure: Dict[str, Any]  # From LLM parser
    library_files: Dict[str, str]
    library_definitions: Dict[str, Any]  # Parsed library structures
    dependency_analysis: Dict[str, Any]
    
    # Step 2: Extract Valuesets
    all_valuesets: Dict[str, Any]
    placeholder_mappings: Dict[str, List[str]]
    
    # Step 3: Generate SQL
    generated_sql: Dict[str, Any]
    
    # Step 4: Validate
    validation_result: Dict[str, Any]
    
    # Step 5: Correct SQL based on validation
    corrected_sql: Dict[str, Any]
    
    # Step 6: Replace
    final_sql: str
    statistics: Dict[str, Any]


class LLMDrivenWorkflow:
    """
    6-step LLM-driven workflow:
    1. Parse & Analyze (LLM) - Understand CQL and dependencies
    2. Extract Valuesets (MCP) - Get all OMOP mappings
    3. Generate SQL (LLM) - Create SQL with full context
    4. Validate SQL (LLM) - Semantic and syntactic validation
    5. Correct SQL (LLM) - Fix issues found during validation
    6. Replace Placeholders (Programmatic) - Final substitution
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize LLM-driven workflow."""
        self.config = config
        
        # Initialize components
        self.cql_parser = CQLParser(config)
        self.sql_generator = SimpleSQLGenerator(config)
        self.sql_validator = SQLValidator(config)
        self.sql_corrector = SQLCorrector(config)
        self.mcp_client = SimplifiedMCPClient(
            server_url=config.get('mcp_server_url'),
            db_config=config.get('database'),
            vsac_username=config.get('vsac_username'),
            vsac_password=config.get('vsac_password'),
            timeout=config.get('mcp_timeout', 120)
        )
        self.library_resolver = LibraryResolver(self.mcp_client)
        
        # Build workflow
        self.workflow = self._build_workflow()
        
    def _build_workflow(self) -> StateGraph:
        """Build the 6-step workflow graph."""
        workflow = StateGraph(WorkflowState)
        
        # Add nodes (6 steps)
        workflow.add_node("parse_and_analyze", self.parse_and_analyze)
        workflow.add_node("extract_all_valuesets", self.extract_all_valuesets)
        workflow.add_node("generate_sql", self.generate_sql)
        workflow.add_node("validate_sql", self.validate_sql)
        workflow.add_node("correct_sql", self.correct_sql)
        workflow.add_node("replace_placeholders", self.replace_placeholders)
        
        # Define flow
        workflow.set_entry_point("parse_and_analyze")
        workflow.add_edge("parse_and_analyze", "extract_all_valuesets")
        workflow.add_edge("extract_all_valuesets", "generate_sql")
        workflow.add_edge("generate_sql", "validate_sql")
        workflow.add_edge("validate_sql", "correct_sql")
        workflow.add_edge("correct_sql", "replace_placeholders")
        workflow.add_edge("replace_placeholders", END)
        
        return workflow.compile()
    
    def parse_and_analyze(self, state: WorkflowState) -> WorkflowState:
        """
        Step 1: Parse CQL and analyze dependencies using LLM.
        Single unified step for understanding structure and relationships.
        """
        logger.info("Step 1: Parsing CQL and analyzing dependencies with LLM")
        
        cql_content = state["cql_content"]
        cql_file_path = state.get("cql_file_path", "")
        
        # Read library files if they exist
        library_files = {}
        if cql_file_path and cql_file_path != "inline":
            # Find library files in same directory
            cql_path = Path(cql_file_path)
            if cql_path.exists():
                cql_dir = cql_path.parent
                for lib_file in cql_dir.glob("*.cql"):
                    if lib_file != cql_path:
                        try:
                            library_files[lib_file.stem] = lib_file.read_text()
                            logger.info(f"Found library file: {lib_file.name}")
                        except Exception as e:
                            logger.warning(f"Could not read library {lib_file}: {e}")
        
        # Use LLM parser to understand CQL structure and dependencies
        parsed_structure = self.cql_parser.parse(cql_content, library_files)
        
        # Extract dependency information from parsed structure
        dependency_analysis = {
            "includes": parsed_structure.includes,
            "dependencies": [],
            "library_usage": {},
            "valueset_sources": {},
            "sql_structure_hints": []
        }
        
        # Analyze which definitions use which libraries
        for definition in parsed_structure.definitions:
            for include in parsed_structure.includes:
                if f"{include.alias}." in definition.logic:
                    if include.alias not in dependency_analysis["library_usage"]:
                        dependency_analysis["library_usage"][include.alias] = []
                    dependency_analysis["library_usage"][include.alias].append(definition.name)
        
        # Identify valueset sources (main vs library)
        for valueset in parsed_structure.valuesets:
            dependency_analysis["valueset_sources"][valueset.name] = "main"
        
        # Add SQL structure hints based on populations
        if parsed_structure.populations:
            dependency_analysis["sql_structure_hints"].append(
                f"Create CTEs for populations: {', '.join(parsed_structure.populations)}"
            )
        
        # Convert parsed structure to dict for state
        state["parsed_structure"] = parsed_structure.model_dump()
        state["library_files"] = library_files
        state["library_definitions"] = parsed_structure.library_definitions  # Store parsed library structures
        state["dependency_analysis"] = dependency_analysis
        
        logger.info(f"Parsed CQL: {parsed_structure.library_name} with {len(parsed_structure.definitions)} definitions")
        logger.info(f"Found {len(parsed_structure.includes)} library dependencies")
        if parsed_structure.library_definitions:
            logger.info(f"Parsed {len(parsed_structure.library_definitions)} library files with definitions")
        
        return state
    
    def extract_all_valuesets(self, state: WorkflowState) -> WorkflowState:
        """
        Step 2: Extract all valuesets and individual codes (main + library) via MCP.
        Single consolidated extraction step.
        """
        logger.info("Step 2: Extracting all valuesets and individual codes via MCP")
        
        cql_content = state["cql_content"]
        library_files = state.get("library_files", {})
        parsed_structure = state["parsed_structure"]
        
        # Extract main valuesets and individual codes
        logger.info("Extracting main CQL valuesets and individual codes")
        main_result = self.mcp_client.extract_and_map_valuesets(cql_content)
        all_valuesets = main_result.get("valuesets", {})
        placeholder_mappings = main_result.get("placeholders", {})
        
        # Extract individual codes from main result
        # The MCP server now returns individual codes in the result
        individual_codes = main_result.get("individual_codes", {})
        
        # Extract library valuesets and individual codes
        for lib_name, lib_content in library_files.items():
            logger.info(f"Extracting valuesets and individual codes from library: {lib_name}")
            lib_result = self.mcp_client.extract_and_map_valuesets(lib_content)
            
            # Merge valuesets
            lib_valuesets = lib_result.get("valuesets", {})
            lib_placeholders = lib_result.get("placeholders", {})
            lib_individual_codes = lib_result.get("individual_codes", {})
            
            # Add library valuesets with prefix to avoid conflicts
            for vs_name, vs_data in lib_valuesets.items():
                prefixed_name = f"{lib_name}_{vs_name}" if vs_name in all_valuesets else vs_name
                all_valuesets[prefixed_name] = vs_data
            
            # Merge placeholders
            placeholder_mappings.update(lib_placeholders)
            
            # Merge individual codes from libraries
            for code_key, code_data in lib_individual_codes.items():
                # Prefix library codes to avoid conflicts
                prefixed_key = f"{lib_name}_{code_key}" if code_key in individual_codes else code_key
                individual_codes[prefixed_key] = code_data
        
        # Create a comprehensive valueset registry including parsed library valuesets
        valueset_registry = {}
        
        # Add main CQL valuesets from parsed structure
        for vs in parsed_structure.get('valuesets', []):
            oid = vs.get('oid', '')
            name = vs.get('name', '')
            if oid:
                valueset_registry[oid] = {
                    'name': name,
                    'oid': oid,
                    'source': 'main'
                }
        
        # Add library valuesets from parsed library definitions
        library_definitions = state.get('library_definitions', {})
        for lib_name, lib_def in library_definitions.items():
            if isinstance(lib_def, dict) and 'valuesets' in lib_def:
                for vs in lib_def.get('valuesets', []):
                    oid = vs.get('oid', '')
                    name = vs.get('name', '')
                    if oid:
                        valueset_registry[oid] = {
                            'name': name,
                            'oid': oid,
                            'source': lib_name
                        }
        
        # Log which valuesets were not extracted via MCP
        missing_valuesets = []
        for oid, vs_info in valueset_registry.items():
            # Check if this OID was extracted
            found = False
            for extracted_oid in all_valuesets.keys():
                if oid in extracted_oid or extracted_oid in oid:
                    found = True
                    break
            if not found:
                missing_valuesets.append(f"{vs_info['name']} ({oid}) from {vs_info['source']}")
                logger.warning(f"Valueset not extracted via MCP: {vs_info['name']} ({oid}) from {vs_info['source']}")
        
        if missing_valuesets:
            logger.error(f"Missing {len(missing_valuesets)} valuesets from MCP extraction")
        
        state["all_valuesets"] = all_valuesets
        state["placeholder_mappings"] = placeholder_mappings
        state["valueset_registry"] = valueset_registry  # Complete registry of all valuesets
        state["individual_codes"] = individual_codes  # Individual code mappings
        
        logger.info(f"Extracted {len(all_valuesets)} valuesets via MCP")
        logger.info(f"Extracted {len(individual_codes)} individual codes via MCP")
        logger.info(f"Registry contains {len(valueset_registry)} total valuesets")
        logger.info(f"Created {len(placeholder_mappings)} placeholder mappings")
        
        return state
    
    def generate_sql(self, state: WorkflowState) -> WorkflowState:
        """
        Step 3: Generate SQL with full library context using LLM.
        Provides all necessary context for accurate translation.
        """
        logger.info("Step 3: Generating SQL with LLM (including library context)")

        # Create valueset hints for OID-based placeholder generation
        valueset_hints = {}
        for oid, vs_data in state.get("all_valuesets", {}).items():
            # Map OID to name for SQL generator reference
            valueset_hints[oid] = vs_data.get('name', '')

        logger.info(f"Passing {len(valueset_hints)} valueset hints to SQL generator")

        # Generate SQL with full context including library definitions
        sql_result = self.sql_generator.generate(
            parsed_cql=state["parsed_structure"],
            valuesets=state["all_valuesets"],
            cql_content=state["cql_content"],
            dependency_analysis=state["dependency_analysis"],
            library_definitions=state.get("library_definitions", {}),
            valueset_registry=state.get("valueset_registry", {}),
            individual_codes=state.get("individual_codes", {}),
            dialect=state.get("sql_dialect", "postgresql"),
            valueset_hints=valueset_hints  # Pass OID to name mappings
        )

        state["generated_sql"] = sql_result
        
        logger.info(f"Generated SQL with {len(sql_result.get('ctes', []))} CTEs")
        if sql_result.get('error'):
            logger.error(f"SQL generation error: {sql_result['error']}")
        
        return state
    
    def validate_sql(self, state: WorkflowState) -> WorkflowState:
        """
        Step 4: Validate SQL semantically and syntactically using LLM.
        Comprehensive validation of logic and syntax.
        """
        logger.info("Step 4: Validating SQL with LLM (semantic + syntactic)")
        
        sql_query = state["generated_sql"].get("sql", "")
        
        if not sql_query:
            logger.warning("No SQL to validate")
            state["validation_result"] = {
                "valid": False,
                "issues": [{"severity": "error", "message": "No SQL generated"}]
            }
            return state
        
        # Validate with LLM
        validation_result = self.sql_validator.validate(
            sql_query=sql_query,
            cql_structure=state["parsed_structure"],
            dialect=state.get("sql_dialect", "postgresql"),
            valuesets=state["all_valuesets"]
        )
        
        state["validation_result"] = validation_result.model_dump()
        
        # Log validation results
        if validation_result.valid:
            logger.info("SQL validation passed")
        else:
            logger.warning("SQL validation failed")
            for issue in validation_result.issues:
                if issue.severity == "error":
                    logger.error(f"  - {issue.message}")
                else:
                    logger.warning(f"  - {issue.message}")
        
        return state
    
    def correct_sql(self, state: WorkflowState) -> WorkflowState:
        """
        Step 5: Correct SQL based on validation feedback using LLM.
        Fixes errors found during validation while preserving placeholders.
        """
        logger.info("Step 5: Correcting SQL based on validation feedback")
        
        # Get the SQL and validation result
        sql_query = state["generated_sql"].get("sql", "")
        validation_result = state.get("validation_result", {})
        
        # Check if there are errors to fix
        if validation_result.get("valid", True):
            logger.info("No validation errors to correct")
            state["corrected_sql"] = {
                "corrected_sql": sql_query,
                "changes_made": [],
                "success": True
            }
            return state
        
        # Correct the SQL using the corrector
        correction_result = self.sql_corrector.correct_sql(
            sql_query=sql_query,
            validation_result=validation_result,
            dialect=state.get("sql_dialect", "postgresql"),
            cql_structure=state["parsed_structure"]
        )
        
        state["corrected_sql"] = correction_result
        
        if correction_result.get("success"):
            logger.info(f"SQL corrected successfully with {len(correction_result.get('changes_made', []))} changes")
        else:
            logger.error(f"SQL correction failed: {correction_result.get('error')}")
        
        return state
    
    def _flatten_concept_ids(self, concept_ids):
        """
        Flatten concept IDs that may be grouped with parentheses.
        Handles both flat lists and lists containing parenthesized groups.

        Examples:
            ['1', '2', '3'] -> ['1', '2', '3']
            ['(1, 2)', '(3, 4)'] -> ['1', '2', '3', '4']
            ['1', '(2, 3)', '4'] -> ['1', '2', '3', '4']
        """
        flattened = []
        for item in concept_ids:
            item_str = str(item).strip()
            # Check if this item is a grouped string with parentheses
            if item_str.startswith('(') and item_str.endswith(')'):
                # Remove parentheses and split on comma
                inner = item_str[1:-1]
                # Split and clean each ID
                for id_part in inner.split(','):
                    cleaned = id_part.strip()
                    if cleaned:
                        flattened.append(cleaned)
            else:
                # Regular single concept ID
                if item_str:
                    flattened.append(item_str)
        return flattened

    def replace_placeholders(self, state: WorkflowState) -> WorkflowState:
        """
        Step 6: Replace placeholders with OMOP concept IDs.
        Purely programmatic - no LLM involvement.
        """
        logger.info("Step 6: Replacing placeholders (programmatic)")
        
        # Use corrected SQL if available, otherwise use generated SQL
        corrected_result = state.get("corrected_sql", {})
        if corrected_result.get("success") and corrected_result.get("corrected_sql"):
            sql_query = corrected_result["corrected_sql"]
            logger.info("Using corrected SQL for placeholder replacement")
        else:
            sql_query = state["generated_sql"].get("sql", "")
            logger.info("Using original generated SQL for placeholder replacement")
        placeholder_mappings = state.get("placeholder_mappings", {})
        
        if not sql_query:
            logger.error("No SQL query to process")
            state["final_sql"] = ""
            return state
        
        # Replace each placeholder with its OMOP concepts
        final_sql = sql_query
        replacements_made = 0
        unmapped_placeholders = []
        
        # Find all placeholders in the SQL
        import re
        placeholders_in_sql = re.findall(r'PLACEHOLDER_[\w_]+', final_sql)
        unique_placeholders = set(placeholders_in_sql)
        
        logger.info(f"Found {len(unique_placeholders)} unique placeholders in SQL")
        
        for placeholder in unique_placeholders:
            if placeholder in placeholder_mappings:
                concept_ids = placeholder_mappings[placeholder]
                if concept_ids:
                    # Flatten concept IDs in case they're grouped with parentheses
                    flattened_ids = self._flatten_concept_ids(concept_ids)
                    # Create SQL IN clause with concept IDs
                    concepts_str = ", ".join(str(c) for c in flattened_ids)
                    logger.debug(f"Flattened {len(concept_ids)} items to {len(flattened_ids)} concept IDs")
                else:
                    # No concepts found - use NULL
                    concepts_str = "NULL"
                    flattened_ids = []
                    logger.warning(f"No OMOP concepts for {placeholder}")

                # Handle different placeholder patterns
                replaced = False

                # Pattern 1: IN (SELECT value FROM PLACEHOLDER_...) - created by SQL corrector
                subquery_pattern = f"IN (SELECT value FROM {placeholder})"
                if subquery_pattern in final_sql:
                    if state.get("sql_dialect") == "sqlserver" and flattened_ids:
                        # For SQL Server, create proper VALUES clause
                        values_list = ', '.join(f"({id})" for id in flattened_ids)
                        replacement = f"IN (SELECT value FROM (VALUES {values_list}) AS t(value))"
                    else:
                        # For other dialects or NULL case, just use direct IN list
                        replacement = f"IN ({concepts_str})"
                    final_sql = final_sql.replace(subquery_pattern, replacement)
                    replaced = True

                # Pattern 2: SELECT value FROM (PLACEHOLDER_...)
                from_pattern1 = f"SELECT value FROM ({placeholder})"
                from_pattern2 = f"SELECT value FROM {placeholder}"
                for from_pattern in [from_pattern1, from_pattern2]:
                    if from_pattern in final_sql:
                        if state.get("sql_dialect") == "sqlserver" and flattened_ids:
                            # For SQL Server, create proper VALUES clause
                            values_list = ', '.join(f"({id})" for id in flattened_ids)
                            replacement = f"SELECT value FROM (VALUES {values_list}) AS t(value)"
                        else:
                            # For other dialects, just return the values
                            replacement = concepts_str
                        final_sql = final_sql.replace(from_pattern, replacement)
                        replaced = True

                # Pattern 3: Already wrapped in parentheses: (PLACEHOLDER_NAME)
                if not replaced:
                    wrapped_pattern = f"({placeholder})"
                    if wrapped_pattern in final_sql:
                        # Already has parentheses, don't add more
                        final_sql = final_sql.replace(wrapped_pattern, f"({concepts_str})")
                        replaced = True

                # Pattern 4: Simple placeholder without parentheses
                if not replaced and placeholder in final_sql:
                    # Add parentheses for IN clause
                    final_sql = final_sql.replace(placeholder, f"({concepts_str})")

                replacements_made += 1
                logger.info(f"Replaced {placeholder} with {len(flattened_ids)} concepts")
            else:
                unmapped_placeholders.append(placeholder)
                logger.error(f"No mapping found for placeholder: {placeholder}")
        
        # Check for any remaining placeholders
        remaining = re.findall(r'PLACEHOLDER_[\w_]+', final_sql)
        if remaining:
            logger.error(f"Unreplaced placeholders remain: {remaining}")
        
        # Compile statistics
        statistics = {
            "valuesets_extracted": len(state.get("all_valuesets", {})),
            "placeholders_found": len(unique_placeholders),
            "placeholders_replaced": replacements_made,
            "unmapped_placeholders": len(unmapped_placeholders),
            "omop_concepts_mapped": sum(
                len(concepts) for concepts in placeholder_mappings.values()
            ),
            "validation_passed": state.get("validation_result", {}).get("valid", False),
            "ctes_generated": len(state.get("generated_sql", {}).get("ctes", [])),
            "libraries_processed": len(state.get("library_files", {}))
        }
        
        state["final_sql"] = final_sql
        state["statistics"] = statistics
        
        logger.info(f"Replacement complete: {replacements_made}/{len(unique_placeholders)} placeholders replaced")
        
        return state
    
    def run(self, cql_content: str, cql_file_path: Optional[str] = None, 
            sql_dialect: str = "postgresql") -> Dict[str, Any]:
        """
        Run the LLM-driven workflow.
        
        Args:
            cql_content: CQL content to translate
            cql_file_path: Path to CQL file (for finding library files)
            sql_dialect: Target SQL dialect (postgresql, snowflake, bigquery, sqlserver)
            
        Returns:
            Final state with SQL and statistics
        """
        logger.info("Starting LLM-driven CQL to SQL translation")
        logger.info(f"Target dialect: {sql_dialect}")
        
        # Initialize state
        initial_state = {
            "cql_content": cql_content,
            "cql_file_path": cql_file_path or "inline",
            "config": self.config,
            "sql_dialect": sql_dialect
        }
        
        # Run workflow
        try:
            final_state = self.workflow.invoke(initial_state)
            
            # Log summary
            stats = final_state.get("statistics", {})
            if final_state.get("final_sql"):
                logger.info("✓ Translation completed successfully")
                logger.info(f"  - Libraries: {stats.get('libraries_processed', 0)}")
                logger.info(f"  - Valuesets: {stats.get('valuesets_extracted', 0)}")
                logger.info(f"  - OMOP concepts: {stats.get('omop_concepts_mapped', 0)}")
                logger.info(f"  - Placeholders: {stats.get('placeholders_replaced', 0)}/{stats.get('placeholders_found', 0)}")
                logger.info(f"  - Validation: {'PASSED' if stats.get('validation_passed') else 'FAILED'}")
            else:
                logger.error("✗ Translation failed - no SQL generated")
            
            return final_state
            
        except Exception as e:
            logger.error(f"Workflow failed: {e}")
            raise