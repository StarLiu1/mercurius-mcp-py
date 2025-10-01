"""
LLM-based CQL parser that replaces regex patterns with intelligent understanding.
"""

import json
import logging
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
from services.llm_factory import LLMFactory
from services.json_utils import unwrap_json_response

logger = logging.getLogger(__name__)


class CQLInclude(BaseModel):
    """Represents a CQL library include."""
    name: str
    version: str
    alias: str


class CQLValueSet(BaseModel):
    """Represents a CQL value set."""
    name: str
    oid: str


class CQLDefinition(BaseModel):
    """Represents a CQL definition."""
    name: str
    logic: str
    type: str  # 'population', 'expression', 'function', 'measure'
    references: List[str] = Field(default_factory=list)


class CQLStructure(BaseModel):
    """Complete CQL structure extracted by LLM."""
    library_name: str
    library_version: str
    using_model: str = ""
    using_version: str = ""
    context: str = "Patient"
    includes: List[CQLInclude] = Field(default_factory=list)
    valuesets: List[CQLValueSet] = Field(default_factory=list)
    codes: List[Dict[str, str]] = Field(default_factory=list)
    definitions: List[CQLDefinition] = Field(default_factory=list)
    populations: List[str] = Field(default_factory=list)
    parameters: List[Dict[str, str]] = Field(default_factory=list)
    library_definitions: Dict[str, Any] = Field(default_factory=dict)  # New field for parsed library structures


class CQLParser:
    """LLM-based CQL parser that understands structure and intent."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize CQL parser with LLM configuration."""
        self.config = config
        self.component_name = 'cql_parser'

        # Use LLMFactory to create client
        self.client, self.model = LLMFactory.create_component_client(config, self.component_name)
        logger.info(f"CQLParser initialized with model: {self.model}")
        
    def parse(self, cql_content: str, library_files: Optional[Dict[str, str]] = None) -> CQLStructure:
        """
        Parse CQL content using LLM to extract complete structure.
        
        Args:
            cql_content: The main CQL content to parse
            library_files: Optional dictionary of library name -> content
            
        Returns:
            CQLStructure with all extracted information
        """
        logger.info("Parsing CQL structure with LLM")
        
        # Parse library files completely if provided
        library_structures = {}
        if library_files:
            logger.info(f"Parsing {len(library_files)} library files")
            for name, content in library_files.items():
                logger.info(f"Parsing library: {name}")
                lib_structure = self._parse_library(content, name)
                if lib_structure:
                    library_structures[name] = lib_structure
        
        # Parse main CQL with library context
        main_structure = self._parse_main_cql(cql_content, library_structures)
        
        # Add parsed library structures to main structure
        main_structure.library_definitions = library_structures
        
        return main_structure
    
    def _parse_library(self, library_content: str, library_name: str) -> Optional[CQLStructure]:
        """
        Parse a library CQL file.
        
        Args:
            library_content: The library CQL content
            library_name: Name of the library for logging
            
        Returns:
            Parsed library structure or None if parsing fails
        """
        logger.info(f"Parsing library {library_name} with LLM")
        
        prompt = f"""
You are a CQL (Clinical Quality Language) expert. Parse the following LIBRARY file and extract its complete structure.

This is a LIBRARY file that will be referenced by a main CQL file. Extract ALL definitions, valuesets, and other components.

Library CQL Content:
{library_content}

Extract and return a JSON object with this EXACT structure:
{{
    "library_name": "string - the library name",
    "library_version": "string - the library version",
    "using_model": "string - the data model used (e.g., 'QDM', 'FHIR')",
    "using_version": "string - the model version",
    "context": "string - the context (usually 'Patient')",
    "includes": [
        {{
            "name": "string - library name",
            "version": "string - library version",
            "alias": "string - the alias used in code"
        }}
    ],
    "valuesets": [
        {{
            "name": "string - valueset name",
            "oid": "string - the OID (e.g., 'urn:oid:2.16.840.1.113883.3.464.1003.104.12.1011')"
        }}
    ],
    "codes": [
        {{
            "name": "string - code name",
            "code": "string - the code value",
            "system": "string - code system name"
        }}
    ],
    "definitions": [
        {{
            "name": "string - definition name",
            "logic": "string - the complete logic/expression",
            "type": "string - one of: 'population', 'expression', 'function', 'measure'",
            "references": ["list of referenced definitions or valuesets"]
        }}
    ],
    "populations": ["list of population names (Initial Population, Denominator, etc.)"],
    "parameters": [
        {{
            "name": "string - parameter name",
            "type": "string - parameter type"
        }}
    ]
}}

IMPORTANT: 
1. Extract ALL definitions from the library - they will be needed by the main CQL
2. Preserve the complete logic for each definition
3. Identify all valuesets used in the library
4. Return ONLY the JSON object, no additional text.
"""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a CQL parsing expert. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)

            # Use universal unwrapper to handle any wrapper format
            result = unwrap_json_response(result)

            # Convert to Pydantic model for validation
            structure = CQLStructure(**result)
            
            logger.info(f"Successfully parsed library: {structure.library_name}")
            logger.info(f"  - {len(structure.definitions)} definitions")
            logger.info(f"  - {len(structure.valuesets)} valuesets")
            
            return structure
            
        except Exception as e:
            logger.error(f"Failed to parse library {library_name}: {e}")
            return None
    
    def _parse_main_cql(self, cql_content: str, library_structures: Dict[str, Any]) -> CQLStructure:
        """
        Parse the main CQL file with library context.
        
        Args:
            cql_content: The main CQL content
            library_structures: Previously parsed library structures
            
        Returns:
            Parsed main CQL structure
        """
        # Build library context for the prompt
        library_context = ""
        if library_structures:
            library_context = "\n\nParsed Library Structures Available:\n"
            for name, structure in library_structures.items():
                library_context += f"\n--- Library: {name} ---\n"
                library_context += f"Library: {structure.library_name} v{structure.library_version}\n"
                library_context += f"Definitions: {[d.name for d in structure.definitions]}\n"
                library_context += f"Valuesets: {[v.name for v in structure.valuesets]}\n"
        
        prompt = f"""
You are a CQL (Clinical Quality Language) expert. Parse the following MAIN CQL file and extract its complete structure.

Main CQL Content:
{cql_content}
{library_context}

Extract and return a JSON object with this EXACT structure:
{{
    "library_name": "string - the library name",
    "library_version": "string - the library version",
    "using_model": "string - the data model used (e.g., 'QDM', 'FHIR')",
    "using_version": "string - the model version",
    "context": "string - the context (usually 'Patient')",
    "includes": [
        {{
            "name": "string - library name",
            "version": "string - library version",
            "alias": "string - the alias used in code"
        }}
    ],
    "valuesets": [
        {{
            "name": "string - valueset name",
            "oid": "string - the OID (e.g., 'urn:oid:2.16.840.1.113883.3.464.1003.104.12.1011')"
        }}
    ],
    "codes": [
        {{
            "name": "string - code name",
            "code": "string - the code value",
            "system": "string - code system name"
        }}
    ],
    "definitions": [
        {{
            "name": "string - definition name",
            "logic": "string - the complete logic/expression",
            "type": "string - one of: 'population', 'expression', 'function', 'measure'",
            "references": ["list of other definitions this one references"]
        }}
    ],
    "populations": ["list of population definition names (Initial Population, Denominator, Numerator, etc.)"],
    "parameters": [
        {{
            "name": "string - parameter name",
            "type": "string - parameter type (e.g., 'Interval<DateTime>')"
        }}
    ]
}}

Important extraction rules:
1. For includes, extract the exact library name, version, and alias from statements like:
   "include MATGlobalCommonFunctionsQDM version '8.0.000' called Global"
2. For valuesets, extract both the name and the complete OID
3. For definitions, include the COMPLETE logic, not just a summary
4. Identify which definitions are populations (Initial Population, Denominator, Numerator, etc.)
5. Track which definitions reference other definitions or library functions
6. Extract any direct code definitions (not valuesets)
7. Include any parameters defined

Return ONLY the JSON object, no additional text.
"""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a CQL parsing expert. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)

            # Use universal unwrapper to handle any wrapper format
            result = unwrap_json_response(result)

            # Convert to Pydantic model for validation
            structure = CQLStructure(**result)
            
            logger.info(f"Parsed CQL: {structure.library_name} v{structure.library_version}")
            logger.info(f"  - {len(structure.includes)} includes")
            logger.info(f"  - {len(structure.valuesets)} valuesets")
            logger.info(f"  - {len(structure.definitions)} definitions")
            logger.info(f"  - {len(structure.populations)} populations")
            
            return structure
            
        except Exception as e:
            logger.error(f"Failed to parse CQL with LLM: {e}")
            # Return minimal structure as fallback
            return CQLStructure(
                library_name="Unknown",
                library_version="0.0.0"
            )
    
    def find_library_dependencies(self, structure: CQLStructure) -> Dict[str, List[str]]:
        """
        Analyze which valuesets and functions come from which libraries.
        
        Returns:
            Dictionary mapping library alias to list of used elements
        """
        dependencies = {}
        
        for definition in structure.definitions:
            # Look for library references in the logic
            for include in structure.includes:
                alias = include.alias
                if f"{alias}." in definition.logic:
                    if alias not in dependencies:
                        dependencies[alias] = []
                    # Extract what's being called from this library
                    import re
                    pattern = rf'{alias}\.(["\w]+)'
                    matches = re.findall(pattern, definition.logic)
                    dependencies[alias].extend(matches)
        
        # Remove duplicates
        for alias in dependencies:
            dependencies[alias] = list(set(dependencies[alias]))
        
        return dependencies