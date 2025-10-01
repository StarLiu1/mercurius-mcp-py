"""
CQL Dependency Analyzer - Analyzes library dependencies and valuesets
"""

import json
import logging
import re
from typing import Dict, Any, List, Optional
from services.llm_factory import LLMFactory

logger = logging.getLogger(__name__)


class DependencyAnalyzer:
    """
    LLM-based analyzer to understand CQL library dependencies.
    Determines how libraries interact and what valuesets are needed.
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize dependency analyzer."""
        self.config = config
        self.component_name = 'dependency_analyzer'

        # Use LLMFactory to create client
        self.client, self.model = LLMFactory.create_component_client(config, self.component_name)
        logger.info(f"DependencyAnalyzer initialized with model: {self.model}")
        
    def analyze(self, main_cql: str, library_files: Dict[str, str]) -> Dict[str, Any]:
        """
        Analyze CQL dependencies and determine SQL structure.
        
        Args:
            main_cql: Main CQL content
            library_files: Dict of library_name -> library_content
            
        Returns:
            Dict with dependency analysis
        """
        logger.info(f"Analyzing dependencies for main CQL with {len(library_files)} libraries")
        
        # Build prompt
        prompt = self._build_prompt(main_cql, library_files)
        
        try:
            # Call OpenAI
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": self._get_system_prompt()},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            
            # Parse response
            result = json.loads(response.choices[0].message.content)
            logger.info(f"Dependency analysis complete: {len(result.get('dependencies', []))} dependencies found")
            
            return result
            
        except Exception as e:
            logger.error(f"Dependency analysis failed: {e}")
            # Return minimal structure on failure
            return {
                "dependencies": [],
                "valueset_sources": {},
                "sql_structure_hints": [],
                "library_usage": {}
            }
    
    def _get_system_prompt(self) -> str:
        """Get system prompt for dependency analysis."""
        return """You are a CQL dependency analyzer. Your job is to understand how CQL libraries interact and what valuesets are needed.

Analyze the provided CQL files and return a JSON object with:
1. dependencies: List of library dependencies and their usage
2. valueset_sources: Map of valueset OID to source library
3. sql_structure_hints: How library definitions should be integrated in SQL
4. library_usage: How each library is used in the main CQL

Focus on:
- Which valuesets come from which library
- How library definitions are referenced (e.g., AdultOutpatientEncounters."Qualifying Encounters")
- What SQL CTEs or subqueries will be needed for library definitions
- Dependencies between libraries

Return valid JSON only."""

    def _build_prompt(self, main_cql: str, library_files: Dict[str, str]) -> str:
        """Build prompt for dependency analysis."""
        prompt_parts = [
            "Analyze these CQL files to understand dependencies and valueset sources.",
            "",
            "MAIN CQL FILE:",
            "```cql",
            main_cql,
            "```",
            ""
        ]
        
        for lib_name, lib_content in library_files.items():
            prompt_parts.extend([
                f"LIBRARY: {lib_name}",
                "```cql",
                lib_content,
                "```",
                ""
            ])
        
        prompt_parts.extend([
            "Analyze the dependencies and return JSON with:",
            "1. Which valuesets come from which library",
            "2. How libraries are used in the main CQL",
            "3. SQL structure hints for integrating library definitions",
            "4. Any cross-library dependencies"
        ])
        
        return "\n".join(prompt_parts)