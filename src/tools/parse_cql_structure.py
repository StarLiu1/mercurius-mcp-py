"""
Tool 1: Parse CQL structure and analyze dependencies using LLM.
"""

import logging
from typing import Dict, Any, Optional
from pathlib import Path
import yaml

from services.cql_parser import CQLParser
from services.library_resolver import LibraryResolver
# from services.mcp_client_simplified import SimplifiedMCPClient

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


async def parse_cql_structure_tool(
    cql_content: str,
    cql_file_path: Optional[str] = None,
    config_path: str = "config.yaml"
) -> Dict[str, Any]:
    """
    Tool 1: Parse CQL structure and analyze dependencies using LLM.
    
    Steps:
    - Parse main CQL with LLM
    - Find and read library files
    - Parse library structures
    - Analyze dependencies between main and libraries
    
    Args:
        cql_content: The main CQL content to parse
        cql_file_path: Optional path to CQL file (for finding libraries)
        config_path: Path to config.yaml
        
    Returns:
        Dict with:
        - parsed_structure: Complete CQL structure
        - library_files: Dict of library content
        - library_definitions: Parsed library structures
        - dependency_analysis: How libraries interact
        - statistics: Parsing statistics
    """
    try:
        logger.info("=" * 80)
        logger.info("TOOL 1: Parsing CQL Structure with LLM")
        logger.info("=" * 80)
        
        # Load configuration
        config = load_config(config_path)
        logger.info(f"Loaded config - Provider: {config.get('model_provider')}")
        
        # Initialize CQL parser
        parser = CQLParser(config)
        

        library_files = {}
        if cql_file_path and cql_file_path != "inline":
            # Import here to avoid circular imports
            from services.library_resolver import LibraryResolver

            resolver = LibraryResolver()

            # ✅ ADD: Log the CQL file path
            logger.info(f"CQL file path provided: {cql_file_path}")
            logger.info(f"CQL file exists: {Path(cql_file_path).exists()}")
            logger.info(f"CQL parent directory: {Path(cql_file_path).parent}")
            
            # Parse include statements from CQL
            includes = resolver.parse_includes(cql_content)
            logger.info(f"Found {len(includes)} include statements in CQL")
            
            # Read the actual library files
            library_files = resolver.read_library_files(cql_file_path, includes)
            logger.info(f"Successfully read {len(library_files)} library files")
        
        
        # Parse CQL with LLM
        logger.info(f"Parsing CQL with {len(library_files)} library files...")
        parsed_structure = parser.parse(cql_content, library_files)
        
        # Extract dependency information
        dependency_analysis = {
            "includes": [inc.dict() for inc in parsed_structure.includes],
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
        
        # Identify valueset sources
        for valueset in parsed_structure.valuesets:
            dependency_analysis["valueset_sources"][valueset.name] = "main"

        # ✅ NEW: Add library valuesets to dependency analysis
        for lib_name, lib_def in parsed_structure.library_definitions.items():
            if isinstance(lib_def, dict) and 'valuesets' in lib_def:
                for vs in lib_def.get('valuesets', []):
                    vs_name = vs.get('name') if isinstance(vs, dict) else vs.name
                    dependency_analysis["valueset_sources"][vs_name] = lib_name
        
        # Add SQL hints based on populations
        if parsed_structure.populations:
            dependency_analysis["sql_structure_hints"].append(
                f"Create CTEs for populations: {', '.join(parsed_structure.populations)}"
            )
        
        # Compile statistics
        statistics = {
            "library_name": parsed_structure.library_name,
            "library_version": parsed_structure.library_version,
            "includes_count": len(parsed_structure.includes),
            "valuesets_count": len(parsed_structure.valuesets),
            "definitions_count": len(parsed_structure.definitions),
            "populations_count": len(parsed_structure.populations),
            "parameters_count": len(parsed_structure.parameters),
            "library_files_found": len(library_files),
            "library_definitions_parsed": len(parsed_structure.library_definitions),
            "library_valuesets_count": sum(
                len(lib_def.get('valuesets', [])) if isinstance(lib_def, dict) else len(lib_def.valuesets)
                for lib_def in parsed_structure.library_definitions.values()
            )
        }
        
        logger.info("=" * 80)
        logger.info("✓ Tool 1 Complete: CQL Structure Parsed")
        logger.info(f"  - Library: {parsed_structure.library_name} v{parsed_structure.library_version}")
        logger.info(f"  - Definitions: {len(parsed_structure.definitions)}")
        logger.info(f"  - Main Valuesets: {len(parsed_structure.valuesets)}")
        logger.info(f"  - Library Valuesets: {statistics['library_valuesets_count']}")
        logger.info(f"  - Includes: {len(parsed_structure.includes)}")
        logger.info(f"  - Library files: {len(library_files)}")
        logger.info("=" * 80)
        
        return {
            "success": True,
            "parsed_structure": parsed_structure.dict(),
            "library_files": library_files,
            "library_definitions": {
                k: v.dict() if hasattr(v, 'dict') else v 
                for k, v in parsed_structure.library_definitions.items()
            },
            "dependency_analysis": dependency_analysis,
            "statistics": statistics
        }
        
    except Exception as e:
        logger.error(f"Tool 1 failed: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e),
            "step": "parse_cql_structure"
        }