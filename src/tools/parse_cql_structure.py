"""
Tool 1: Parse CQL structure and analyze dependencies using LLM.
"""

import logging
from typing import Dict, Any, Optional
from pathlib import Path
import yaml

from services.cql_parser import CQLParser
from services.library_resolver import LibraryResolver
from services.mcp_client_simplified import SimplifiedMCPClient

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
        
        # Read library files if path provided
        library_files = {}
        if cql_file_path and cql_file_path != "inline":
            cql_path = Path(cql_file_path)
            if cql_path.exists():
                cql_dir = cql_path.parent
                logger.info(f"Looking for library files in: {cql_dir}")
                
                for lib_file in cql_dir.glob("*.cql"):
                    if lib_file != cql_path:
                        try:
                            library_files[lib_file.stem] = lib_file.read_text()
                            logger.info(f"  ✓ Found library: {lib_file.name}")
                        except Exception as e:
                            logger.warning(f"  ✗ Could not read {lib_file}: {e}")
        
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
            "library_definitions_parsed": len(parsed_structure.library_definitions)
        }
        
        logger.info("=" * 80)
        logger.info("✓ Tool 1 Complete: CQL Structure Parsed")
        logger.info(f"  - Library: {parsed_structure.library_name} v{parsed_structure.library_version}")
        logger.info(f"  - Definitions: {len(parsed_structure.definitions)}")
        logger.info(f"  - Valuesets: {len(parsed_structure.valuesets)}")
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