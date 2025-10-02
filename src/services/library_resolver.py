"""
CQL Library Resolver - Handles included libraries and multiple MCP calls
"""

import os
import re
import logging
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)


class LibraryResolver:
    """
    Resolves CQL library includes and coordinates multiple MCP calls.
    """
    
    def __init__(self):
        """
        Initialize library resolver.
        
        Args:
            mcp_client: SimplifiedMCPClient instance for valueset extraction
        """
        # self.mcp_client = mcp_client
        
    def parse_includes(self, cql_content: str) -> List[Dict[str, str]]:
        """
        Parse include statements from CQL content.
        
        Args:
            cql_content: CQL content to parse
            
        Returns:
            List of dicts with library info (name, version, alias)
        """
        includes = []
        
        # Pattern to match include statements
        # Example: include AdultOutpatientEncountersQDM version '4.0.000' called AdultOutpatientEncounters
        pattern = r"include\s+(\w+)\s+version\s+'([^']+)'(?:\s+called\s+(\w+))?"
        
        for match in re.finditer(pattern, cql_content):
            library_name = match.group(1)
            version = match.group(2)
            alias = match.group(3) or library_name  # Use library name if no alias
            
            includes.append({
                "name": library_name,
                "version": version,
                "alias": alias
            })
            
        logger.info(f"Found {len(includes)} library includes")
        return includes
    
    def locate_library_file(self, base_path: str, library_name: str, version: str) -> Optional[str]:
        """
        Locate library CQL file based on name and version.
        
        Args:
            base_path: Directory containing main CQL file
            library_name: Name of the library
            version: Version of the library
            
        Returns:
            Path to library file if found, None otherwise
        """
        # Determine base directory
        base_dir = Path(base_path).parent if os.path.isfile(base_path) else Path(base_path)
        
        # Handle version format: 4.0.000 -> 4-0-000
        version_dashes = version.replace('.', '-')
        
        # Try different file naming patterns
        patterns = [
            f"{library_name}-v{version_dashes}-QDM-5-6.cql",
            f"{library_name}-v{version_dashes}.cql",
            f"{library_name}-{version}.cql",
            f"{library_name}.cql"
        ]
        
        logger.debug(f"Looking for library {library_name} v{version} in {base_dir}")
        logger.debug(f"Trying patterns: {patterns}")
        
        for pattern in patterns:
            file_path = base_dir / pattern
            if file_path.exists():
                logger.info(f"Found library file: {file_path}")
                return str(file_path)
        
        # List available files for debugging
        available_files = list(base_dir.glob("*.cql"))
        logger.warning(f"Library file not found for {library_name} version {version}")
        logger.debug(f"Available CQL files in {base_dir}: {[f.name for f in available_files]}")
        
        return None
    
    def read_library_files(self, cql_file_path: str, includes: List[Dict[str, str]]) -> Dict[str, str]:
        """
        Read all included library files.
        
        Args:
            cql_file_path: Path to main CQL file
            includes: List of library includes from parse_includes
            
        Returns:
            Dict of library_alias -> library_content
        """
        library_files = {}
        
        for include in includes:
            library_path = self.locate_library_file(
                cql_file_path, 
                include["name"], 
                include["version"]
            )
            
            if library_path:
                try:
                    with open(library_path, 'r') as f:
                        content = f.read()
                        library_files[include["alias"]] = content
                        logger.info(f"Read library {include['alias']}: {len(content)} chars")
                except Exception as e:
                    logger.error(f"Failed to read library {library_path}: {e}")
            else:
                logger.warning(f"Skipping library {include['name']} - file not found")
        
        return library_files
    
    # def extract_library_valuesets(self, library_files: Dict[str, str]) -> Tuple[Dict[str, Any], Dict[str, List[str]]]:
    #     """
    #     Extract valuesets from all library files using multiple MCP calls.
        
    #     Args:
    #         library_files: Dict of library_alias -> library_content
            
    #     Returns:
    #         Tuple of (all_valuesets, all_placeholder_mappings)
    #     """
    #     all_valuesets = {}
    #     all_placeholder_mappings = {}
        
    #     for library_alias, library_content in library_files.items():
    #         logger.info(f"Extracting valuesets from library: {library_alias}")
            
    #         try:
    #             # Call MCP for this library
    #             result = self.mcp_client.extract_and_map_valuesets(library_content)
                
    #             # Get valuesets and placeholders
    #             library_valuesets = result.get("valuesets", {})
    #             library_placeholders = result.get("placeholders", {})
                
    #             # Prefix placeholders with library alias for uniqueness
    #             for placeholder, concepts in library_placeholders.items():
    #                 # Create a library-specific placeholder name
    #                 prefixed_placeholder = f"PLACEHOLDER_{library_alias.upper()}_{placeholder.replace('PLACEHOLDER_', '')}"
    #                 all_placeholder_mappings[prefixed_placeholder] = concepts
                
    #             # Merge valuesets (keeping library context)
    #             for oid, valueset_info in library_valuesets.items():
    #                 valueset_info["source_library"] = library_alias
    #                 all_valuesets[oid] = valueset_info
                
    #             logger.info(f"Library {library_alias}: {len(library_valuesets)} valuesets, {len(library_placeholders)} placeholders")
                
    #         except Exception as e:
    #             logger.error(f"Failed to extract valuesets from library {library_alias}: {e}")
        
    #     return all_valuesets, all_placeholder_mappings
    
    # def merge_valueset_results(self, main_valuesets: Dict, main_placeholders: Dict,
    #                           library_valuesets: Dict, library_placeholders: Dict) -> Tuple[Dict, Dict]:
    #     """
    #     Merge valueset results from main CQL and libraries.
        
    #     Args:
    #         main_valuesets: Valuesets from main CQL
    #         main_placeholders: Placeholders from main CQL
    #         library_valuesets: Valuesets from libraries
    #         library_placeholders: Placeholders from libraries
            
    #     Returns:
    #         Tuple of (merged_valuesets, merged_placeholders)
    #     """
    #     # Start with main valuesets
    #     merged_valuesets = main_valuesets.copy()
    #     merged_placeholders = main_placeholders.copy()
        
    #     # Add library valuesets (mark source)
    #     for oid, valueset_info in library_valuesets.items():
    #         if oid not in merged_valuesets:
    #             merged_valuesets[oid] = valueset_info
    #         else:
    #             # If duplicate, log it
    #             logger.info(f"Valueset {oid} found in multiple sources")
        
    #     # Add library placeholders
    #     merged_placeholders.update(library_placeholders)
        
    #     logger.info(f"Merged results: {len(merged_valuesets)} valuesets, {len(merged_placeholders)} placeholders")
        
    #     return merged_valuesets, merged_placeholders