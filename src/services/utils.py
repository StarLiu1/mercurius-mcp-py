"""
Utility functions for the pipeline
"""

import os
import re
import yaml
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Any
from pathlib import Path


def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from YAML file
    
    Args:
        config_path: Path to config file
        
    Returns:
        Configuration dictionary
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at {config_path}")
    
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def load_omop_schema(xml_path: str) -> str:
    """Load OMOP CDM schema from XML file
    
    Args:
        xml_path: Path to OMOP_CDM.xml file
        
    Returns:
        XML content as string for RAG context
    """
    if not os.path.exists(xml_path):
        raise FileNotFoundError(f"OMOP CDM schema not found at {xml_path}")
    
    with open(xml_path, 'r') as f:
        return f.read()


def parse_omop_tables(xml_content: str) -> Dict[str, Dict[str, Any]]:
    """Parse OMOP tables from XML content
    
    Args:
        xml_content: XML content as string
        
    Returns:
        Dictionary of table information
    """
    root = ET.fromstring(xml_content)
    tables = {}
    
    for table in root.findall(".//table"):
        table_name = table.get("name", "").lower()
        columns = []
        
        for column in table.findall("column"):
            columns.append({
                "name": column.get("name", "").lower(),
                "type": column.get("type", ""),
                "nullable": column.get("nullable", "") == "true",
                "remarks": column.get("remarks", "")
            })
        
        tables[table_name] = {
            "name": table_name,
            "columns": columns,
            "remarks": table.get("remarks", "")
        }
    
    return tables


def load_cql_file(file_path: str) -> str:
    """Load CQL file content
    
    Args:
        file_path: Path to CQL file
        
    Returns:
        CQL content as string
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CQL file not found at {file_path}")
    
    with open(file_path, 'r') as f:
        return f.read()


def load_library_files(cql_content: str, base_path: str) -> Dict[str, str]:
    """Load library files referenced in CQL
    
    Args:
        cql_content: Main CQL content
        base_path: Base path for library files
        
    Returns:
        Dictionary mapping library name to content
    """
    libraries = {}
    
    # Find include statements
    include_pattern = r"include\s+([^\s]+)(?:\s+version\s+'([^']+)')?"
    matches = re.findall(include_pattern, cql_content)
    
    for library_name, version in matches:
        # Try to find library file
        possible_paths = [
            os.path.join(base_path, f"{library_name}.cql"),
            os.path.join(base_path, library_name, f"{library_name}.cql"),
            os.path.join(os.path.dirname(base_path), library_name, f"{library_name}.cql")
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                with open(path, 'r') as f:
                    libraries[library_name] = f.read()
                break
    
    return libraries


def extract_value_set_oids(cql_content: str) -> List[str]:
    """Extract value set OIDs from CQL content
    
    Args:
        cql_content: CQL content
        
    Returns:
        List of OIDs
    """
    oids = []
    
    # Pattern for value set definitions
    valueset_pattern = r"valueset\s+\"[^\"]+\":\s+'(urn:oid:[0-9.]+)'"
    matches = re.findall(valueset_pattern, cql_content)
    oids.extend(matches)
    
    return list(set(oids))


def replace_placeholders(sql: str, mappings: Dict[str, List[str]]) -> str:
    """Replace placeholders with concept IDs
    
    Args:
        sql: SQL with placeholders
        mappings: Dictionary mapping placeholder to concept IDs
        
    Returns:
        SQL with concept IDs
    """
    result = sql
    
    for placeholder, concepts in mappings.items():
        if concepts:
            # Format concept IDs as SQL IN clause
            concept_list = ", ".join(concepts)
            # Replace placeholder with concept list
            result = result.replace(f"{{{{PLACEHOLDER_{placeholder}}}}}", f"({concept_list})")
            result = result.replace(f"PLACEHOLDER_{placeholder}", f"({concept_list})")
    
    return result


def verify_placeholder_replacement(sql: str) -> Dict[str, Any]:
    """Verify all placeholders have been replaced
    
    Args:
        sql: SQL to check
        
    Returns:
        Verification results
    """
    # Find remaining placeholders
    placeholder_pattern = r"(PLACEHOLDER_[A-Z_]+|\{\{PLACEHOLDER_[A-Z_]+\}\})"
    remaining = re.findall(placeholder_pattern, sql)
    
    return {
        "all_replaced": len(remaining) == 0,
        "remaining_placeholders": list(set(remaining)),
        "count": len(set(remaining))
    }


def format_sql(sql: str) -> str:
    """Format SQL for readability
    
    Args:
        sql: SQL to format
        
    Returns:
        Formatted SQL
    """
    # Basic formatting - can be enhanced
    keywords = [
        'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT JOIN', 'RIGHT JOIN',
        'INNER JOIN', 'ON', 'AND', 'OR', 'GROUP BY', 'ORDER BY',
        'HAVING', 'WITH', 'AS', 'UNION', 'UNION ALL', 'CASE', 'WHEN',
        'THEN', 'ELSE', 'END'
    ]
    
    formatted = sql
    for keyword in keywords:
        formatted = re.sub(
            rf'\b{keyword}\b',
            f'\n{keyword}',
            formatted,
            flags=re.IGNORECASE
        )
    
    # Clean up excessive newlines
    formatted = re.sub(r'\n+', '\n', formatted)
    formatted = formatted.strip()
    
    return formatted


def validate_omop_tables(sql: str, omop_tables: Dict[str, Any]) -> List[str]:
    """Validate that SQL references valid OMOP tables
    
    Args:
        sql: SQL to validate
        omop_tables: Dictionary of OMOP tables
        
    Returns:
        List of validation errors
    """
    errors = []
    
    # Extract table references from SQL
    table_pattern = r"FROM\s+([a-z_]+)|JOIN\s+([a-z_]+)"
    matches = re.findall(table_pattern, sql.lower())
    
    referenced_tables = set()
    for match in matches:
        for table in match:
            if table:
                referenced_tables.add(table)
    
    # Check if tables exist in OMOP
    for table in referenced_tables:
        if table not in omop_tables:
            errors.append(f"Table '{table}' not found in OMOP CDM")
    
    return errors