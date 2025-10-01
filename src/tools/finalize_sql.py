"""
Tool 6: Replace placeholders with OMOP concept IDs (final step).
"""

import logging
import re
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


def flatten_concept_ids(concept_ids: List) -> List[str]:
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


async def finalize_sql_tool(
    sql_query: str,
    placeholder_mappings: Dict[str, List[str]],
    sql_dialect: str = "postgresql"
) -> Dict[str, Any]:
    """
    Tool 6: Replace placeholders with OMOP concept IDs (final step).
    
    This is a purely programmatic step - no LLM involved.
    
    Replaces patterns like:
    - PLACEHOLDER_NAME
    - (PLACEHOLDER_NAME)
    - IN (PLACEHOLDER_NAME)
    - IN (SELECT value FROM PLACEHOLDER_NAME)
    - SELECT value FROM (PLACEHOLDER_NAME)
    
    With actual concept ID lists.
    
    Args:
        sql_query: SQL query with placeholders (from Tool 5 or Tool 3)
        placeholder_mappings: Dict of placeholder -> concept_ids from Tool 2
        sql_dialect: Target SQL dialect (for dialect-specific handling)
        
    Returns:
        Dict with:
        - final_sql: SQL with all placeholders replaced
        - replacements_made: Number of placeholders replaced
        - unmapped_placeholders: List of placeholders not found in mappings
        - statistics: Replacement statistics
    """
    try:
        logger.info("=" * 80)
        logger.info("TOOL 6: Replacing Placeholders (Programmatic)")
        logger.info("=" * 80)
        
        if not sql_query:
            return {
                "success": False,
                "error": "No SQL query provided",
                "step": "finalize_sql"
            }
        
        if not placeholder_mappings:
            logger.warning("No placeholder mappings provided")
            return {
                "success": False,
                "error": "No placeholder mappings provided",
                "step": "finalize_sql"
            }
        
        logger.info(f"SQL length: {len(sql_query):,} characters")
        logger.info(f"Placeholder mappings available: {len(placeholder_mappings)}")
        logger.info(f"Target dialect: {sql_dialect}")
        
        # Find all placeholders in the SQL
        placeholders_in_sql = re.findall(r'PLACEHOLDER_[\w_]+', sql_query)
        unique_placeholders = set(placeholders_in_sql)
        
        logger.info(f"Found {len(unique_placeholders)} unique placeholders in SQL")
        
        # Replace each placeholder
        final_sql = sql_query
        replacements_made = 0
        unmapped_placeholders = []
        
        for placeholder in unique_placeholders:
            if placeholder in placeholder_mappings:
                concept_ids = placeholder_mappings[placeholder]
                
                if concept_ids:
                    # Flatten concept IDs in case they're grouped with parentheses
                    flattened_ids = flatten_concept_ids(concept_ids)
                    concepts_str = ", ".join(str(c) for c in flattened_ids)
                    logger.debug(f"Flattened {len(concept_ids)} items to {len(flattened_ids)} concept IDs")
                else:
                    # No concepts found - use NULL
                    concepts_str = "NULL"
                    flattened_ids = []
                    logger.warning(f"No OMOP concepts for {placeholder}")
                
                # Handle different placeholder patterns
                replaced = False
                
                # Pattern 1: IN (SELECT value FROM PLACEHOLDER_...)
                subquery_pattern = f"IN (SELECT value FROM {placeholder})"
                if subquery_pattern in final_sql:
                    if sql_dialect == "sqlserver" and flattened_ids:
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
                        if sql_dialect == "sqlserver" and flattened_ids:
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
            "placeholders_found": len(unique_placeholders),
            "placeholders_replaced": replacements_made,
            "unmapped_placeholders": len(unmapped_placeholders),
            "remaining_placeholders": len(remaining),
            "total_concept_ids_used": sum(
                len(flatten_concept_ids(concepts)) 
                for placeholder, concepts in placeholder_mappings.items() 
                if placeholder in unique_placeholders
            ),
            "sql_length_before": len(sql_query),
            "sql_length_after": len(final_sql)
        }
        
        logger.info("=" * 80)
        logger.info("✓ Tool 6 Complete: Placeholders Replaced")
        logger.info(f"  - Placeholders replaced: {replacements_made}/{len(unique_placeholders)}")
        logger.info(f"  - Unmapped placeholders: {len(unmapped_placeholders)}")
        logger.info(f"  - Remaining placeholders: {len(remaining)}")
        logger.info(f"  - Total concept IDs used: {statistics['total_concept_ids_used']}")
        logger.info(f"  - SQL length: {len(sql_query):,} → {len(final_sql):,} characters")
        logger.info("=" * 80)
        
        success = len(remaining) == 0
        
        return {
            "success": success,
            "final_sql": final_sql,
            "original_sql": sql_query,
            "replacements_made": replacements_made,
            "unmapped_placeholders": unmapped_placeholders,
            "remaining_placeholders": remaining,
            "statistics": statistics
        }
        
    except Exception as e:
        logger.error(f"Tool 6 failed: {e}", exc_info=True)
        return {
            "success": False,
            "final_sql": sql_query,
            "error": str(e),
            "step": "finalize_sql"
        }