import json
from typing import List

def format_list_with_double_quotes(items: List[str]) -> str:
    """
    Format a list of strings with double quotes around each item.
    
    Args:
        items: List of strings
        
    Returns:
        String representation with double quotes around each item
        
    Example:
        Input: ['item1', 'item2', 'item3']
        Output: '["item1", "item2", "item3"]'
    """
    return json.dumps(items)

def format_oids_for_display(oids: List[str]) -> str:
    """
    Format OID list with double quotes for easy copy-paste.
    
    Args:
        oids: List of OID strings
        
    Returns:
        Formatted string with double quotes
    """
    return json.dumps(oids)