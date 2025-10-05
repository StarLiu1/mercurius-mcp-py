import json
import logging
from typing import Any, Dict, Optional, Union

logger = logging.getLogger(__name__)


def normalize_dict_param(
    param: Optional[Union[Dict[str, Any], str]], 
    param_name: str,
    required: bool = False
) -> Dict[str, Any]:
    """
    Normalize a parameter that should be a dict but might be JSON string.
    
    This handles Claude Desktop's parameter passing where nested dicts
    may be serialized to JSON strings.
    
    Args:
        param: The parameter to normalize (dict, JSON string, or None)
        param_name: Name for logging
        required: Whether to raise error if invalid
        
    Returns:
        Dict (empty if param was None/invalid and not required)
        
    Raises:
        ValueError: If required=True and param is invalid
    """
    if param is None:
        if required:
            raise ValueError(f"{param_name} is required but was None")
        return {}
    
    if isinstance(param, dict):
        logger.debug(f"{param_name}: using dict directly")
        return param
    
    if isinstance(param, str):
        # Try to parse as JSON
        try:
            parsed = json.loads(param)
            if isinstance(parsed, dict):
                logger.info(f"{param_name}: parsed from JSON string")
                return parsed
            else:
                logger.error(f"{param_name}: JSON parsed to {type(parsed)}, expected dict")
                if required:
                    raise ValueError(f"{param_name} parsed to wrong type: {type(parsed)}")
                return {}
        except json.JSONDecodeError as e:
            logger.error(f"{param_name}: failed to parse JSON - {e}")
            logger.debug(f"{param_name} value (first 200 chars): {param[:200]}")
            if required:
                raise ValueError(f"{param_name} is not valid JSON: {e}")
            return {}
    
    # Unexpected type
    logger.error(f"{param_name}: unexpected type {type(param)}")
    if required:
        raise ValueError(f"{param_name} must be dict or JSON string, got {type(param)}")
    return {}


def normalize_string_param(
    param: Optional[Union[str, Dict]], 
    param_name: str,
    default: str = ""
) -> str:
    """
    Normalize a parameter that should be a string.
    
    Args:
        param: The parameter to normalize
        param_name: Name for logging
        default: Default value if param is None/invalid
        
    Returns:
        String value
    """
    if param is None:
        return default
    
    if isinstance(param, str):
        return param
    
    if isinstance(param, dict):
        # Sometimes Claude wraps strings in dicts
        if param_name in param:
            logger.info(f"{param_name}: extracted from dict wrapper")
            return str(param[param_name])
        logger.error(f"{param_name}: got dict but no matching key")
        return default
    
    # Try to convert to string
    logger.warning(f"{param_name}: converting {type(param)} to string")
    return str(param)


def log_parameter_types(func_name: str, **kwargs):
    """Log all parameter types for debugging."""
    logger.info(f"=== Parameter types for {func_name} ===")
    for key, value in kwargs.items():
        value_type = type(value).__name__
        if isinstance(value, dict):
            preview = f"dict with {len(value)} keys"
        elif isinstance(value, str):
            preview = f"str: '{value[:50]}...'" if len(value) > 50 else f"str: '{value}'"
        elif isinstance(value, list):
            preview = f"list with {len(value)} items"
        else:
            preview = str(value)[:50]
        logger.info(f"  {key}: {value_type} = {preview}")