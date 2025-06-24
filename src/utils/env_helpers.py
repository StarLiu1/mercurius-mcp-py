# src/utils/env_helpers.py
"""
Helper utilities for handling environment variable defaults in MCP tools.
"""

from typing import Optional, Any, Dict
from config.settings import settings


def apply_env_defaults(**kwargs) -> Dict[str, Any]:
    """
    Apply environment variable defaults to tool parameters.
    
    This function takes keyword arguments and applies environment variable defaults
    for any None values, using a predefined mapping.
    
    Args:
        **kwargs: Keyword arguments from tool function
        
    Returns:
        Dict with environment variable defaults applied
    """
    env_mapping = {
        'vsac_username': settings.vsac_username,
        'vsac_password': settings.vsac_password,
        'database_user': settings.database_user,
        'database_endpoint': settings.database_endpoint,
        'database_name': settings.database_name,
        'database_password': settings.database_password,
        'omop_database_schema': settings.omop_database_schema,
        'username': settings.vsac_username,  # Alias for vsac_username
        'password': settings.vsac_password,  # Alias for vsac_password
    }
    
    result = {}
    for key, value in kwargs.items():
        if value is None and key in env_mapping:
            result[key] = env_mapping[key]
        else:
            result[key] = value
    
    return result


def with_env_defaults(func):
    """
    Decorator that automatically applies environment variable defaults to tool function parameters.
    
    Usage:
        @mcp.tool()
        @with_env_defaults
        async def my_tool(param1: str, vsac_username: Optional[str] = None) -> dict:
            # vsac_username will automatically get settings.vsac_username if None
            pass
    """
    import functools
    import inspect
    
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        # Get function signature to map positional args to parameter names
        sig = inspect.signature(func)
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()
        
        # Apply environment defaults
        updated_params = apply_env_defaults(**bound_args.arguments)
        
        # Call original function with updated parameters
        return await func(**updated_params)
    
    return wrapper


# Specific helper functions for common credential patterns
def get_vsac_credentials(username: Optional[str] = None, password: Optional[str] = None) -> tuple[Optional[str], Optional[str]]:
    """Get VSAC credentials with environment variable fallback."""
    actual_username = username if username is not None else settings.vsac_username
    actual_password = password if password is not None else settings.vsac_password
    return actual_username, actual_password


def get_database_config(
    user: Optional[str] = None,
    endpoint: Optional[str] = None,
    name: Optional[str] = None,
    password: Optional[str] = None,
    schema: Optional[str] = None
) -> dict:
    """Get database configuration with environment variable fallback."""
    return {
        'user': user if user is not None else settings.database_user,
        'endpoint': endpoint if endpoint is not None else settings.database_endpoint,
        'name': name if name is not None else settings.database_name,
        'password': password if password is not None else settings.database_password,
        'schema': schema if schema is not None else settings.omop_database_schema
    }


def validate_required_credentials(credentials: Dict[str, Any], required_keys: list[str]) -> tuple[bool, list[str]]:
    """
    Validate that required credentials are present.
    
    Args:
        credentials: Dict of credential key-value pairs
        required_keys: List of required credential keys
        
    Returns:
        Tuple of (all_present: bool, missing_keys: list[str])
    """
    missing_keys = []
    for key in required_keys:
        if not credentials.get(key):
            missing_keys.append(key)
    
    return len(missing_keys) == 0, missing_keys


def create_credentials_error_response(missing_keys: list[str], operation: str) -> dict:
    """Create a standardized error response for missing credentials."""
    env_var_mapping = {
        'vsac_username': 'VSAC_USERNAME',
        'vsac_password': 'VSAC_PASSWORD',
        'database_user': 'DATABASE_USER',
        'database_endpoint': 'DATABASE_ENDPOINT',
        'database_name': 'DATABASE_NAME',
        'database_password': 'DATABASE_PASSWORD',
        'omop_database_schema': 'OMOP_DATABASE_SCHEMA',
        'username': 'VSAC_USERNAME',
        'password': 'VSAC_PASSWORD'
    }
    
    missing_env_vars = [env_var_mapping.get(key, key.upper()) for key in missing_keys]
    
    return {
        "success": False,
        "error": f"Required credentials missing for {operation}",
        "missing_credentials": missing_keys,
        "missing_environment_variables": missing_env_vars,
        "message": f"Set the following environment variables: {', '.join(missing_env_vars)}",
        "suggestion": "Run check_environment_status() for detailed setup instructions"
    }