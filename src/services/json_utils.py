"""
JSON utility functions for handling various model response formats.
"""
import json
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def unwrap_json_response(response: Any) -> Any:
    """
    Universal JSON unwrapper that handles various model response formats.

    Handles:
    - Direct JSON objects (no wrapper)
    - Single-key wrappers with dict values
    - Single-key wrappers with stringified JSON values
    - Nested wrappers

    Args:
        response: The response from an LLM model

    Returns:
        The unwrapped JSON object
    """
    if not isinstance(response, dict):
        return response

    # If the response has the expected structure fields, it's already unwrapped
    # Check for common top-level keys that indicate valid content
    expected_keys = {
        'library_name', 'library_version', 'sql', 'ctes', 'errors',
        'is_valid', 'corrected_sql', 'populations', 'definitions',
        'valuesets', 'includes', 'parameters'
    }

    if any(key in response for key in expected_keys):
        logger.debug("Response appears to be already unwrapped")
        return response

    # If it's a single-key dictionary, try to unwrap it
    if len(response) == 1:
        wrapper_key = list(response.keys())[0]
        wrapped_value = response[wrapper_key]

        logger.info(f"Detected single-key wrapper: '{wrapper_key}'")

        # If the wrapped value is a string, try to parse it as JSON
        if isinstance(wrapped_value, str):
            try:
                logger.info(f"Parsing stringified JSON from '{wrapper_key}' wrapper")
                unwrapped = json.loads(wrapped_value)

                # Recursively unwrap in case there are nested wrappers
                return unwrap_json_response(unwrapped)
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse stringified JSON: {e}")
                # Return the original response if parsing fails
                return response

        # If it's a dict, recursively unwrap it
        elif isinstance(wrapped_value, dict):
            logger.info(f"Unwrapping dictionary from '{wrapper_key}' wrapper")
            return unwrap_json_response(wrapped_value)

        # Otherwise, return the wrapped value as-is
        else:
            return wrapped_value

    # If it's a multi-key dictionary but doesn't have expected keys,
    # it might still be wrapped in an unusual way
    # Check for common wrapper patterns
    wrapper_keys = {'result', 'output', 'response', 'data', 'final', 'json', 'final JSON'}

    for key in wrapper_keys:
        if key in response:
            logger.info(f"Found potential wrapper key: '{key}'")
            wrapped_value = response[key]

            # Try to unwrap based on the value type
            if isinstance(wrapped_value, str):
                try:
                    unwrapped = json.loads(wrapped_value)
                    return unwrap_json_response(unwrapped)
                except json.JSONDecodeError:
                    pass
            elif isinstance(wrapped_value, dict):
                return unwrap_json_response(wrapped_value)

    # If no unwrapping was needed or possible, return the original
    logger.debug("No unwrapping needed or possible, returning original response")
    return response


def safe_json_parse(text: str) -> Optional[Dict]:
    """
    Safely parse JSON text with error handling.

    Args:
        text: JSON string to parse

    Returns:
        Parsed JSON object or None if parsing fails
    """
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        logger.error(f"JSON parsing failed: {e}")
        logger.debug(f"Failed to parse: {text[:500]}...")  # Log first 500 chars
        return None