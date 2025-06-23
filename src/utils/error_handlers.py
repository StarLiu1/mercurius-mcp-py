import logging
from typing import Optional, List

logger = logging.getLogger(__name__)


class VSACError(Exception):
    """Custom exception for VSAC-related errors."""
    
    def __init__(self, message: str, code: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.code = code
        self.status_code = status_code


def handle_vsac_error(error, value_set_id: str):
    """Handle different types of VSAC errors and provide appropriate error messages."""
    logger.error(f"VSAC Error for {value_set_id}: {error}")
    
    if hasattr(error, 'response') and error.response:
        status = error.response.status_code
        
        if status == 401:
            raise VSACError(
                'VSAC authentication failed. Check your UMLS username and password.',
                'AUTH_FAILED',
                401
            )
        elif status == 403:
            raise VSACError(
                'VSAC access forbidden. Ensure your UMLS account has VSAC access enabled.',
                'ACCESS_FORBIDDEN',
                403
            )
        elif status == 404:
            raise VSACError(
                f'Value set not found: {value_set_id}. Verify the OID is correct.',
                'VALUESET_NOT_FOUND',
                404
            )
        elif status == 429:
            raise VSACError(
                'VSAC rate limit exceeded. Please wait before retrying.',
                'RATE_LIMIT',
                429
            )
        elif status >= 500:
            raise VSACError(
                'VSAC service temporarily unavailable. Please try again later.',
                'SERVICE_UNAVAILABLE',
                status
            )
        else:
            raise VSACError(
                f'VSAC API error ({status}): {getattr(error.response, "text", str(error))}',
                'API_ERROR',
                status
            )
    
    # Handle network errors
    if hasattr(error, 'request'):
        raise VSACError(
            'Unable to connect to VSAC. Check your internet connection.',
            'NETWORK_ERROR'
        )
    
    # Generic error fallback
    raise VSACError(
        f'Unexpected VSAC error: {str(error)}',
        'UNKNOWN_ERROR'
    )


def get_vsac_error_guidance(error: VSACError) -> List[str]:
    """Get guidance for resolving VSAC errors."""
    guidance_map = {
        'AUTH_FAILED': [
            'Verify your UMLS username and password',
            'Check if your UMLS account is active',
            'Try logging into https://uts.nlm.nih.gov/uts/ manually'
        ],
        'ACCESS_FORBIDDEN': [
            'Log into your UMLS account at https://uts.nlm.nih.gov/uts/',
            'Navigate to "Profile" and ensure VSAC access is enabled',
            'Contact UMLS support if access issues persist'
        ],
        'VALUESET_NOT_FOUND': [
            'Verify the ValueSet OID format (e.g., 2.16.840.1.113883.x.x.x)',
            'Check if the ValueSet exists in VSAC web interface',
            'Try searching for the ValueSet by name in VSAC'
        ],
        'RATE_LIMIT': [
            'Wait 60 seconds before retrying',
            'Reduce the number of concurrent requests',
            'Consider batching multiple ValueSet requests'
        ],
        'SERVICE_UNAVAILABLE': [
            'Wait a few minutes and retry',
            'Check VSAC service status',
            'Try during off-peak hours'
        ]
    }
    
    return guidance_map.get(error.code, [
        'Check the error message for specific details',
        'Verify your VSAC credentials and network connection',
        'Try again after a short wait'
    ])