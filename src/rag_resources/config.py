from typing import Dict, Any
from config.settings import settings


async def config_resource() -> Dict[str, Any]:
    """Get current configuration resource with environment variable status."""
    return {
        "server_info": {
            "name": "OMOP-NLP-Translator",
            "version": "1.0.0",
            "capabilities": ["nl-to-cql", "vsac-integration", "omop-mapping", "sql-generation"]
        },
        "llm_configuration": {
            "provider": settings.llm_provider,
            "model": {
                "openai": settings.openai_model,
                "azure_openai": settings.azure_openai_model,
                "anthropic": settings.anthropic_model
            }
        },
        "environment_variables": {
            "llm_credentials": {
                "OPENAI_API_KEY": "SET" if settings.openai_api_key else "NOT SET",
                "AZURE_OPENAI_API_KEY": "SET" if settings.azure_openai_api_key else "NOT SET",
                "AZURE_OPENAI_ENDPOINT": "SET" if settings.azure_openai_endpoint else "NOT SET",
                "ANTHROPIC_API_KEY": "SET" if settings.anthropic_api_key else "NOT SET"
            },
            "vsac_credentials": {
                "VSAC_USERNAME": "SET" if settings.vsac_username else "NOT SET",
                "VSAC_PASSWORD": "SET" if settings.vsac_password else "NOT SET"
            },
            "database_credentials": {
                "DATABASE_USER": settings.database_user,
                "DATABASE_ENDPOINT": settings.database_endpoint,
                "DATABASE_NAME": settings.database_name,
                "DATABASE_PASSWORD": "SET" if settings.database_password else "NOT SET",
                "OMOP_DATABASE_SCHEMA": settings.omop_database_schema
            }
        },
        "auto_defaults": {
            "description": "All tools automatically use environment variables as defaults",
            "credentials_required": {
                "vsac_tools": ["VSAC_USERNAME", "VSAC_PASSWORD"],
                "omop_mapping": ["DATABASE_PASSWORD"],
                "llm_parsing": ["LLM_PROVIDER specific key"]
            },
            "override_instructions": "Pass parameters explicitly to tools to override environment variables"
        },
        "usage_examples": {
            "simple_vsac_fetch": "fetch_multiple_vsac(['2.16.840.1.113883.3.464.1003.103.12.1001']) - uses env vars automatically",
            "override_vsac_credentials": "fetch_multiple_vsac(['oid'], 'custom_user', 'custom_pass') - uses custom credentials",
            "full_pipeline": "map_vsac_to_omop('cql_query') - uses all env vars automatically"
        }
    }