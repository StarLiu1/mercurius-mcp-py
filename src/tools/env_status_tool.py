#!/usr/bin/env python3
"""
Tool to check environment variable status and provide setup guidance.
"""

import logging
import os
from pathlib import Path
from typing import Dict, Any
from config.settings import settings

logger = logging.getLogger(__name__)


async def check_environment_status_tool() -> Dict[str, Any]:
    """
    Check environment variable status and provide setup guidance.
    
    Returns:
        Dict with environment status and setup instructions
    """
    
    # Get detailed environment file status
    env_file_status = settings.get_env_file_status()
    
    # Check all environment variables
    env_status = {
        "llm_provider": {
            "current": settings.llm_provider,
            "openai_api_key": "SET" if settings.openai_api_key else "NOT SET",
            "azure_openai_api_key": "SET" if settings.azure_openai_api_key else "NOT SET", 
            "azure_openai_endpoint": "SET" if settings.azure_openai_endpoint else "NOT SET",
            "anthropic_api_key": "SET" if settings.anthropic_api_key else "NOT SET"
        },
        "vsac": {
            "username": "SET" if settings.vsac_username else "NOT SET",
            "password": "SET" if settings.vsac_password else "NOT SET"
        },
        "database": {
            "user": settings.database_user,
            "endpoint": settings.database_endpoint,
            "name": settings.database_name,
            "password": "SET" if settings.database_password else "NOT SET",
            "schema": settings.omop_database_schema
        }
    }
    
    # Check direct environment variables (to see if issue is loading vs. setting)
    direct_env_check = {}
    env_vars = ['VSAC_USERNAME', 'VSAC_PASSWORD', 'DATABASE_PASSWORD', 'LLM_PROVIDER', 
                'OPENAI_API_KEY', 'AZURE_OPENAI_API_KEY', 'AZURE_OPENAI_ENDPOINT', 'ANTHROPIC_API_KEY']
    for var in env_vars:
        value = os.getenv(var)
        direct_env_check[var] = "SET" if value else "NOT SET"
    
    # Determine readiness status
    llm_ready = False
    if settings.llm_provider == "openai" and settings.openai_api_key:
        llm_ready = True
    elif settings.llm_provider == "azure-openai" and settings.azure_openai_api_key and settings.azure_openai_endpoint:
        llm_ready = True
    elif settings.llm_provider == "anthropic" and settings.anthropic_api_key:
        llm_ready = True
    
    vsac_ready = bool(settings.vsac_username and settings.vsac_password)
    database_ready = bool(settings.database_password)
    
    readiness = {
        "llm_parsing": llm_ready,
        "vsac_integration": vsac_ready,
        "omop_mapping": database_ready,
        "overall": llm_ready and vsac_ready and database_ready
    }
    
    # Generate setup instructions
    setup_instructions = []
    
    if not env_file_status['env_file_exists']:
        setup_instructions.append("❌ .env file does not exist - run setup_env.py to create it")
    
    if not llm_ready:
        if settings.llm_provider == "openai":
            setup_instructions.append("Set OPENAI_API_KEY in your .env file")
        elif settings.llm_provider == "azure-openai":
            setup_instructions.append("Set AZURE_OPENAI_API_KEY and AZURE_OPENAI_ENDPOINT in your .env file")
        elif settings.llm_provider == "anthropic":
            setup_instructions.append("Set ANTHROPIC_API_KEY in your .env file")
    
    if not vsac_ready:
        setup_instructions.append("Set VSAC_USERNAME and VSAC_PASSWORD (your UMLS credentials) in your .env file")
    
    if not database_ready:
        setup_instructions.append("Set DATABASE_PASSWORD in your .env file")
    
    # Create missing env file template
    env_template = []
    env_template.append("# LLM Provider Configuration")
    env_template.append(f"LLM_PROVIDER={settings.llm_provider}")
    env_template.append("")
    
    if settings.llm_provider == "openai":
        env_template.append("# OpenAI")
        env_template.append("OPENAI_API_KEY=your_openai_api_key_here")
    elif settings.llm_provider == "azure-openai":
        env_template.append("# Azure OpenAI")
        env_template.append("AZURE_OPENAI_API_KEY=your_azure_api_key_here")
        env_template.append("AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/")
        env_template.append("AZURE_OPENAI_MODEL=gpt-4")
    elif settings.llm_provider == "anthropic":
        env_template.append("# Anthropic")
        env_template.append("ANTHROPIC_API_KEY=your_anthropic_api_key_here")
    
    env_template.extend([
        "",
        "# VSAC (UMLS) Credentials",
        "VSAC_USERNAME=your_umls_username",
        "VSAC_PASSWORD=your_umls_password",
        "",
        "# Database Configuration",
        f"DATABASE_USER={settings.database_user}",
        f"DATABASE_ENDPOINT={settings.database_endpoint}",
        f"DATABASE_NAME={settings.database_name}",
        "DATABASE_PASSWORD=your_database_password",
        f"OMOP_DATABASE_SCHEMA={settings.omop_database_schema}"
    ])
    
    return {
        "environment_status": env_status,
        "direct_environment_check": direct_env_check,
        "env_file_status": env_file_status,
        "readiness": readiness,
        "setup_required": not readiness["overall"],
        "setup_instructions": setup_instructions,
        "diagnostics": {
            "pydantic_settings_loading": "Environment variables loaded through Pydantic Settings",
            "env_file_path_used": env_file_status['env_file_path'],
            "potential_issues": []
        },
        "tool_capabilities": {
            "parse_nl_to_cql": {"requires": "LLM credentials", "ready": llm_ready},
            "fetch_multiple_vsac": {"requires": "VSAC credentials", "ready": vsac_ready},
            "map_vsac_to_omop": {"requires": "All credentials", "ready": readiness["overall"]},
            "debug_vsac_omop_pipeline": {"requires": "Varies by step", "ready": True}
        },
        "env_file_template": "\n".join(env_template),
        "quick_fixes": [
            "1. Run: python setup_env.py (in project root)",
            "2. Or manually create .env file in project root", 
            "3. Restart MCP server after creating .env file",
            "4. Use check_environment_status() to verify"
        ],
        "usage_tips": [
            "All tools automatically use environment variables - no need to pass credentials manually",
            "You can still override by passing parameters explicitly to tools",
            "Use the config://current resource to check current environment status",
            "If .env file exists but values aren't loading, check file location and restart server"
        ]
    }
    
    # Add potential issues to diagnostics
    diagnostics = []
    if not env_file_status['env_file_exists']:
        diagnostics.append(".env file not found - this is the most likely issue")
    if env_file_status['env_file_exists'] and not vsac_ready:
        diagnostics.append("Environment file exists but VSAC credentials not loaded - check file format")
    if env_file_status['current_working_directory'] != env_file_status['project_root']:
        diagnostics.append("Working directory differs from project root - but this should be handled automatically")
    
    # Update the result with diagnostics
    result = {
        "environment_status": env_status,
        "direct_environment_check": direct_env_check,
        "env_file_status": env_file_status,
        "readiness": readiness,
        "setup_required": not readiness["overall"],
        "setup_instructions": setup_instructions,
        "diagnostics": {
            "pydantic_settings_loading": "Environment variables loaded through Pydantic Settings",
            "env_file_path_used": env_file_status['env_file_path'],
            "potential_issues": diagnostics
        },
        "tool_capabilities": {
            "parse_nl_to_cql": {"requires": "LLM credentials", "ready": llm_ready},
            "fetch_multiple_vsac": {"requires": "VSAC credentials", "ready": vsac_ready},
            "map_vsac_to_omop": {"requires": "All credentials", "ready": readiness["overall"]},
            "debug_vsac_omop_pipeline": {"requires": "Varies by step", "ready": True}
        },
        "env_file_template": "\n".join(env_template),
        "quick_fixes": [
            "1. Run: python setup_env.py (in project root)",
            "2. Or manually create .env file in project root", 
            "3. Restart MCP server after creating .env file",
            "4. Use check_environment_status() to verify"
        ],
        "usage_tips": [
            "All tools automatically use environment variables - no need to pass credentials manually",
            "You can still override by passing parameters explicitly to tools",
            "Use the config://current resource to check current environment status",
            "If .env file exists but values aren't loading, check file location and restart server"
        ]
    }
    
    return result