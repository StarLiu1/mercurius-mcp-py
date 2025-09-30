import os
from pathlib import Path
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # LLM Configuration
    llm_provider: str = "openai"
    
    # OpenAI
    openai_api_key: Optional[str] = None
    openai_model: str = "gpt-4-turbo"
    
    # Azure OpenAI
    azure_openai_api_key: Optional[str] = None
    azure_openai_endpoint: Optional[str] = None
    azure_openai_model: str = "gpt-4"
    azure_openai_api_version: str = "2024-02-15-preview"
    
    # Anthropic
    anthropic_api_key: Optional[str] = None
    anthropic_model: str = "claude-3-opus-20240229"
    
    # VSAC Configuration
    vsac_username: Optional[str] = None
    vsac_password: Optional[str] = None
    
    # Database Configuration
    database_user: str = "dbadmin"
    database_endpoint: str = "52.167.131.85"
    database_name: str = "tufts"
    database_password: Optional[str] = None
    omop_database_schema: str = "dbo"
    
    # Server Configuration
    mcp_request_timeout: int = 30000
    mcp_tool_timeout: int = 60000

    loinc_username: Optional[str] = None
    loinc_password: Optional[str] = None
    
    class Config:
        # Find .env file relative to project root, not current working directory
        env_file = os.path.join(Path(__file__).parent.parent.parent, ".env")
        case_sensitive = False
        
        # Explicit environment variable mapping to ensure UPPERCASE consistency
        fields = {
            # LLM Configuration
            'llm_provider': {'env': 'LLM_PROVIDER'},
            
            # OpenAI
            'openai_api_key': {'env': 'OPENAI_API_KEY'},
            'openai_model': {'env': 'OPENAI_MODEL'},
            
            # Azure OpenAI  
            'azure_openai_api_key': {'env': 'AZURE_OPENAI_API_KEY'},
            'azure_openai_endpoint': {'env': 'AZURE_OPENAI_ENDPOINT'},
            'azure_openai_model': {'env': 'AZURE_OPENAI_MODEL'},
            'azure_openai_api_version': {'env': 'AZURE_OPENAI_API_VERSION'},
            
            # Anthropic
            'anthropic_api_key': {'env': 'ANTHROPIC_API_KEY'},
            'anthropic_model': {'env': 'ANTHROPIC_MODEL'},
            
            # VSAC Configuration
            'vsac_username': {'env': 'VSAC_USERNAME'},
            'vsac_password': {'env': 'VSAC_PASSWORD'},
            
            # Database Configuration
            'database_user': {'env': 'DATABASE_USER'},
            'database_endpoint': {'env': 'DATABASE_ENDPOINT'},
            'database_name': {'env': 'DATABASE_NAME'},
            'database_password': {'env': 'DATABASE_PASSWORD'},
            'omop_database_schema': {'env': 'OMOP_DATABASE_SCHEMA'},
            
            # Server Configuration
            'mcp_request_timeout': {'env': 'MCP_REQUEST_TIMEOUT'},
            'mcp_tool_timeout': {'env': 'MCP_TOOL_TIMEOUT'}
        }
        
    def get_env_file_status(self) -> dict:
        """Get information about the .env file location and status."""
        env_file_path = self.Config.env_file
        return {
            "env_file_path": env_file_path,
            "env_file_exists": os.path.exists(env_file_path),
            "current_working_directory": os.getcwd(),
            "settings_file_location": str(Path(__file__).parent),
            "project_root": str(Path(__file__).parent.parent.parent),
            "relative_env_would_be": os.path.join(os.getcwd(), ".env"),
            "relative_env_exists": os.path.exists(os.path.join(os.getcwd(), ".env"))
        }
    
    def get_environment_variable_mapping(self) -> dict:
        """Get the mapping of Python attributes to environment variable names."""
        return {
            field_name: field_info.get('env', field_name.upper())
            for field_name, field_info in self.Config.fields.items()
        }


# Create settings instance
settings = Settings()

# Debug function to check environment loading
def debug_environment_loading():
    """Debug function to check environment variable loading."""
    print("🔍 Environment Loading Debug:")
    print(f"   Working Directory: {os.getcwd()}")
    print(f"   Settings File: {__file__}")
    print(f"   Project Root: {Path(__file__).parent.parent.parent}")
    
    env_status = settings.get_env_file_status()
    print(f"   Env File Path: {env_status['env_file_path']}")
    print(f"   Env File Exists: {env_status['env_file_exists']}")
    
    # Show environment variable mapping
    print(f"\n🗺️  Environment Variable Mapping:")
    mapping = settings.get_environment_variable_mapping()
    for python_attr, env_var in mapping.items():
        print(f"   {python_attr} -> {env_var}")
    
    # Check some key variables
    print(f"\n🔑 Key Environment Variables (via Pydantic):")
    print(f"   VSAC_USERNAME: {'SET' if settings.vsac_username else 'NOT SET'}")
    print(f"   VSAC_PASSWORD: {'SET' if settings.vsac_password else 'NOT SET'}")
    print(f"   DATABASE_PASSWORD: {'SET' if settings.database_password else 'NOT SET'}")
    print(f"   LLM_PROVIDER: {settings.llm_provider}")
    
    # Check environment variables directly
    print(f"\n🌐 Direct Environment Check:")
    env_vars = ['VSAC_USERNAME', 'VSAC_PASSWORD', 'DATABASE_PASSWORD', 'LLM_PROVIDER',
                'OPENAI_API_KEY', 'AZURE_OPENAI_API_KEY', 'AZURE_OPENAI_ENDPOINT', 'ANTHROPIC_API_KEY']
    for var in env_vars:
        value = os.getenv(var)
        print(f"   {var}: {'SET' if value else 'NOT SET'}")
    
    return env_status


if __name__ == "__main__":
    debug_environment_loading()