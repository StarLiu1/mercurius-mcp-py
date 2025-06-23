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
    
    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()