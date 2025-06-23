#!/usr/bin/env python3
"""
Environment setup helper for OMOP MCP Server
"""

import os
import sys
from pathlib import Path

def find_project_root():
    """Find the project root directory."""
    current = Path(__file__).parent
    
    # Look for key project files
    project_markers = ['pyproject.toml', 'requirements.txt', 'src/main.py']
    
    for marker in project_markers:
        if (current / marker).exists():
            return current
    
    # If not found, use current directory
    return current


def check_env_file_status():
    """Check current .env file status."""
    project_root = find_project_root()
    env_file = project_root / ".env"
    
    print(f"🔍 Environment File Status:")
    print(f"   Project Root: {project_root}")
    print(f"   Env File Path: {env_file}")
    print(f"   Env File Exists: {env_file.exists()}")
    print(f"   Current Working Dir: {os.getcwd()}")
    
    if env_file.exists():
        print(f"   Env File Size: {env_file.stat().st_size} bytes")
        print(f"\n📄 Current .env file contents:")
        try:
            with open(env_file, 'r') as f:
                content = f.read()
                if content.strip():
                    # Mask sensitive values
                    lines = content.split('\n')
                    for line in lines:
                        if '=' in line and line.strip() and not line.strip().startswith('#'):
                            key, value = line.split('=', 1)
                            if any(sensitive in key.upper() for sensitive in ['PASSWORD', 'KEY', 'SECRET']):
                                print(f"      {key}=***MASKED***")
                            else:
                                print(f"      {line}")
                        else:
                            print(f"      {line}")
                else:
                    print("      (file is empty)")
        except Exception as e:
            print(f"      Error reading file: {e}")
    else:
        print("   ❌ .env file does not exist")
    
    return project_root, env_file


def create_env_template():
    """Create a template .env file."""
    
    template = """# OMOP-NLP-MCP Environment Configuration
# All environment variables use UPPERCASE with underscores (standard convention)
# Copy this template and fill in your actual values

# =================================
# LLM Provider Configuration
# =================================
# Choose one: openai, azure-openai, anthropic
LLM_PROVIDER=azure-openai

# OpenAI Configuration
# OPENAI_API_KEY=sk-your-openai-api-key-here
# OPENAI_MODEL=gpt-4-turbo

# Azure OpenAI Configuration
# AZURE_OPENAI_API_KEY=your-azure-openai-key-here
# AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
# AZURE_OPENAI_MODEL=gpt-4
# AZURE_OPENAI_API_VERSION=2024-02-15-preview

# Anthropic Configuration
# ANTHROPIC_API_KEY=your-anthropic-api-key-here
# ANTHROPIC_MODEL=claude-3-opus-20240229

# =================================
# VSAC (UMLS) Credentials
# =================================
# Get these from https://uts.nlm.nih.gov/uts/
# VSAC_USERNAME=your-umls-username
# VSAC_PASSWORD=your-umls-password

# =================================
# Database Configuration
# =================================
DATABASE_USER=dbadmin
DATABASE_ENDPOINT=52.167.131.85
DATABASE_NAME=tufts
# DATABASE_PASSWORD=your-database-password-here
OMOP_DATABASE_SCHEMA=dbo

# =================================
# Server Configuration (Optional)
# =================================
MCP_REQUEST_TIMEOUT=30000
MCP_TOOL_TIMEOUT=60000
"""
    
    return template


def interactive_setup():
    """Interactive setup for environment variables."""
    project_root, env_file = check_env_file_status()
    
    print(f"\n🚀 Interactive Environment Setup")
    
    # Check if .env already exists
    if env_file.exists():
        response = input(f"\n❓ .env file already exists. Overwrite? (y/N): ").lower().strip()
        if response != 'y':
            print("👋 Setup cancelled. Edit your .env file manually if needed.")
            return
    
    print(f"\n📝 Creating .env file at: {env_file}")
    
    # Get LLM provider
    print(f"\n🤖 LLM Provider Setup:")
    print("   1. OpenAI")
    print("   2. Azure OpenAI") 
    print("   3. Anthropic")
    provider_choice = input("Choose LLM provider (1-3) [2]: ").strip() or "2"
    
    providers = {"1": "openai", "2": "azure-openai", "3": "anthropic"}
    llm_provider = providers.get(provider_choice, "azure-openai")
    
    # Collect configuration
    config = {"LLM_PROVIDER": llm_provider}
    
    # LLM-specific config
    if llm_provider == "openai":
        config["OPENAI_API_KEY"] = input("OpenAI API Key: ").strip()
    elif llm_provider == "azure-openai":
        config["AZURE_OPENAI_API_KEY"] = input("Azure OpenAI API Key: ").strip()
        config["AZURE_OPENAI_ENDPOINT"] = input("Azure OpenAI Endpoint: ").strip()
        config["AZURE_OPENAI_MODEL"] = input("Azure OpenAI Model [gpt-4]: ").strip() or "gpt-4"
    elif llm_provider == "anthropic":
        config["ANTHROPIC_API_KEY"] = input("Anthropic API Key: ").strip()
    
    # VSAC credentials
    print(f"\n🏥 VSAC (UMLS) Credentials:")
    config["VSAC_USERNAME"] = input("UMLS Username: ").strip()
    config["VSAC_PASSWORD"] = input("UMLS Password: ").strip()
    
    # Database
    print(f"\n🗄️ Database Configuration:")
    config["DATABASE_PASSWORD"] = input("Database Password: ").strip()
    
    # Write .env file
    try:
        with open(env_file, 'w') as f:
            f.write("# OMOP-NLP-MCP Environment Configuration\n")
            f.write("# All environment variables use UPPERCASE convention\n")
            f.write("# Generated by setup script\n\n")
            
            for key, value in config.items():
                f.write(f"{key}={value}\n")
            
            # Add defaults
            f.write(f"\n# Database defaults\n")
            f.write(f"DATABASE_USER=dbadmin\n")
            f.write(f"DATABASE_ENDPOINT=52.167.131.85\n")
            f.write(f"DATABASE_NAME=tufts\n")
            f.write(f"OMOP_DATABASE_SCHEMA=dbo\n")
        
        print(f"\n✅ .env file created successfully!")
        print(f"📍 Location: {env_file}")
        
        # Test loading
        print(f"\n🧪 Testing environment loading...")
        try:
            # Add project root to path
            sys.path.insert(0, str(project_root / "src"))
            
            from config.settings import settings, debug_environment_loading
            debug_environment_loading()
            print(f"\n🎉 Environment variables loaded successfully!")
            
        except Exception as e:
            print(f"\n⚠️ Error testing environment loading: {e}")
            print(f"You may need to restart your server for changes to take effect.")
    
    except Exception as e:
        print(f"\n❌ Error creating .env file: {e}")


def main():
    """Main function."""
    print("🔧 OMOP MCP Server - Environment Setup Helper")
    print("=" * 50)
    
    project_root, env_file = check_env_file_status()
    
    if not env_file.exists():
        print(f"\n💡 Recommendations:")
        print(f"   1. Run interactive setup to create .env file")
        print(f"   2. Or manually create .env file with template")
        
        response = input(f"\n❓ Run interactive setup? (Y/n): ").lower().strip()
        if response != 'n':
            interactive_setup()
        else:
            print(f"\n📄 Template .env file:")
            print(create_env_template())
            print(f"\n💾 Save this as: {env_file}")
    else:
        print(f"\n💡 .env file exists. To test environment loading:")
        print(f"   python -c \"from src.config.settings import debug_environment_loading; debug_environment_loading()\"")


if __name__ == "__main__":
    main()