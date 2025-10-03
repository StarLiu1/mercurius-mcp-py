import os
import yaml
from pathlib import Path
from typing import Dict, Any

def get_project_root() -> Path:
    """Get absolute path to project root."""
    # This file is in src/utils/, go up two levels
    return Path(__file__).parent.parent.parent

def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """Load config.yaml with absolute path resolution."""
    if not os.path.isabs(config_path):
        project_root = get_project_root()
        config_path = project_root / config_path
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Expand environment variables
    def expand_env_vars(obj):
        if isinstance(obj, dict):
            return {k: expand_env_vars(v) for k, v in obj.items()}
        elif isinstance(obj, str) and obj.startswith('${') and obj.endswith('}'):
            env_var = obj[2:-1]
            return os.getenv(env_var, obj)
        return obj
    
    return expand_env_vars(config)