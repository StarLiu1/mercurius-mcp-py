#!/usr/bin/env python3
"""
Process-based launcher that avoids event loop conflicts.
"""

import os
import sys
import subprocess
from pathlib import Path

def start_mcp_server():
    """Start the MCP server in a clean subprocess."""
    
    # Get project paths
    project_root = Path(__file__).parent
    main_script = project_root / "src" / "main.py"
    venv_path = project_root / ".venv"
    
    # Find Python executable
    if os.name == 'nt':  # Windows
        python_exe = venv_path / "Scripts" / "python.exe"
    else:  # macOS/Linux
        python_exe = venv_path / "bin" / "python"
    
    # Fall back to system python if venv not found
    if not python_exe.exists():
        python_exe = sys.executable
        print(f"Virtual environment not found, using system Python: {python_exe}")
    else:
        print(f"Using virtual environment Python: {python_exe}")
    
    # Set up environment
    env = os.environ.copy()
    
    # Add src to PYTHONPATH
    src_path = str(project_root / "src")
    if "PYTHONPATH" in env:
        env["PYTHONPATH"] = f"{src_path}{os.pathsep}{env['PYTHONPATH']}"
    else:
        env["PYTHONPATH"] = src_path
    
    print(f"Starting OMOP MCP Server...")
    print(f"Working directory: {project_root}")
    print(f"Script: {main_script}")
    print("=" * 60)
    
    try:
        # Create a completely isolated subprocess
        process = subprocess.Popen(
            [str(python_exe), str(main_script)],
            cwd=str(project_root),
            env=env,
            stdout=sys.stdout,
            stderr=sys.stderr,
            stdin=sys.stdin
        )
        
        # Wait for the process to complete
        return_code = process.wait()
        
        if return_code == 0:
            print("\nServer exited normally")
        else:
            print(f"\nServer exited with code: {return_code}")
        
        return return_code
        
    except KeyboardInterrupt:
        print("\nInterrupt received, stopping server...")
        try:
            process.terminate()
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
        return 0
        
    except Exception as error:
        print(f"Error starting server: {error}")
        return 1

if __name__ == "__main__":
    sys.exit(start_mcp_server())