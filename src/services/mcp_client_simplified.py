"""
Simplified MCP Client that leverages the map-vsac-to-omop tool
"""

import os
import json
import logging
import requests
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class SimplifiedMCPClient:
    """Simplified MCP client that uses the map-vsac-to-omop tool."""
    
    def __init__(self, server_url: Optional[str] = None, db_config: Optional[Dict[str, Any]] = None, 
                 vsac_username: Optional[str] = None, vsac_password: Optional[str] = None,
                 timeout: Optional[int] = None):
        """Initialize MCP client.
        
        Args:
            server_url: MCP server URL
            db_config: Database configuration
            vsac_username: VSAC username
            vsac_password: VSAC password
            timeout: Timeout in seconds for MCP server calls
        """
        self.server_url = server_url or os.getenv('MCP_SERVER_URL', 'http://localhost:3000')
        self.db_config = db_config or {}
        self.session = None
        self.vsac_username = vsac_username or os.getenv('VSAC_USERNAME', '')
        self.vsac_password = vsac_password or os.getenv('VSAC_PASSWORD', '')
        self.timeout = timeout or 120  # Default 120 seconds if not provided
        
        logger.info(f"Initialized SimplifiedMCPClient with server: {self.server_url}")
        
    def extract_and_map_valuesets(self, cql_content: str) -> Dict[str, Any]:
        """
        Use MCP's map-vsac-to-omop tool to extract and map valuesets.
        
        This single call:
        1. Extracts OIDs from CQL
        2. Fetches concepts from VSAC
        3. Maps to OMOP concepts
        
        Args:
            cql_content: CQL content to process
            
        Returns:
            Dict with extracted valuesets and OMOP mappings
        """
        if not self.vsac_username or not self.vsac_password:
            raise ValueError("VSAC credentials are required. No fallbacks.")
        
        if not self.db_config:
            logger.error("Database configuration missing")
            return {}
            
        # Initialize session if needed
        if not self.session:
            self._initialize_session()
        
        # Prepare request for map-vsac-to-omop tool
        request_data = {
            "cqlQuery": cql_content,  # Changed from cqlContent to cqlQuery
            "vsacUsername": self.vsac_username,
            "vsacPassword": self.vsac_password,
            "databaseEndpoint": self.db_config.get('host', 'localhost'),
            "databasePort": str(self.db_config.get('port', 5432)),
            "databaseName": self.db_config.get('database', ''),
            "databaseSchema": self.db_config.get('schema', 'public'),
            "databaseUser": self.db_config.get('user', ''),
            "databasePassword": self.db_config.get('password', ''),
            "includeVerbatim": False,
            "includeStandard": False,
            "includeMapped": True
        }
        
        try:
            # Call MCP tool
            response = self._call_tool("map-vsac-to-omop", request_data)
            
            # Check if we have a successful response
            if response and 'result' in response:
                # Parse the nested JSON response from MCP
                result_content = response['result'].get('content', [])
                if result_content and result_content[0].get('type') == 'text':
                    # Parse the JSON string in the text field
                    import json
                    mcp_data = json.loads(result_content[0]['text'])
                    logger.info(f"MCP extraction successful: {mcp_data.get('summary', {}).get('total_valuesets_extracted', 0)} valuesets")
                    return self._process_mcp_response(mcp_data)
                else:
                    logger.error(f"Unexpected MCP response format: {response}")
                    raise RuntimeError(f"Unexpected MCP response format")
            elif response and 'error' in response:
                logger.error(f"MCP extraction failed: {response['error']}")
                raise RuntimeError(f"MCP extraction failed: {response['error']}")
            else:
                logger.error(f"MCP extraction failed: {response}")
                raise RuntimeError(f"MCP extraction failed: {response}")
                
        except Exception as e:
            logger.error(f"MCP extraction error: {e}")
            raise
    
    def _initialize_session(self):
        """Initialize MCP session."""
        try:
            # MCP server needs initialization
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream"
            }
            
            # Send initialize request
            init_request = {
                "jsonrpc": "2.0",
                "method": "initialize",
                "params": {
                    "protocolVersion": "0.1.0",
                    "capabilities": {
                        "tools": {"call": {}}
                    },
                    "clientInfo": {
                        "name": "cql-sql-pipeline",
                        "version": "1.0.0"
                    }
                },
                "id": 1
            }
            
            response = requests.post(
                f"{self.server_url}/mcp",
                json=init_request,
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                # Store session ID if provided
                if 'mcp-session-id' in response.headers:
                    self.session = response.headers['mcp-session-id']
                logger.info(f"MCP session initialized: {self.session}")
                
                # Send initialized notification
                initialized_request = {
                    "jsonrpc": "2.0",
                    "method": "notifications/initialized",
                    "params": {}
                }
                
                headers_with_session = headers.copy()
                if self.session:
                    headers_with_session["mcp-session-id"] = self.session
                    
                requests.post(
                    f"{self.server_url}/mcp",
                    json=initialized_request,
                    headers=headers_with_session,
                    timeout=5
                )
                logger.info("MCP client ready")
            else:
                logger.error(f"Failed to initialize MCP session: {response.status_code}")
        except Exception as e:
            logger.error(f"MCP session initialization error: {e}")
    
    def _call_tool(self, tool_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Call an MCP tool.
        
        Args:
            tool_name: Name of the tool to call
            params: Parameters for the tool
            
        Returns:
            Tool response
        """
        try:
            # MCP server expects requests at /mcp endpoint
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream"  # Required by MCP server
            }
            if self.session:
                headers["mcp-session-id"] = self.session
            
            # Format request for MCP protocol
            mcp_request = {
                "jsonrpc": "2.0",
                "method": "tools/call",
                "params": {
                    "name": tool_name,
                    "arguments": params
                },
                "id": 1
            }
            
            response = requests.post(
                f"{self.server_url}/mcp",
                json=mcp_request,
                headers=headers,
                timeout=self.timeout
            )
            
            logger.info(f"MCP tool response status: {response.status_code}")
            logger.info(f"MCP tool response headers: {response.headers}")
            
            if response.status_code == 200:
                # Store session ID if provided by server
                if 'mcp-session-id' in response.headers:
                    self.session = response.headers['mcp-session-id']
                
                # Check if response is SSE stream
                if 'text/event-stream' in response.headers.get('content-type', ''):
                    logger.info("Received SSE stream response")
                    # Parse SSE stream
                    lines = response.text.strip().split('\n')
                    for line in lines:
                        if line.startswith('data: '):
                            data = line[6:]  # Remove "data: " prefix
                            if data:
                                try:
                                    result = json.loads(data)
                                    logger.info(f"Parsed SSE result: {result}")
                                    return result
                                except json.JSONDecodeError:
                                    logger.warning(f"Could not parse SSE data: {data}")
                    return {"success": False, "error": "No valid data in SSE stream"}
                else:
                    # Regular JSON response
                    try:
                        result = response.json()
                        return result
                    except json.JSONDecodeError:
                        logger.error(f"Could not parse JSON response: {response.text}")
                        return {"success": False, "error": "Invalid JSON response"}
            else:
                logger.error(f"MCP tool call failed: {response.status_code} - {response.text}")
                return {"success": False, "error": f"HTTP {response.status_code}"}
                
        except Exception as e:
            logger.error(f"MCP tool call error: {e}")
            return {"success": False, "error": str(e)}
    
    def _process_mcp_response(self, mcp_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process MCP response into a standard format.
        
        Args:
            mcp_data: Parsed MCP response data
            
        Returns:
            Processed valueset and individual code mappings
        """
        processed = {
            "valuesets": {},
            "placeholders": {},
            "omop_concepts": {},
            "individual_codes": {}  # Add individual codes section
        }
        
        # Extract valuesets and individual codes from the pipeline data
        pipeline = mcp_data.get('pipeline', {})
        step1 = pipeline.get('step1_extraction', {})
        step3 = pipeline.get('step3_omop_mapping', {})
        
        # Get OMOP concepts (use mapped concepts for better coverage - includes Maps To relationships)
        # Mapped concepts provide 153.6% coverage vs only 8.2% for standard concepts
        mapped_concepts = step3.get('mapped', [])
        
        # Group concepts by valueset OID
        for vs_info in step1.get('valuesets', []):
            oid = vs_info['oid']
            name = vs_info['name']
            
            # Get all concept IDs for this valueset
            concept_ids = []
            for concept in mapped_concepts:
                if concept.get('concept_set_id') == oid:
                    concept_ids.append(str(concept['concept_id']))
            
            # Store valueset data
            processed["valuesets"][oid] = {
                "name": name,
                "oid": oid,
                "omop_concept_ids": concept_ids,
                "concept_count": len(concept_ids)
            }

            # Primary: OID-based placeholder (always works)
            placeholder_oid = f"PLACEHOLDER_{oid.replace('.', '_')}"
            processed["placeholders"][placeholder_oid] = concept_ids

            # Keep name-based for backward compatibility but log warning
            placeholder_name = f"PLACEHOLDER_{name.upper().replace(' ', '_').replace('-', '_')}"
            if placeholder_name != placeholder_oid:
                logger.warning(f"Name-based placeholder {placeholder_name} may not match. Use {placeholder_oid} for reliability")
            processed["placeholders"][placeholder_name] = concept_ids
            
            # Store detailed OMOP concepts
            processed["omop_concepts"][oid] = mapped_concepts
        
        # Process individual codes if present
        individual_codes = step1.get('codes', [])
        for code_info in individual_codes:
            code = code_info.get('code', '')
            name = code_info.get('name', '')
            system = code_info.get('system', '')
            
            if code and system:
                # Find matching OMOP concepts for this individual code
                # Individual codes use placeholder as concept_set_id
                clean_code = code.replace('-', '_').replace('.', '_')
                placeholder_key = f"PLACEHOLDER_{system.upper()}_{clean_code}"
                
                # Get concept IDs for this individual code
                concept_ids = []
                for concept in mapped_concepts:
                    # Check if this concept matches the individual code placeholder
                    if concept.get('concept_set_id') == placeholder_key:
                        concept_ids.append(str(concept['concept_id']))
                
                # Store individual code data
                code_key = f"{system}_{code}"
                processed["individual_codes"][code_key] = {
                    "name": name,
                    "code": code,
                    "system": system,
                    "omop_concept_ids": concept_ids,
                    "placeholder": placeholder_key
                }
                
                # Add to placeholder mappings
                if concept_ids:
                    processed["placeholders"][placeholder_key] = concept_ids
                    logger.info(f"Mapped individual {system} code {code} to OMOP concepts: {concept_ids}")
        
        logger.info(f"Processed {len(processed['valuesets'])} valuesets with {sum(len(v['omop_concept_ids']) for v in processed['valuesets'].values())} total concepts")
        logger.info(f"Processed {len(processed['individual_codes'])} individual codes")
        
        return processed