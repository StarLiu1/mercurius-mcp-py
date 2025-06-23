import logging
import asyncio
import base64
from typing import Dict, List, Optional
import httpx
from lxml import etree
from config.settings import settings
from models.vsac_models import VSACValueSet, VSACConcept, VSACMetadata
from utils.error_handlers import VSACError, handle_vsac_error

logger = logging.getLogger(__name__)


class VSACService:
    def __init__(self):
        self.base_url = "https://vsac.nlm.nih.gov/vsac/svs/"
        self.cache: Dict[str, VSACValueSet] = {}
    
    def create_basic_auth(self, username: str, password: str) -> str:
        """Create basic authentication header."""
        if not username or not password:
            raise VSACError("VSAC username and password are required", "AUTH_REQUIRED")
        
        clean_username = username.strip()
        clean_password = password.strip()
        
        credentials = f"{clean_username}:{clean_password}"
        encoded = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
        
        logger.info(f"Creating Basic Auth for user: {clean_username}")
        return f"Basic {encoded}"
    
    def parse_purpose_field(self, purpose_text: Optional[str]) -> Dict[str, Optional[str]]:
        """Parse the Purpose field to extract clinical metadata."""
        if not purpose_text:
            return {
                "clinical_focus": None,
                "data_element_scope": None,
                "inclusion_criteria": None,
                "exclusion_criteria": None
            }
        
        import re
        metadata = {
            "clinical_focus": None,
            "data_element_scope": None,
            "inclusion_criteria": None,
            "exclusion_criteria": None
        }
        
        try:
            patterns = {
                "clinical_focus": r'\(Clinical Focus:\s*([^)]+)\)',
                "data_element_scope": r'\(Data Element Scope:\s*([^)]+)\)',
                "inclusion_criteria": r'\(Inclusion Criteria:\s*([^)]+)\)',
                "exclusion_criteria": r'\(Exclusion Criteria:\s*([^)]+)\)'
            }
            
            for key, pattern in patterns.items():
                match = re.search(pattern, purpose_text, re.IGNORECASE)
                if match:
                    metadata[key] = match.group(1).strip()
        
        except Exception as error:
            logger.error(f"Error parsing purpose field: {error}")
        
        return metadata
    
    def parse_vsac_response(self, response_xml: str) -> VSACValueSet:
        """Parse VSAC XML response into structured data."""
        try:
            logger.info("Parsing VSAC XML response...")
            
            # Remove namespaces for easier parsing
            response_xml = response_xml.replace('ns0:', '').replace('xmlns:ns0=', 'xmlns=')
            
            root = etree.fromstring(response_xml.encode('utf-8'))
            
            # Find value set
            value_set = root.find('.//DescribedValueSet') or root.find('.//ValueSet')
            
            if value_set is None:
                raise VSACError("No ValueSet found in response", "PARSE_ERROR")
            
            # Extract metadata
            metadata = VSACMetadata(
                id=value_set.get('ID'),
                display_name=value_set.get('displayName'),
                version=value_set.get('version')
            )
            
            # Extract additional metadata elements
            for elem_name in ['Source', 'Type', 'Binding', 'Status', 'RevisionDate', 'Purpose', 'Description']:
                elem = value_set.find(f'.//{elem_name}')
                if elem is not None and elem.text:
                    setattr(metadata, elem_name.lower(), elem.text)
            
            # Parse purpose field for clinical metadata
            if hasattr(metadata, 'purpose') and metadata.purpose:
                purpose_metadata = self.parse_purpose_field(metadata.purpose)
                for key, value in purpose_metadata.items():
                    setattr(metadata, key, value)
            
            # Extract concepts
            concepts = []
            concept_list = value_set.find('.//ConceptList')
            
            if concept_list is not None:
                for concept_elem in concept_list.findall('.//Concept'):
                    concept = VSACConcept(
                        code=concept_elem.get('code', ''),
                        code_system=concept_elem.get('codeSystem', ''),
                        code_system_name=concept_elem.get('codeSystemName', ''),
                        code_system_version=concept_elem.get('codeSystemVersion'),
                        display_name=concept_elem.get('displayName', '')
                    )
                    concepts.append(concept)
            
            logger.info(f"Successfully parsed {len(concepts)} concepts from VSAC response")
            
            return VSACValueSet(metadata=metadata, concepts=concepts)
        
        except Exception as error:
            logger.error(f"Error parsing VSAC XML response: {error}")
            raise VSACError(f"XML parsing failed: {error}", "PARSE_ERROR")
    
    async def retrieve_value_set(
        self, 
        value_set_identifier: str, 
        version: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None
    ) -> VSACValueSet:
        """Retrieve value set from VSAC with caching."""
        username = username or settings.vsac_username
        password = password or settings.vsac_password
        
        cache_key = f"{value_set_identifier}_{version or 'latest'}"
        
        # Check cache first
        if cache_key in self.cache:
            logger.info(f"Cache hit for value set: {value_set_identifier}")
            return self.cache[cache_key]
        
        logger.info(f"Fetching value set from VSAC: {value_set_identifier}")
        
        try:
            endpoint = self.base_url + "RetrieveMultipleValueSets"
            auth_header = self.create_basic_auth(username, password)
            
            params = {"id": value_set_identifier}
            if version:
                params["version"] = version
            
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    endpoint,
                    headers={
                        "Authorization": auth_header,
                        "Accept": "application/xml"
                    },
                    params=params,
                    timeout=30.0
                )
            
            logger.info(f"VSAC response status: {response.status_code}")
            
            if response.status_code != 200:
                handle_vsac_error(response, value_set_identifier)
            
            parsed_data = self.parse_vsac_response(response.text)
            
            # Cache the result
            self.cache[cache_key] = parsed_data
            
            return parsed_data
        
        except httpx.HTTPError as error:
            logger.error(f"HTTP error querying VSAC: {error}")
            handle_vsac_error(error, value_set_identifier)
        except Exception as error:
            logger.error(f"Error querying VSAC for ValueSet {value_set_identifier}: {error}")
            raise VSACError(f"VSAC query failed: {error}", "QUERY_ERROR")
    
    async def retrieve_multiple_value_sets(
        self,
        value_set_ids: List[str],
        username: Optional[str] = None,
        password: Optional[str] = None,
        concurrency: int = 3
    ) -> Dict[str, VSACValueSet]:
        """Retrieve multiple value sets efficiently with concurrency control."""
        results = {}
        
        # Process in batches to respect rate limits
        for i in range(0, len(value_set_ids), concurrency):
            batch = value_set_ids[i:i + concurrency]
            
            # Create tasks for current batch
            tasks = [
                self.retrieve_value_set(oid, username=username, password=password)
                for oid in batch
            ]
            
            # Execute batch
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for oid, result in zip(batch, batch_results):
                if isinstance(result, Exception):
                    logger.error(f"Failed to retrieve value set {oid}: {result}")
                    # Create error placeholder
                    results[oid] = VSACValueSet(
                        metadata=VSACMetadata(id=oid, display_name="Error", status="ERROR"),
                        concepts=[]
                    )
                else:
                    results[oid] = result
        
        return results
    
    def get_cache_stats(self) -> Dict[str, any]:
        """Get cache statistics."""
        return {
            "size": len(self.cache),
            "keys": list(self.cache.keys())
        }
    
    def clear_cache(self):
        """Clear cache."""
        self.cache.clear()
        logger.info("VSAC cache cleared")


# Singleton instance
vsac_service = VSACService()