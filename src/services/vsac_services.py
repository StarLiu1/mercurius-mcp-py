import logging
import asyncio
import base64
from typing import Dict, List, Optional
import httpx
from lxml import etree
from config.settings import settings
from models.vsac_models import VSACValueSet, VSACConcept, VSACMetadata
from utils.error_handlers import VSACError, handle_vsac_error
from datetime import datetime

logger = logging.getLogger(__name__)


class VSACService:
    def __init__(self):
        self.base_url = "https://vsac.nlm.nih.gov/vsac/svs/"
        self.cache: Dict[str, VSACValueSet] = {}
    
    def create_basic_auth(self, username: str, password: str) -> str:
        """Create basic authentication header."""
        if not username or not password:
            raise VSACError("VSAC username and password are required", "AUTH_REQUIRED")
        
        # Clean the credentials to remove any whitespace/newlines (like JavaScript version)
        clean_username = username.strip()
        clean_password = password.strip()
        
        credentials = f"{clean_username}:{clean_password}"
        encoded = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
        
        logger.info(f"Creating Basic Auth for user: {clean_username}")
        logger.debug(f"Encoded credentials length: {len(encoded)}")
        
        return f"Basic {encoded}"
    
    def parse_purpose_field(self, purpose_text: Optional[str]) -> Dict[str, Optional[str]]:
        """Parse the Purpose field to extract clinical metadata (matches JavaScript logic)."""
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
            # Use the same regex patterns as JavaScript
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
            
            logger.debug(f'Parsed purpose metadata: {metadata}')
        
        except Exception as error:
            logger.error(f"Error parsing purpose field: {error}")
        
        return metadata
    
    def parse_vsac_response(self, response_xml: str) -> VSACValueSet:
        """Parse VSAC XML response - handles exact VSAC XML format."""
        try:
            logger.info("Parsing VSAC XML response...")
            logger.debug(f"Response length: {len(response_xml)}")
            
            # Check if response looks like HTML (error page)
            if response_xml.strip().startswith('<!DOCTYPE html') or response_xml.strip().startswith('<html'):
                logger.error("Received HTML response instead of XML - likely an error page")
                raise VSACError("VSAC returned HTML instead of XML - authentication or service error", "HTML_ERROR_RESPONSE")
            
            # Parse XML
            try:
                root = etree.fromstring(response_xml.encode('utf-8'))
            except etree.XMLSyntaxError as e:
                logger.error(f"XML parsing error: {e}")
                logger.debug(f"Problematic XML: {response_xml[:1000]}")
                raise VSACError(f"Invalid XML response from VSAC: {e}", "XML_PARSE_ERROR")
            
            # Initialize result structure
            result = VSACMetadata()
            concepts = []
            
            # Handle the exact VSAC XML structure with proper namespace handling
            # Define the namespace map based on the XML structure
            namespaces = {
                'ns0': 'urn:ihe:iti:svs:2008',
                'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
            }
            
            # Look for RetrieveMultipleValueSetsResponse
            retrieve_response = None
            
            # If root itself is the response element, use it
            if 'RetrieveMultipleValueSetsResponse' in root.tag:
                retrieve_response = root
                logger.debug('Root element is RetrieveMultipleValueSetsResponse')
            else:
                # Look for the response element using proper namespace
                try:
                    retrieve_response = root.find('.//ns0:RetrieveMultipleValueSetsResponse', namespaces)
                    if retrieve_response is not None:
                        logger.debug('Found RetrieveMultipleValueSetsResponse with ns0 namespace')
                except Exception:
                    pass
                
                # Fallback to wildcard search
                if retrieve_response is None:
                    retrieve_response = root.find('.//{*}RetrieveMultipleValueSetsResponse')
                    if retrieve_response is not None:
                        logger.debug('Found RetrieveMultipleValueSetsResponse with wildcard namespace')
            
            if retrieve_response is None:
                logger.error("No RetrieveMultipleValueSetsResponse found in XML")
                logger.debug(f"Root tag: {root.tag}, Root children: {[child.tag for child in root]}")
                raise VSACError("Invalid VSAC response structure", "NO_RESPONSE_FOUND")
            
            # Look for DescribedValueSet elements using proper namespace
            value_sets = []
            
            try:
                value_sets = retrieve_response.findall('.//ns0:DescribedValueSet', namespaces)
                if value_sets:
                    logger.debug(f'Found {len(value_sets)} DescribedValueSet elements with ns0 namespace')
            except Exception:
                pass
            
            # Fallback to wildcard search
            if not value_sets:
                value_sets = retrieve_response.findall('.//{*}DescribedValueSet')
                if value_sets:
                    logger.debug(f'Found {len(value_sets)} DescribedValueSet elements with wildcard namespace')
            
            # Try ValueSet elements as final fallback
            if not value_sets:
                try:
                    value_sets = retrieve_response.findall('.//ns0:ValueSet', namespaces)
                    if value_sets:
                        logger.debug(f'Found {len(value_sets)} ValueSet elements with ns0 namespace')
                except Exception:
                    pass
                
                if not value_sets:
                    value_sets = retrieve_response.findall('.//{*}ValueSet')
                    if value_sets:
                        logger.debug(f'Found {len(value_sets)} ValueSet elements with wildcard namespace')
            
            if not value_sets:
                logger.warning("No DescribedValueSet or ValueSet elements found")
                return VSACValueSet(
                    metadata=result,
                    concepts=[VSACConcept(
                        code='NO_VALUESET',
                        code_system='N/A',
                        code_system_name='VSAC',
                        display_name='No ValueSet found in response'
                    )]
                )
            
            # Process the first (primary) value set
            value_set = value_sets[0]
            logger.debug(f'Processing ValueSet with tag: {value_set.tag}')
            
            # Extract metadata from value set attributes
            result.id = value_set.get('ID')
            result.display_name = value_set.get('displayName') 
            result.version = value_set.get('version')
            
            logger.debug(f'ValueSet attributes: ID={result.id}, displayName={result.display_name}, version={result.version}')
            
            # Extract metadata elements using proper namespace handling
            metadata_elements = ['Source', 'Type', 'Binding', 'Status', 'RevisionDate', 'Description', 'Purpose']
            purpose_text = None
            
            for elem_name in metadata_elements:
                elem_value = None
                
                # Try with ns0 namespace first
                try:
                    elem = value_set.find(f'.//ns0:{elem_name}', namespaces)
                    if elem is not None and elem.text:
                        elem_value = elem.text.strip()
                        logger.debug(f'Found {elem_name} with ns0 namespace: {elem_value[:100]}...' if len(elem_value) > 100 else f'Found {elem_name}: {elem_value}')
                except Exception:
                    pass
                
                # Fallback to wildcard namespace
                if elem_value is None:
                    elem = value_set.find(f'.//{{{""}}}{elem_name}')
                    if elem is not None and elem.text:
                        elem_value = elem.text.strip()
                        logger.debug(f'Found {elem_name} with wildcard: {elem_value[:100]}...' if len(elem_value) > 100 else f'Found {elem_name}: {elem_value}')
                
                # Final fallback - no namespace
                if elem_value is None:
                    elem = value_set.find(f'.//{elem_name}')
                    if elem is not None and elem.text:
                        elem_value = elem.text.strip()
                        logger.debug(f'Found {elem_name} without namespace: {elem_value[:100]}...' if len(elem_value) > 100 else f'Found {elem_name}: {elem_value}')
                
                # Set the appropriate attribute
                if elem_value:
                    if elem_name == 'Source':
                        result.source = elem_value
                    elif elem_name == 'Type':
                        result.type = elem_value
                    elif elem_name == 'Binding':
                        result.binding = elem_value
                    elif elem_name == 'Status':
                        result.status = elem_value
                    elif elem_name == 'RevisionDate':
                        result.revision_date = elem_value
                    elif elem_name == 'Description':
                        result.description = elem_value
                    elif elem_name == 'Purpose':
                        purpose_text = elem_value
            
            # Parse purpose field for clinical metadata
            if purpose_text:
                logger.debug(f'Parsing purpose field: {purpose_text}')
                purpose_metadata = self.parse_purpose_field(purpose_text)
                result.clinical_focus = purpose_metadata.get("clinical_focus")
                result.data_element_scope = purpose_metadata.get("data_element_scope") 
                result.inclusion_criteria = purpose_metadata.get("inclusion_criteria")
                result.exclusion_criteria = purpose_metadata.get("exclusion_criteria")
            
            # Handle concept list using proper namespace handling
            concept_list = None
            
            # Try with ns0 namespace first
            try:
                concept_list = value_set.find('.//ns0:ConceptList', namespaces)
                if concept_list is not None:
                    logger.debug('Found ConceptList with ns0 namespace')
            except Exception:
                pass
            
            # Fallback to wildcard namespace
            if concept_list is None:
                concept_list = value_set.find('.//{*}ConceptList')
                if concept_list is not None:
                    logger.debug('Found ConceptList with wildcard namespace')
            
            # Final fallback - no namespace
            if concept_list is None:
                concept_list = value_set.find('.//ConceptList')
                if concept_list is not None:
                    logger.debug('Found ConceptList without namespace')
            
            if concept_list is None:
                logger.warning('No ConceptList found in ValueSet')
                return VSACValueSet(
                    metadata=result,
                    concepts=[VSACConcept(
                        code='EMPTY_VALUESET',
                        code_system='N/A',
                        code_system_name='VSAC',
                        display_name='ValueSet exists but contains no concepts (may be retired)'
                    )]
                )
            
            # Extract concepts from the concept list using proper namespace handling
            vsac_concepts = []
            
            # Try with ns0 namespace first
            try:
                vsac_concepts = concept_list.findall('.//ns0:Concept', namespaces)
                if vsac_concepts:
                    logger.debug(f'Found {len(vsac_concepts)} Concept elements with ns0 namespace')
            except Exception:
                pass
            
            # Fallback to wildcard namespace
            if not vsac_concepts:
                vsac_concepts = concept_list.findall('.//{*}Concept')
                if vsac_concepts:
                    logger.debug(f'Found {len(vsac_concepts)} Concept elements with wildcard namespace')
            
            # Final fallback - no namespace
            if not vsac_concepts:
                vsac_concepts = concept_list.findall('.//Concept')
                if vsac_concepts:
                    logger.debug(f'Found {len(vsac_concepts)} Concept elements without namespace')
            
            logger.debug(f"Found {len(vsac_concepts)} concepts in ConceptList")
            
            for concept_elem in vsac_concepts:
                # Extract concept attributes exactly as they appear in VSAC XML
                code = concept_elem.get('code')
                code_system = concept_elem.get('codeSystem')
                code_system_name = concept_elem.get('codeSystemName')
                code_system_version = concept_elem.get('codeSystemVersion')
                display_name = concept_elem.get('displayName')
                
                logger.debug(f'Processing concept: code={code}, codeSystemName={code_system_name}, displayName={display_name}')
                
                if code and code_system and code_system_name and display_name:
                    concepts.append(VSACConcept(
                        code=code,
                        code_system=code_system,
                        code_system_name=code_system_name,
                        code_system_version=code_system_version,
                        display_name=display_name
                    ))
                else:
                    logger.warning(f'Incomplete concept data: code={code}, codeSystem={code_system}, codeSystemName={code_system_name}, displayName={display_name}')
            
            logger.info(f"Successfully parsed {len(concepts)} concepts from VSAC response")
            
            return VSACValueSet(metadata=result, concepts=concepts)
            
        except VSACError:
            # Re-raise VSAC errors as-is
            raise
        except Exception as error:
            logger.error(f"Unexpected error parsing VSAC XML response: {error}")
            logger.debug(f"Raw response: {response_xml[:1000]}")
            
            # Return diagnostic entry instead of empty result
            error_metadata = VSACMetadata(
                id=None,
                display_name='Parse Error',
                version=None,
                status='ERROR',
                description=f'XML parsing failed: {str(error)}'
            )
            
            return VSACValueSet(
                metadata=error_metadata,
                concepts=[VSACConcept(
                    code='PARSE_ERROR',
                    code_system='N/A',
                    code_system_name='VSAC_PARSER',
                    display_name=f'XML parsing failed: {str(error)}'
                )]
            )
    
    async def retrieve_value_set(
        self, 
        value_set_identifier: str, 
        version: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None
    ) -> VSACValueSet:
        """Retrieve value set from VSAC - matches JavaScript functionality."""
        username = username or settings.vsac_username
        password = password or settings.vsac_password
        
        cache_key = f"{value_set_identifier}_{version or 'latest'}"
        
        # Check cache first (like JavaScript)
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
            
            headers = {
                "Authorization": auth_header,
                "Accept": "application/xml",
                "User-Agent": "OMOP-NLP-MCP/1.0"  # Like JavaScript
            }
            
            logger.debug(f"Making request to: {endpoint}")
            logger.debug(f"Parameters: {params}")
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(
                    endpoint,
                    headers=headers,
                    params=params
                )
            
            logger.info(f"VSAC response status: {response.status_code}")
            
            if response.status_code != 200:
                logger.error(f"VSAC API error: {response.status_code}")
                logger.debug(f"Response content: {response.text[:1000]}")
                handle_vsac_error(response, value_set_identifier)
            
            response_text = response.text
            logger.debug(f"Response length: {len(response_text)} characters")
            
            parsed_data = self.parse_vsac_response(response_text)
            
            # Cache the result (like JavaScript)
            self.cache[cache_key] = parsed_data
            
            return parsed_data
        
        except httpx.HTTPError as error:
            logger.error(f"HTTP error querying VSAC: {error}")
            raise VSACError(f"Network error connecting to VSAC: {error}", "NETWORK_ERROR")
        except VSACError:
            # Re-raise VSAC errors as-is
            raise
        except Exception as error:
            logger.error(f"Unexpected error querying VSAC for ValueSet {value_set_identifier}: {error}")
            raise VSACError(f"VSAC query failed: {error}", "QUERY_ERROR")
    
    async def retrieve_multiple_value_sets(
        self,
        value_set_ids: List[str],
        username: Optional[str] = None,
        password: Optional[str] = None,
        concurrency: int = 3  # Match JavaScript default
    ) -> Dict[str, VSACValueSet]:
        """Retrieve multiple value sets - matches JavaScript logic exactly."""
        results = {}
        
        logger.info(f"Retrieving {len(value_set_ids)} value sets with concurrency limit of {concurrency}")
        
        # Normalize function (like JavaScript version)
        def normalize(raw, oid):
            # Handle FHIR expansion.contains
            if hasattr(raw, 'expansion') and hasattr(raw.expansion, 'contains'):
                raw.concepts = raw.expansion.contains
            
            # VSAC sometimes nests concepts under ConceptList/concept
            if not hasattr(raw, 'concepts') and hasattr(raw, 'ConceptList') and hasattr(raw.ConceptList, 'Concept'):
                raw.concepts = raw.ConceptList.Concept
            
            # Single-concept collapse: wrap object → array
            if not isinstance(getattr(raw, 'concepts', []), list):
                raw.concepts = [raw.concepts] if hasattr(raw, 'concepts') and raw.concepts else []
            
            # Minimal metadata sanity
            if not hasattr(raw, 'metadata'):
                raw.metadata = VSACMetadata()
            if not raw.metadata.id:
                raw.metadata.id = oid
            
            return raw
        
        def make_error_shell(oid, err):
            error_metadata = VSACMetadata(
                id=oid, 
                display_name='Error', 
                status='ERROR'
            )
            return VSACValueSet(
                metadata=error_metadata,
                concepts=[]
            )
        
        # Process in batches (like JavaScript)
        for i in range(0, len(value_set_ids), concurrency):
            batch = value_set_ids[i:i + concurrency]
            logger.debug(f"Processing batch {i//concurrency + 1}: {batch}")
            
            # Create tasks for current batch
            async def fetch_single(oid):
                try:
                    raw = await self.retrieve_value_set(oid, None, username, password)
                    return {"oid": oid, "valueSetData": normalize(raw, oid)}
                except Exception as err:
                    logger.error(f"Failed to retrieve value set {oid}: {err}")
                    return {"oid": oid, "valueSetData": make_error_shell(oid, err)}
            
            tasks = [fetch_single(oid) for oid in batch]
            
            # Wait for the current batch before starting the next (like JavaScript)
            batch_results = await asyncio.gather(*tasks)
            
            for result in batch_results:
                results[result["oid"]] = result["valueSetData"]
        
        logger.info(f"Batch retrieval completed for {len(value_set_ids)} value sets")
        return results
    
    def get_cache_stats(self) -> Dict[str, any]:
        """Get cache statistics (matches JavaScript)."""
        return {
            "size": len(self.cache),
            "keys": list(self.cache.keys())
        }
    
    def clear_cache(self):
        """Clear cache (matches JavaScript)."""
        self.cache.clear()
        logger.info("VSAC cache cleared")


# Singleton instance
vsac_service = VSACService()