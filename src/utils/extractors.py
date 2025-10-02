import re
import logging
from typing import List, Dict, Tuple, Any
from models.vsac_models import ValueSetReference

logger = logging.getLogger(__name__)

def extract_valueset_identifiers_from_cql(cql_query: str) -> Tuple[List[str], List[ValueSetReference]]:
    """
    Extract ValueSet OID identifiers from CQL query using valueset declaration pattern.
    
    Returns:
        Tuple of (oids, valuesets) where oids is a list of OID strings 
        and valuesets is a list of ValueSetReference objects with name/oid pairs
    """
    try:
        logger.info("Extracting ValueSet OIDs using valueset declaration pattern...")
        
        if not cql_query or not isinstance(cql_query, str):
            logger.error(f"Invalid CQL query input: {type(cql_query)}")
            return [], []
        
        oids = set()  # Use set to avoid duplicates
        valuesets = []  # Array to store name/oid pairs
        
        # Pattern: (valueset\s")(.+)(":\s')(urn:oid:)((\d+\.)*\d+)(')
        # Group 2: name, Group 5: OID
        pattern = r'(valueset\s")(.+)(":\s\')(urn:oid:)((\d+\.)*\d+)(\')' 
        
        matches = re.finditer(pattern, cql_query, re.IGNORECASE)
        
        for match in matches:
            name = match.group(2)  # Extract group 2 - the name
            oid = match.group(5)   # Extract group 5 - the OID part
            
            if oid and isinstance(oid, str) and name and isinstance(name, str):
                oids.add(oid)
                valuesets.append(ValueSetReference(
                    name=name.strip(),
                    oid=oid.strip()
                ))
                logger.info(f'Found valueset declaration: "{name}" -> {oid}')
        
        oid_list = list(oids)
        logger.info(f"Total unique OIDs extracted: {len(oid_list)}")
        logger.info(f"Total valuesets with names: {len(valuesets)}")
        
        return oid_list, valuesets
        
    except Exception as error:
        logger.error(f"Error extracting ValueSet OIDs: {error}")
        return [], []


def validate_extracted_oids(oids: List[str]) -> List[str]:
    """
    Validate that extracted OIDs follow proper format.
    
    Args:
        oids: Array of OID strings
        
    Returns:
        Array of valid OIDs
    """
    if not oids or not isinstance(oids, list):
        logger.error(f"validateExtractedOids: Invalid input, expected array but got: {type(oids)}")
        return []
    
    valid_oid_pattern = re.compile(r'^\d+(?:\.\d+)+$')
    
    valid_oids = []
    for oid in oids:
        if not isinstance(oid, str):
            logger.error(f"validateExtractedOids: Non-string OID found: {oid}")
            continue
        if valid_oid_pattern.match(oid):
            valid_oids.append(oid)
    
    return valid_oids


def map_vsac_to_omop_vocabulary(vsac_code_system_name: str) -> str:
    """
    Map VSAC code system names to OMOP vocabulary_id values.
    
    Args:
        vsac_code_system_name: Code system name from VSAC
        
    Returns:
        OMOP vocabulary_id
    """
    mappings = {
        'ICD10CM': 'ICD10CM',
        'ICD-10-CM': 'ICD10CM',
        'SNOMEDCT_US': 'SNOMED',
        'SNOMEDCT': 'SNOMED',
        'SNOMED CT US Edition': 'SNOMED',
        'CPT': 'CPT4',
        'HCPCS': 'HCPCS',
        'LOINC': 'LOINC',
        'RxNorm': 'RxNorm',
        'ICD9CM': 'ICD9CM',
        'ICD-9-CM': 'ICD9CM',
        'NDC': 'NDC',
        'RXNORM': 'RxNorm'
    }
    
    return mappings.get(vsac_code_system_name, vsac_code_system_name)

def extract_individual_codes_from_cql(cql_query: str) -> Dict[str, Any]:
    """
    Extract individual LOINC/SNOMED codes from CQL query.
    
    Returns:
        Dict with codes array and count
    """
    codes = []
    
    # Pattern for code declarations like: code "8462-4": '8462-4' from "LOINC"
    code_pattern = r'code\s+"([^"]+)":\s+\'([^\']+)\'\s+from\s+"([^"]+)"'
    
    matches = re.finditer(code_pattern, cql_query, re.IGNORECASE)
    
    for match in matches:
        name = match.group(1)
        code = match.group(2)
        system = match.group(3)
        
        codes.append({
            "name": name,
            "code": code,
            "system": system
        })
        logger.info(f'Found individual code: {name} ({code}) from {system}')
    
    return {
        "codes": codes,
        "count": len(codes)
    }
