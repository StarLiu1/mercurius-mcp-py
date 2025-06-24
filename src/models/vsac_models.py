from pydantic import BaseModel
from typing import List, Optional, Dict, Any


class VSACConcept(BaseModel):
    code: str
    code_system: str
    code_system_name: str
    code_system_version: Optional[str] = None
    display_name: str


class VSACMetadata(BaseModel):
    id: Optional[str] = None
    display_name: Optional[str] = None
    version: Optional[str] = None
    source: Optional[str] = None
    type: Optional[str] = None
    binding: Optional[str] = None
    status: Optional[str] = None
    revision_date: Optional[str] = None
    description: Optional[str] = None
    # Clinical metadata fields (from Purpose field parsing)
    clinical_focus: Optional[str] = None
    data_element_scope: Optional[str] = None
    inclusion_criteria: Optional[str] = None
    exclusion_criteria: Optional[str] = None


class VSACValueSet(BaseModel):
    metadata: VSACMetadata
    concepts: List[VSACConcept]


class ValueSetReference(BaseModel):
    name: str
    oid: str