from pydantic import BaseModel
from typing import List, Optional, Dict, Any


class OMOPConcept(BaseModel):
    concept_id: int
    concept_code: str
    vocabulary_id: str
    domain_id: str
    concept_class_id: str
    concept_name: str
    standard_concept: Optional[str] = None
    source_concept_id: Optional[int] = None
    relationship_id: Optional[str] = None
    mapping_type: str


class ConceptMapping(BaseModel):
    concept_set_id: str
    concept_set_name: str
    concept_code: str
    vocabulary_id: str
    original_vocabulary: str
    display_name: str
    code_system: Optional[str] = None


class MappingResults(BaseModel):
    verbatim: List[OMOPConcept] = []
    standard: List[OMOPConcept] = []
    mapped: List[OMOPConcept] = []


class MappingSummary(BaseModel):
    total_source_concepts: int
    total_mappings: int
    unique_target_concepts: int
    mapping_counts: Dict[str, int]
    mapping_percentages: Dict[str, str]