from typing import Dict, Any


async def omop_schema_resource() -> Dict[str, Any]:
    """Get OMOP schema information."""
    return {
        "version": "6.0",
        "tables": ["person", "observation_period", "visit_occurrence", "condition_occurrence"],
        "status": "placeholder - full schema not loaded"
    }