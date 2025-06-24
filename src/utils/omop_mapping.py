# src/utils/omop_mapping.py
"""
OMOP mapping utilities - matches JavaScript mapVsacToOmop.js functionality
"""

# Import all the functions from the fixed_omop_mapping_python artifact
from fixed_omop_mapping_python import (
    prepare_concepts_and_summary,
    summarise_vsac_fetch,
    map_concepts_to_omop_database,
    execute_verbatim_query_real,
    execute_standard_query_real,
    execute_mapped_query_real,
    group_concepts_by_value_set,
    generate_omop_mapping_summary,
    generate_verbatim_sql,
    generate_standard_sql,
    generate_mapped_sql,
    generate_mapping_summary,
    map_vsac_to_omop_tool,
    debug_vsac_omop_pipeline_tool
)