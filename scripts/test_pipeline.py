#!/usr/bin/env python3
"""
Test the complete CQL to SQL translation pipeline.
"""

import asyncio
import sys
from pathlib import Path

# Add src to path
# Add src to path - test_scripts and src are siblings
project_root = Path(__file__).parent.parent  # Go up to project root
sys.path.insert(0, str(project_root / "src"))

from tools.translate_cql_to_sql_complete import translate_cql_to_sql_complete_tool


# Simple test CQL
TEST_CQL = """
library TestMeasure version '1.0.0'

using QDM version '5.6'

valueset "Diabetes": 'urn:oid:2.16.840.1.113883.3.464.1003.103.12.1001'
valueset "Office Visit": 'urn:oid:2.16.840.1.113883.3.464.1003.101.12.1001'

context Patient

define "Initial Population":
  exists ["Encounter, Performed": "Office Visit"]

define "Denominator":
  "Initial Population"

define "Has Diabetes":
  exists ["Condition": "Diabetes"]

define "Numerator":
  "Denominator" and "Has Diabetes"
"""


async def test_pipeline():
    """Test the complete pipeline."""
    print("=" * 80)
    print("Testing Complete CQL to SQL Translation Pipeline")
    print("=" * 80)
    
    result = await translate_cql_to_sql_complete_tool(
        cql_content=TEST_CQL,
        sql_dialect="postgresql",
        validate=True,
        correct_errors=True
    )
    
    print("\n" + "=" * 80)
    print("RESULTS")
    print("=" * 80)
    
    if result.get('success'):
        print("✅ Pipeline succeeded!")
        print(f"\nFinal SQL ({len(result['final_sql'])} characters):")
        print("-" * 80)
        print(result['final_sql'][:500])  # Print first 500 chars
        print("...")
        print("-" * 80)
        
        # Print statistics
        stats = result.get('statistics', {})
        print("\n📊 Statistics:")
        for category, values in stats.items():
            print(f"\n  {category.upper()}:")
            if isinstance(values, dict):
                for key, value in values.items():
                    print(f"    - {key}: {value}")
            else:
                print(f"    {values}")
    else:
        print("❌ Pipeline failed!")
        print(f"\nErrors:")
        for error in result.get('errors', []):
            print(f"  - {error}")
    
    return result


if __name__ == "__main__":
    result = asyncio.run(test_pipeline())
    sys.exit(0 if result.get('success') else 1)