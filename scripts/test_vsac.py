#!/usr/bin/env python3
"""Test script for VSAC integration."""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from services.vsac_service import vsac_service
from config.settings import settings


async def test_vsac_integration():
    """Test VSAC integration with common value sets."""
    print("🧪 Testing VSAC Integration...\n")
    
    # Check environment variables
    if not settings.vsac_username or not settings.vsac_password:
        print("❌ Error: VSAC_USERNAME and VSAC_PASSWORD environment variables are required")
        print("Add them to your .env file:")
        print("VSAC_USERNAME=your_username")
        print("VSAC_PASSWORD=your_password")
        return
    
    # Test cases - common VSAC value sets
    test_value_sets = [
        {
            "name": "Essential Hypertension",
            "oid": "2.16.840.1.113883.3.464.1003.104.12.1011",
            "description": "Common hypertension value set"
        },
        {
            "name": "Diabetes",
            "oid": "2.16.840.1.113883.3.464.1003.103.12.1001",
            "description": "Diabetes mellitus value set"
        }
    ]
    
    for test_case in test_value_sets:
        print(f"📋 Testing: {test_case['name']} ({test_case['oid']})")
        print(f"   Description: {test_case['description']}")
        
        try:
            import time
            start_time = time.time()
            
            value_set = await vsac_service.retrieve_value_set(
                test_case["oid"],
                username=settings.vsac_username,
                password=settings.vsac_password
            )
            
            end_time = time.time()
            duration = int((end_time - start_time) * 1000)
            
            print(f"✅ Success! Retrieved {len(value_set.concepts)} concepts in {duration}ms")
            
            if value_set.concepts:
                sample = value_set.concepts[0]
                print(f"   Sample concept: {sample.code} - {sample.display_name} ({sample.code_system_name})")
                
                code_systems = set(c.code_system_name for c in value_set.concepts)
                print(f"   Code systems found: {', '.join(code_systems)}")
            else:
                print("   ⚠️  Warning: No concepts found for this value set")
            
        except Exception as error:
            print(f"❌ Error: {error}")
        
        print("")  # Empty line for readability
    
    # Test cache functionality
    print("🗄️  Testing cache functionality...")
    cache_stats = vsac_service.get_cache_stats()
    print(f"   Cache size: {cache_stats['size']} value sets")
    cached_keys = cache_stats['keys']
    print(f"   Cached OIDs: {', '.join(cached_keys) if cached_keys else 'None'}")
    
    # Test batch retrieval
    print("\n📦 Testing batch retrieval...")
    batch_oids = [test['oid'] for test in test_value_sets]
    
    try:
        start_time = time.time()
        batch_results = await vsac_service.retrieve_multiple_value_sets(
            batch_oids,
            settings.vsac_username,
            settings.vsac_password
        )
        end_time = time.time()
        duration = int((end_time - start_time) * 1000)
        
        print(f"✅ Batch retrieval completed in {duration}ms")
        
        for oid, value_set in batch_results.items():
            print(f"   {oid}: {len(value_set.concepts)} concepts")
    
    except Exception as error:
        print(f"❌ Batch retrieval error: {error}")
    
    print("\n🎉 VSAC integration test completed!")


if __name__ == "__main__":
    asyncio.run(test_vsac_integration())