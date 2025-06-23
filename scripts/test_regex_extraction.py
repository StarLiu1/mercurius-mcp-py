#!/usr/bin/env python3
"""Test script to verify regex-based ValueSet OID extraction."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from utils.extractors import extract_valueset_identifiers_from_cql, validate_extracted_oids


def run_tests():
    """Run comprehensive tests for ValueSet OID extraction."""
    print("🧪 Testing valueset declaration regex-based ValueSet OID extraction\n")
    print("📋 Pattern used: (valueset\\s\".+\":\\s')(urn:oid:)((\\d+\\.)*\\d+)(')")
    print("🎯 Extracts group 3: the OID part\n")
    
    # Test cases with valueset declaration pattern only
    test_cases = [
        {
            "name": "Single valueset declaration with single quotes",
            "cql": 'valueset "Diabetes": \'urn:oid:2.16.840.1.113883.3.464.1003.103.12.1001\'',
            "expected": ["2.16.840.1.113883.3.464.1003.103.12.1001"]
        },
        {
            "name": "Multiple valueset declarations",
            "cql": '''
                valueset "Diabetes": 'urn:oid:2.16.840.1.113883.3.464.1003.103.12.1001'
                valueset "Hypertension": 'urn:oid:2.16.840.1.113883.3.464.1003.104.12.1011'
                valueset "Medications": 'urn:oid:2.16.840.1.113883.3.464.1003.196.12.1001'
            ''',
            "expected": [
                "2.16.840.1.113883.3.464.1003.103.12.1001",
                "2.16.840.1.113883.3.464.1003.104.12.1011",
                "2.16.840.1.113883.3.464.1003.196.12.1001"
            ]
        },
        {
            "name": "valueset with double quotes (should NOT match)",
            "cql": 'valueset "Diabetes": "urn:oid:2.16.840.1.113883.3.464.1003.103.12.1001"',
            "expected": []  # Should not match - requires single quotes
        },
        {
            "name": "No valueset declarations",
            "cql": '''define "AgeInYears": AgeInYears()
                      define "Female": Patient.gender = 'female'
                      define "Test": [Condition: "urn:oid:2.16.840.1.113883.3.464.1003.103.12.1001"]''',
            "expected": []  # Should not match non-valueset references
        },
        {
            "name": "Real-world CQL with valueset declarations",
            "cql": '''
                library DiabetesScreening version '1.0.0'
                
                using FHIR version '4.0.1'
                
                include FHIRHelpers version '4.0.1' called FHIRHelpers
                
                valueset "Diabetes": 'urn:oid:2.16.840.1.113883.3.464.1003.103.12.1001'
                valueset "HbA1c Laboratory Test": 'urn:oid:2.16.840.1.113883.3.464.1003.198.12.1013'
                valueset "Office Visit": 'urn:oid:2.16.840.1.113883.3.464.1003.101.12.1001'
                
                parameter "Measurement Period" Interval<DateTime>
                
                context Patient
                
                define "Initial Population":
                    AgeInYearsAt(start of "Measurement Period") >= 18
            ''',
            "expected": [
                "2.16.840.1.113883.3.464.1003.103.12.1001",
                "2.16.840.1.113883.3.464.1003.198.12.1013",
                "2.16.840.1.113883.3.464.1003.101.12.1001"
            ]
        }
    ]
    
    passed = 0
    failed = 0
    
    for test_case in test_cases:
        print(f"📝 Test: {test_case['name']}")
        
        try:
            oids, valuesets = extract_valueset_identifiers_from_cql(test_case['cql'])
            
            # Sort arrays for comparison
            extracted_sorted = sorted(oids)
            expected_sorted = sorted(test_case['expected'])
            
            # Check if arrays are equal
            arrays_equal = (extracted_sorted == expected_sorted)
            
            if arrays_equal:
                print(f"  ✅ PASSED")
                print(f"  📊 Extracted: {len(oids)} OIDs")
                passed += 1
            else:
                print(f"  ❌ FAILED")
                print(f"  📊 Expected: {expected_sorted}")
                print(f"  📊 Got:      {extracted_sorted}")
                failed += 1
            
            # Show validation details
            valid_oids = validate_extracted_oids(oids)
            if len(valid_oids) != len(oids):
                invalid_oids = [oid for oid in oids if oid not in valid_oids]
                print(f"  🔍 Invalid OIDs found: {invalid_oids}")
            
        except Exception as error:
            print(f"  💥 ERROR: {error}")
            failed += 1
        
        print('')  # Empty line between tests
    
    # Summary
    print("📈 Test Summary:")
    print(f"  ✅ Passed: {passed}")
    print(f"  ❌ Failed: {failed}")
    print(f"  📊 Total:  {passed + failed}")
    
    if failed == 0:
        print("\n🎉 All tests passed! Regex extraction is working correctly.")
    else:
        print(f"\n⚠️  {failed} test(s) failed. Please review the regex patterns.")
    
    return {"passed": passed, "failed": failed}


def performance_test():
    """Performance test for regex extraction."""
    print("\n⚡ Performance Test: valueset declaration regex extraction")
    
    large_cql = '''
        library LargeExample version '1.0.0'
        
        valueset "VS1": 'urn:oid:2.16.840.1.113883.3.464.1003.103.12.1001'
        valueset "VS2": 'urn:oid:2.16.840.1.113883.3.464.1003.104.12.1002'
        valueset "VS3": 'urn:oid:2.16.840.1.113883.3.464.1003.105.12.1003'
        valueset "VS4": 'urn:oid:2.16.840.1.113883.3.464.1003.106.12.1004'
        valueset "VS5": 'urn:oid:2.16.840.1.113883.3.464.1003.107.12.1005'
    ''' * 10  # Repeat to make it larger
    
    iterations = 100
    
    print(f"  📏 CQL size: {len(large_cql)} characters")
    print(f"  🔄 Iterations: {iterations}")
    
    # Time regex extraction
    import time
    start_time = time.time()
    for i in range(iterations):
        extract_valueset_identifiers_from_cql(large_cql)
    end_time = time.time()
    
    total_time = (end_time - start_time) * 1000  # Convert to ms
    avg_time = total_time / iterations
    
    print(f"  ⏱️  Total time: {total_time:.2f}ms")
    print(f"  📊 Average per extraction: {avg_time:.2f}ms")
    print(f"  🚀 Estimated speedup vs LLM: ~100-1000x faster")


if __name__ == "__main__":
    results = run_tests()
    performance_test()
    
    # Exit with appropriate code
    sys.exit(1 if results["failed"] > 0 else 0)