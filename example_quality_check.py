"""
Example script demonstrating how to use the quality_check function
with scraped car data from Carlist and Mudah/Carsome.
"""

import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from extract import get_carlist, get_carsome, quality_check

def main():
    """
    Example workflow:
    1. Extract data from both sources
    2. Run quality checks on the extracted data
    3. Display results
    """

    print("="*60)
    print("CAR SCRAPING PIPELINE - QUALITY CHECK EXAMPLE")
    print("="*60)
    print()

    # Step 1: Extract data from Carlist
    print("Step 1: Extracting data from Carlist...")
    carlist_data = get_carlist()
    print(f"Extracted {len(carlist_data) if carlist_data else 0} records from Carlist")
    print()

    # Step 2: Extract data from Mudah/Carsome
    print("Step 2: Extracting data from Mudah/Carsome...")
    carsome_data = get_carsome()
    carsome_count = len(carsome_data) if carsome_data is not None else 0
    print(f"Extracted {carsome_count} records from Mudah/Carsome")
    print()

    # Step 3: Run quality checks
    print("Step 3: Running quality checks on extracted data...")
    print()

    # You can optionally provide historical averages for anomaly detection
    # For example, if you normally get around 25 records from each source
    results = quality_check(
        carlist_data=carlist_data,
        carsome_data=carsome_data,
        historical_avg_carlist=25,  # Optional: set based on your historical data
        historical_avg_carsome=25   # Optional: set based on your historical data
    )

    # Step 4: Display detailed results
    print("\n" + "="*60)
    print("DETAILED QUALITY CHECK RESULTS")
    print("="*60)

    for source in ['carlist', 'carsome']:
        if source in results:
            print(f"\n{source.upper()} Results:")
            print("-" * 40)
            for check_name, check_result in results[source]['detailed_results'].items():
                status = "✓ PASS" if check_result['passed'] else "✗ FAIL"
                print(f"  {status} {check_name}: {check_result['details']}")

    # Overall summary
    if 'overall' in results:
        print("\n" + "="*60)
        print("OVERALL SUMMARY")
        print("="*60)
        print(f"Average Quality Score: {results['overall']['average_quality_score']:.1f}%")
        print(f"All Checks Passed: {results['overall']['all_checks_passed']}")
        print(f"Timestamp: {results['overall']['timestamp']}")
        print("="*60)

if __name__ == "__main__":
    main()
