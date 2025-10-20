"""
Script to demonstrate the quality_check return structure
"""
import sys
import os
import json

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from quality_check import example_usage

if __name__ == "__main__":
    # Run the example
    results = example_usage()

    # Pretty print the entire results dictionary
    print(json.dumps(results, indent=2, default=str))
