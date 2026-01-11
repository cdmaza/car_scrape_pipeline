import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union
import requests
import logging
from dataclasses import dataclass

@dataclass
class QualityCheckResult:
    check_name: str
    passed: bool
    details: str
    value: Any = None
    threshold: Any = None   

class DataQualityChecker:

    def __init__(self):
        self.results = []
        self.logger = logging.getLogger(__name__)
    
    def quality_check(self, 
                     data: pd.DataFrame,
                     schema: Dict[str, type] = None,
                     data_source_url: str = None,
                     historical_avg_records: int = None,
                     record_count_tolerance: float = 0.2,
                     timestamp_column: str = None, 
                     freshness_threshold_hours: int = 24,
                     required_fields: List[str] = None,
                     unique_fields: List[str] = None,
                     id_fields: List[str] = None) -> Dict[str, Any]:
        
        self.results = []
        
        self._check_schema_conformity(data, schema)
        self._check_data_availability(data_source_url)
        self._check_record_count_anomaly(data, historical_avg_records, record_count_tolerance)
        self._check_timestamp_freshness(data, timestamp_column, freshness_threshold_hours)
        self._check_missing_values(data, required_fields)
        self._check_duplicate_records(data, unique_fields)
        self._check_source_identifiers(data, id_fields)
        
        # Compile final results
        return self._compile_results()
    
    def _check_schema_conformity(self, data: pd.DataFrame, schema: Dict[str, type]):
        """Check if fields/types match expected schema"""
        if not schema:
            self.results.append(QualityCheckResult(
                "schema_conformity", True, "No schema provided - skipping check"))
            return
        
        issues = []
        
        # Check for missing columns
        missing_cols = set(schema.keys()) - set(data.columns)
        if missing_cols:
            issues.append(f"Missing columns: {missing_cols}")
        
        # Check data types
        for col, expected_type in schema.items():
            if col in data.columns:
                actual_type = data[col].dtype
                if expected_type == str and not pd.api.types.is_string_dtype(actual_type):
                    issues.append(f"Column '{col}': expected string, got {actual_type}")
                elif expected_type == int and not pd.api.types.is_integer_dtype(actual_type):
                    issues.append(f"Column '{col}': expected integer, got {actual_type}")
                elif expected_type == float and not pd.api.types.is_numeric_dtype(actual_type):
                    issues.append(f"Column '{col}': expected numeric, got {actual_type}")
                elif expected_type == datetime and not pd.api.types.is_datetime64_any_dtype(actual_type):
                    issues.append(f"Column '{col}': expected datetime, got {actual_type}")
        
        passed = len(issues) == 0
        details = "Schema conformity passed" if passed else f"Schema issues: {'; '.join(issues)}"
        
        self.results.append(QualityCheckResult("schema_conformity", passed, details))
    
    def _check_data_availability(self, data_source_url: str):
        """Check if data source is reachable"""
        if not data_source_url:
            self.results.append(QualityCheckResult(
                "data_availability", True, "No data source URL provided - skipping check"))
            return
        
        try:
            response = requests.get(data_source_url, timeout=30)
            passed = response.status_code == 200
            details = f"Data source reachable (Status: {response.status_code})" if passed else f"Data source unreachable (Status: {response.status_code})"
        except requests.exceptions.Timeout:
            passed = False
            details = "Data source timeout - unreachable within 30 seconds"
        except requests.exceptions.RequestException as e:
            passed = False
            details = f"Data source error: {str(e)}"
        
        self.results.append(QualityCheckResult("data_availability", passed, details))
    
    def _check_record_count_anomaly(self, data: pd.DataFrame, historical_avg: int, tolerance: float):
        """Compare record counts to historical averages"""
        current_count = len(data)
        
        if not historical_avg:
            self.results.append(QualityCheckResult(
                "record_count_anomaly", True, 
                f"Current record count: {current_count}. No historical average provided - skipping anomaly check",
                value=current_count))
            return
        
        # Calculate acceptable range
        lower_bound = historical_avg * (1 - tolerance)
        upper_bound = historical_avg * (1 + tolerance)
        
        passed = lower_bound <= current_count <= upper_bound
        deviation_pct = abs(current_count - historical_avg) / historical_avg * 100
        
        if passed:
            details = f"Record count normal: {current_count} (historical avg: {historical_avg}, deviation: {deviation_pct:.1f}%)"
        else:
            details = f"Record count anomaly: {current_count} vs historical avg {historical_avg} (deviation: {deviation_pct:.1f}%, tolerance: {tolerance*100}%)"
        
        self.results.append(QualityCheckResult(
            "record_count_anomaly", passed, details, 
            value=current_count, threshold=f"{lower_bound:.0f}-{upper_bound:.0f}"))
    
    def _check_timestamp_freshness(self, data: pd.DataFrame, timestamp_column: str, threshold_hours: int):
        """Check if data is current or stale"""
        if not timestamp_column or timestamp_column not in data.columns:
            self.results.append(QualityCheckResult(
                "timestamp_freshness", True, "No timestamp column specified or found - skipping check"))
            return
        
        try:
            # Convert to datetime if not already
            timestamps = pd.to_datetime(data[timestamp_column])
            latest_timestamp = timestamps.max()
            current_time = datetime.now()
            
            # Handle timezone-naive datetimes
            if pd.isna(latest_timestamp):
                passed = False
                details = "No valid timestamps found in data"
            else:
                if latest_timestamp.tzinfo is None and current_time.tzinfo is None:
                    age_hours = (current_time - latest_timestamp).total_seconds() / 3600
                else:
                    # Convert both to naive datetime for comparison
                    if latest_timestamp.tzinfo is not None:
                        latest_timestamp = latest_timestamp.replace(tzinfo=None)
                    age_hours = (current_time - latest_timestamp).total_seconds() / 3600
                
                passed = age_hours <= threshold_hours
                details = f"Data age: {age_hours:.1f} hours (threshold: {threshold_hours} hours, latest: {latest_timestamp})"
        
        except Exception as e:
            passed = False
            details = f"Error checking timestamp freshness: {str(e)}"
        
        self.results.append(QualityCheckResult("timestamp_freshness", passed, details))
    
    def _check_missing_values(self, data: pd.DataFrame, required_fields: List[str]):
        """Check for null/missing values in required fields"""
        if not required_fields:
            self.results.append(QualityCheckResult(
                "missing_values", True, "No required fields specified - skipping check"))
            return
        
        issues = []
        
        for field in required_fields:
            if field not in data.columns:
                issues.append(f"Required field '{field}' not found in data")
                continue
            
            null_count = data[field].isnull().sum()
            null_pct = (null_count / len(data)) * 100
            
            if null_count > 0:
                issues.append(f"Field '{field}': {null_count} missing values ({null_pct:.1f}%)")
        
        passed = len(issues) == 0
        details = "No missing values in required fields" if passed else f"Missing value issues: {'; '.join(issues)}"
        
        self.results.append(QualityCheckResult("missing_values", passed, details))
    
    def _check_duplicate_records(self, data: pd.DataFrame, unique_fields: List[str]):
        """Check for duplicate records"""
        if not unique_fields:
            # Check for completely duplicate rows
            duplicate_count = data.duplicated().sum()
            passed = duplicate_count == 0
            details = f"Complete duplicate rows: {duplicate_count}"
        else:
            # Check for duplicates in specified fields
            issues = []
            
            for field in unique_fields:
                if field not in data.columns:
                    issues.append(f"Unique field '{field}' not found in data")
                    continue
                
                duplicate_count = data[field].duplicated().sum()
                if duplicate_count > 0:
                    issues.append(f"Field '{field}': {duplicate_count} duplicate values")
            
            passed = len(issues) == 0
            details = "No duplicates found in unique fields" if passed else f"Duplicate issues: {'; '.join(issues)}"
        
        self.results.append(QualityCheckResult("duplicate_records", passed, details))
    
    def _check_source_identifiers(self, data: pd.DataFrame, id_fields: List[str]):
        """Verify integrity of IDs, keys, etc."""
        if not id_fields:
            self.results.append(QualityCheckResult(
                "source_identifiers", True, "No ID fields specified - skipping check"))
            return
        
        issues = []
        
        for field in id_fields:
            if field not in data.columns:
                issues.append(f"ID field '{field}' not found in data")
                continue
            
            # Check for null IDs
            null_count = data[field].isnull().sum()
            if null_count > 0:
                issues.append(f"ID field '{field}': {null_count} null values")
            
            # Check for empty string IDs (if string type)
            if data[field].dtype == 'object':
                empty_count = (data[field] == '').sum()
                if empty_count > 0:
                    issues.append(f"ID field '{field}': {empty_count} empty string values")
            
            # Check for negative IDs (if numeric)
            if pd.api.types.is_numeric_dtype(data[field]):
                negative_count = (data[field] < 0).sum()
                if negative_count > 0:
                    issues.append(f"ID field '{field}': {negative_count} negative values")
        
        passed = len(issues) == 0
        details = "All ID fields have valid values" if passed else f"ID integrity issues: {'; '.join(issues)}"
        
        self.results.append(QualityCheckResult("source_identifiers", passed, details))
    
    def _compile_results(self) -> Dict[str, Any]:
        """Compile all check results into a comprehensive report"""
        total_checks = len(self.results)
        passed_checks = sum(1 for r in self.results if r.passed)
        overall_passed = passed_checks == total_checks
        
        return {
            'overall_quality_score': (passed_checks / total_checks) * 100 if total_checks > 0 else 0,
            'overall_passed': overall_passed,
            'total_checks': total_checks,
            'passed_checks': passed_checks,
            'failed_checks': total_checks - passed_checks,
            'timestamp': datetime.now().isoformat(),
            'detailed_results': {
                result.check_name: {
                    'passed': result.passed,
                    'details': result.details,
                    'value': result.value,
                    'threshold': result.threshold
                } for result in self.results
            },
            'summary': self._generate_summary()
        }
    
    def _generate_summary(self) -> str:
        """Generate a human-readable summary of results"""
        failed_checks = [r.check_name for r in self.results if not r.passed]
        
        if not failed_checks:
            return "All data quality checks passed successfully."
        else:
            return f"Data quality issues found in: {', '.join(failed_checks)}"


# Example usage function
def example_usage():    
    # Create sample data
    sample_data = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', None, 'Eve'],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com', 'dave@example.com', 'eve@example.com'],
        'created_at': pd.to_datetime(['2024-06-28', '2024-06-28', '2024-06-29', '2024-06-29', '2024-06-29']),
        'score': [85.5, 92.0, 78.5, 88.0, 95.5]
    })
    
    # Initialize checker
    checker = DataQualityChecker()
    
    # Define quality check parameters
    schema = {
        'id': int,
        'name': str,
        'email': str,
        'created_at': datetime,
        'score': float
    }
    
    # Run quality checks
    results = checker.quality_check(
        data=sample_data,
        schema=schema,
        data_source_url='https://httpbin.org/status/200',  # Example URL that returns 200
        historical_avg_records=5,
        record_count_tolerance=0.2,
        timestamp_column='created_at',
        freshness_threshold_hours=48,
        required_fields=['id', 'name', 'email'],
        unique_fields=['id', 'email'],
        id_fields=['id']
    )
    
    return results

if __name__ == "__main__":
    results = example_usage()
    print("Data Quality Check Results:")
    print(f"Overall Score: {results['overall_quality_score']:.1f}%")
    print(f"Summary: {results['summary']}")
    print("\nDetailed Results:")
    for check_name, check_result in results['detailed_results'].items():
        status = "✓ PASS" if check_result['passed'] else "✗ FAIL"
        print(f"{status} {check_name}: {check_result['details']}")