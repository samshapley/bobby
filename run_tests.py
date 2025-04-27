#!/usr/bin/env python3

"""
Test runner script for the UK Police API extractor project.
Run this script to execute all tests with proper reporting.
"""

import unittest
import sys
import os
import time
from datetime import datetime

def run_tests():
    """Run all tests and return the results."""
    # Set up test discovery
    start_time = time.time()
    start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Create test suite for all tests
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover("tests", pattern="test_*.py")
    
    # Get a list of all test files for reporting
    test_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tests")
    test_files = [f for f in os.listdir(test_dir) if f.startswith("test_") and f.endswith(".py")]
    print(f"Discovered {len(test_files)} test files: {', '.join(test_files)}")
    
    # Run the tests with text runner
    runner = unittest.TextTestRunner(verbosity=2)
    print(f"\n{'=' * 80}")
    print(f"RUNNING ALL TESTS - Started at {start_datetime}")
    print(f"{'=' * 80}\n")
    
    result = runner.run(test_suite)
    
    # Calculate execution time
    end_time = time.time()
    execution_time = end_time - start_time
    
    # Print summary
    print(f"\n{'=' * 80}")
    print("TEST SUMMARY")
    print(f"{'=' * 80}")
    print(f"Ran {result.testsRun} tests in {execution_time:.2f} seconds")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped)}")
    
    # Print details of failures and errors
    if result.failures:
        print(f"\n{'=' * 80}")
        print("FAILURES:")
        print(f"{'=' * 80}")
        for i, (test, traceback) in enumerate(result.failures, 1):
            print(f"Failure {i}: {test}")
            print(f"{traceback}")
    
    if result.errors:
        print(f"\n{'=' * 80}")
        print("ERRORS:")
        print(f"{'=' * 80}")
        for i, (test, traceback) in enumerate(result.errors, 1):
            print(f"Error {i}: {test}")
            print(f"{traceback}")
    
    # Return success status (0 for success, 1 for failure)
    return 0 if result.wasSuccessful() else 1

if __name__ == "__main__":
    # Set working directory to the project root
    project_root = os.path.dirname(os.path.abspath(__file__))
    os.chdir(project_root)
    
    # Run tests and exit with appropriate status code
    sys.exit(run_tests())