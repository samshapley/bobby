#!/usr/bin/env python3
"""
Test script for the Report Manager

This script tests the basic functionality of the report management system
to ensure it works correctly before integrating it with the Bobby agent.
"""

import os
import shutil
import time
from bobby.report_manager import ReportManager

# Test directory for reports (will be deleted after tests)
TEST_REPORTS_DIR = "test_reports"

def main():
    """Run tests for the report manager."""
    print("Testing Report Manager...")
    
    # Clean up previous test directory if it exists
    if os.path.exists(TEST_REPORTS_DIR):
        shutil.rmtree(TEST_REPORTS_DIR)
    
    # Create a fresh test directory
    os.makedirs(TEST_REPORTS_DIR, exist_ok=True)
    
    # Create the report manager
    manager = ReportManager(reports_dir=TEST_REPORTS_DIR)
    
    # Test 1: Create a report
    print("\nTest 1: Creating a report")
    report = manager.create_report(
        title="UK Police Crime Analysis",
        label="crime_analysis",
        abstract="Analysis of crime statistics from various UK police forces."
    )
    print(f"Created report: {report.title} (label: {report.label})")
    
    # Test 2: Add sections to the report
    print("\nTest 2: Adding sections to the report")
    
    # Add introduction section
    manager.create_report_section(
        report_label="crime_analysis",
        header="Introduction",
        header_level=2,
        content="This report analyzes crime data from UK police forces for the year 2023.\n\n"
                "The data is collected from the UK Police API and shows trends across different cities.",
        section_label="introduction"
    )
    print("Added introduction section")
    
    # Add methodology section
    manager.create_report_section(
        report_label="crime_analysis",
        header="Methodology",
        header_level=2,
        content="## Data Collection\n\n"
                "Data was collected using the UK Police API, focusing on the following cities:\n\n"
                "- London\n"
                "- Manchester\n"
                "- Birmingham\n\n"
                "## Analysis Approach\n\n"
                "Crime categories were aggregated and compared across cities using SQL queries.",
        section_label="methodology"
    )
    print("Added methodology section")
    
    # Add findings section
    manager.create_report_section(
        report_label="crime_analysis",
        header="Key Findings",
        header_level=2,
        content="### Crime Trends\n\n"
                "The analysis revealed the following trends:\n\n"
                "1. Violent crime decreased by 5% in London compared to the previous year\n"
                "2. Anti-social behavior increased in Manchester by 3%\n"
                "3. Birmingham showed stable rates across most crime categories\n\n"
                "### Regional Variations\n\n"
                "Significant variations were observed between city centers and suburban areas.",
        section_label="findings"
    )
    print("Added findings section")
    
    # Test 3: Update a section
    print("\nTest 3: Updating a section")
    manager.update_report_section(
        report_label="crime_analysis",
        section_label="findings",
        content="### Crime Trends\n\n"
                "The analysis revealed the following trends:\n\n"
                "1. Violent crime decreased by 5% in London compared to the previous year\n"
                "2. Anti-social behavior increased in Manchester by 3%\n"
                "3. Birmingham showed stable rates across most crime categories\n"
                "4. Drug-related offenses showed a notable decrease across all cities\n\n"
                "### Regional Variations\n\n"
                "Significant variations were observed between city centers and suburban areas.\n\n"
                "### Seasonal Patterns\n\n"
                "Crime rates showed seasonal variation, with higher rates during summer months.",
    )
    print("Updated findings section")
    
    # Test 4: Get report as markdown
    print("\nTest 4: Getting report as markdown")
    markdown_report = manager.get_report_as_markdown("crime_analysis")
    print(f"Report markdown length: {len(markdown_report)} characters")
    print("First 200 characters:")
    print(markdown_report[:200] + "...")
    
    # Test 5: Generate PDF
    print("\nTest 5: Generating PDF")
    pdf_path = os.path.join(TEST_REPORTS_DIR, "test_report.pdf")
    result = manager.generate_report_pdf("crime_analysis", output_path=pdf_path)
    if result:
        print(f"PDF generated at: {pdf_path}")
    else:
        print("Failed to generate PDF")
    
    # Test 6: Create a second report
    print("\nTest 6: Creating a second report")
    report2 = manager.create_report(
        title="Stop and Search Analysis",
        label="stop_search_analysis",
        abstract="Analysis of stop and search practices across UK police forces."
    )
    print(f"Created report: {report2.title} (label: {report2.label})")
    
    # Add a section to the second report
    manager.create_report_section(
        report_label="stop_search_analysis",
        header="Executive Summary",
        header_level=2,
        content="This report examines stop and search practices across different police forces.\n\n"
                "The analysis focuses on demographic patterns and outcome distributions.",
        section_label="executive_summary"
    )
    print("Added executive summary section to second report")
    
    # Test 7: List available reports
    print("\nTest 7: Listing available reports")
    reports = manager.get_available_reports()
    print(f"Found {len(reports)} reports:")
    for report_info in reports:
        print(f"- {report_info['title']} (label: {report_info['label']}, sections: {report_info['sections']})")
    
    # Test 8: Print a report preview
    print("\nTest 8: Print report preview")
    print("\nPreview of 'crime_analysis' report:")
    manager.print_report_preview("crime_analysis")
    
    # Clean up
    print("\nCleaning up test directory...")
    # Uncomment the following line to keep the test reports for inspection
    # shutil.rmtree(TEST_REPORTS_DIR)
    
    print("\nAll tests completed!")

if __name__ == "__main__":
    main()