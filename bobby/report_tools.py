#!/usr/bin/env python3
"""
Report Tools for Bobby - Integration of reporting tools with the Bobby agent

This module provides the integration between the Bobby agent and the report
management system. It defines the tools that the agent can use to create
and manage reports.
"""

import os
import logging
from typing import Dict, List, Optional, Any
from .report_manager import ReportManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("report_tools")

# Create a singleton report manager instance
report_manager = ReportManager()

# Tool Functions

def create_or_update_report(
    title: str,
    label: str,
    abstract: str = ""
) -> Dict[str, Any]:
    """Create a new report or update an existing one.
    
    Args:
        title: The report title
        label: Unique label for this report (format: label_here)
        abstract: A brief description of the report
        
    Returns:
        Dictionary with status and message
    """
    try:
        # Validate the label
        if not label or not isinstance(label, str):
            return {
                "success": False,
                "message": "Invalid label. Please provide a valid label in the format label_here."
            }
            
        # Create or update the report
        report = report_manager.create_report(
            title=title,
            label=label,
            abstract=abstract
        )
        
        return {
            "success": True,
            "message": f"Report '{title}' with label '{label}' has been successfully created/updated.",
            "report_info": {
                "title": report.title,
                "label": report.label,
                "abstract": report.abstract,
                "created_at": report.created_at,
                "updated_at": report.updated_at,
                "section_count": len(report.sections)
            }
        }
        
    except Exception as e:
        logger.error(f"Error creating/updating report: {str(e)}")
        return {
            "success": False,
            "message": f"Failed to create/update report: {str(e)}"
        }

def create_or_update_report_section(
    report_label: str,
    header: str,
    header_level: int,
    content: str,
    section_label: str
) -> Dict[str, Any]:
    """Create or update a section within a report.
    
    Args:
        report_label: Label of the report to add the section to
        header: The section header text
        header_level: The header level (1-6)
        content: The section content in markdown format
        section_label: Unique label for this section
        
    Returns:
        Dictionary with status and message
    """
    try:
        # Validate inputs
        if not report_label or not isinstance(report_label, str):
            return {
                "success": False,
                "message": "Invalid report label. Please provide a valid report label."
            }
            
        if not section_label or not isinstance(section_label, str):
            return {
                "success": False,
                "message": "Invalid section label. Please provide a valid section label."
            }
            
        if not header or not isinstance(header, str):
            return {
                "success": False,
                "message": "Invalid header. Please provide a valid section header."
            }
            
        if not isinstance(header_level, int) or header_level < 1 or header_level > 6:
            return {
                "success": False,
                "message": "Invalid header level. Please provide a level between 1 and 6."
            }
            
        if not content or not isinstance(content, str):
            return {
                "success": False,
                "message": "Invalid content. Please provide valid section content."
            }
            
        # Check if report exists
        report = report_manager.get_report(report_label)
        if not report:
            return {
                "success": False,
                "message": f"Report with label '{report_label}' not found."
            }
            
        # Check if section exists to determine if this is an add or update
        existing_section = report.sections.get(section_label)
        operation = "Update" if existing_section else "Create"
        
        # Create or update the section
        result = report_manager.create_report_section(
            report_label=report_label,
            header=header,
            header_level=header_level,
            content=content,
            section_label=section_label
        )
        
        if not result:
            return {
                "success": False,
                "message": f"Failed to {operation.lower()} section '{section_label}'."
            }
            
        return {
            "success": True,
            "message": f"{operation}d section '{header}' with label '{section_label}' in report '{report.title}'.",
            "operation": operation.lower(),
            "section_info": {
                "header": header,
                "header_level": header_level,
                "section_label": section_label,
                "content_length": len(content)
            }
        }
        
    except Exception as e:
        logger.error(f"Error creating/updating report section: {str(e)}")
        return {
            "success": False,
            "message": f"Failed to create/update report section: {str(e)}"
        }

def create_report_pdf(
    report_label: str,
    output_path: Optional[str] = None
) -> Dict[str, Any]:
    """Generate a PDF from a report.
    
    Args:
        report_label: Label of the report to convert
        output_path: Path to save the PDF (if None, saves to desktop)
        
    Returns:
        Dictionary with status, message, and the path to the generated PDF
    """
    try:
        # Validate inputs
        if not report_label or not isinstance(report_label, str):
            return {
                "success": False,
                "message": "Invalid report label. Please provide a valid report label."
            }
            
        # Check if report exists
        report = report_manager.get_report(report_label)
        if not report:
            return {
                "success": False,
                "message": f"Report with label '{report_label}' not found."
            }
            
        # Generate the PDF
        pdf_path = report_manager.generate_report_pdf(
            report_label=report_label,
            output_path=output_path
        )
        
        if not pdf_path:
            return {
                "success": False,
                "message": f"Failed to generate PDF for report '{report.title}'."
            }
            
        return {
            "success": True,
            "message": f"PDF generated for report '{report.title}' at '{pdf_path}'.",
            "pdf_path": pdf_path
        }
        
    except Exception as e:
        logger.error(f"Error generating PDF: {str(e)}")
        return {
            "success": False,
            "message": f"Failed to generate PDF: {str(e)}"
        }

def list_available_reports() -> Dict[str, Any]:
    """Get a list of available reports.
    
    Returns:
        Dictionary with status, message, and list of reports
    """
    try:
        reports = report_manager.get_available_reports()
        
        return {
            "success": True,
            "message": f"Found {len(reports)} reports.",
            "reports": reports
        }
        
    except Exception as e:
        logger.error(f"Error listing reports: {str(e)}")
        return {
            "success": False,
            "message": f"Failed to list reports: {str(e)}"
        }

def get_report_preview(report_label: str) -> Dict[str, Any]:
    """Get a preview of a report as markdown.
    
    Args:
        report_label: Label of the report to preview
        
    Returns:
        Dictionary with status, message, and the report markdown
    """
    try:
        # Validate inputs
        if not report_label or not isinstance(report_label, str):
            return {
                "success": False,
                "message": "Invalid report label. Please provide a valid report label."
            }
            
        # Get the report markdown
        markdown = report_manager.get_report_as_markdown(report_label)
        
        if not markdown:
            return {
                "success": False,
                "message": f"Report with label '{report_label}' not found or has no content."
            }
            
        # Get the report object for additional info
        report = report_manager.get_report(report_label)
        
        return {
            "success": True,
            "message": f"Retrieved markdown preview for report '{report.title}'.",
            "report_info": {
                "title": report.title,
                "label": report.label,
                "abstract": report.abstract,
                "created_at": report.created_at,
                "updated_at": report.updated_at,
                "section_count": len(report.sections)
            },
            "markdown": markdown
        }
        
    except Exception as e:
        logger.error(f"Error getting report preview: {str(e)}")
        return {
            "success": False,
            "message": f"Failed to get report preview: {str(e)}"
        }