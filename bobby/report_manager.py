#!/usr/bin/env python3
"""
Report Manager for Bobby - Allows creation and maintenance of structured reports

This module handles all report-related functionality, including:
- Creating and updating reports
- Adding and updating report sections
- Generating PDF from markdown reports
"""

import os
import json
import time
from datetime import datetime
from pathlib import Path
import logging
import sys
from typing import Dict, List, Optional, Union, Any
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.markdown import Markdown

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("report_manager")

# Constants
REPORTS_DIR = "reports"
DEFAULT_DESKTOP_PATH = os.path.expanduser("~/Desktop")


class ReportSection:
    """Represents a section within a report."""

    def __init__(
        self,
        header: str,
        header_level: int,
        content: str,
        label: str
    ):
        """Initialize a report section.

        Args:
            header: The section header text
            header_level: The header level (1-6)
            content: The section content in markdown format
            label: Unique label for this section
        """
        self.header = header
        self.header_level = max(1, min(6, header_level))  # Ensure between 1-6
        self.content = content
        self.label = label
        self.created_at = datetime.now().isoformat()
        self.updated_at = self.created_at

    def update(
        self,
        header: Optional[str] = None,
        header_level: Optional[int] = None,
        content: Optional[str] = None
    ):
        """Update the section with new information.

        Args:
            header: New header text (if None, keeps current)
            header_level: New header level (if None, keeps current)
            content: New content text (if None, keeps current)
        """
        if header is not None:
            self.header = header

        if header_level is not None:
            self.header_level = max(1, min(6, header_level))

        if content is not None:
            self.content = content

        self.updated_at = datetime.now().isoformat()

    def to_markdown(self) -> str:
        """Convert the section to markdown format."""
        header_markers = "#" * self.header_level
        return f"{header_markers} {self.header}\n\n{self.content}\n\n"

    def to_dict(self) -> Dict[str, Any]:
        """Convert the section to a dictionary for serialization."""
        return {
            "header": self.header,
            "header_level": self.header_level,
            "content": self.content,
            "label": self.label,
            "created_at": self.created_at,
            "updated_at": self.updated_at
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ReportSection':
        """Create a ReportSection instance from a dictionary."""
        section = cls(
            header=data["header"],
            header_level=data["header_level"],
            content=data["content"],
            label=data["label"]
        )
        section.created_at = data["created_at"]
        section.updated_at = data["updated_at"]
        return section


class Report:
    """Represents a complete report with metadata and sections."""

    def __init__(self, title: str, label: str, abstract: str = ""):
        """Initialize a report.

        Args:
            title: The report title
            label: Unique label for this report (format: label_here)
            abstract: A brief description of the report
        """
        self.title = title
        self.label = label
        self.abstract = abstract
        self.sections: Dict[str, ReportSection] = {}
        self.created_at = datetime.now().isoformat()
        self.updated_at = self.created_at
        self.section_order: List[str] = []

    def update(self, title: Optional[str] = None, abstract: Optional[str] = None):
        """Update the report's metadata.

        Args:
            title: New title (if None, keeps current)
            abstract: New abstract (if None, keeps current)
        """
        if title is not None:
            self.title = title

        if abstract is not None:
            self.abstract = abstract

        self.updated_at = datetime.now().isoformat()

    def add_section(self, section: ReportSection) -> bool:
        """Add a section to the report.

        Args:
            section: The section to add

        Returns:
            True if a new section was added, False if an existing one was updated
        """
        is_new = section.label not in self.sections

        if is_new:
            self.section_order.append(section.label)

        self.sections[section.label] = section
        self.updated_at = datetime.now().isoformat()

        return is_new

    def update_section(
        self,
        label: str,
        header: Optional[str] = None,
        header_level: Optional[int] = None,
        content: Optional[str] = None
    ) -> bool:
        """Update an existing section.

        Args:
            label: Label of the section to update
            header: New header text (if None, keeps current)
            header_level: New header level (if None, keeps current)
            content: New content text (if None, keeps current)

        Returns:
            True if the section was found and updated, False otherwise
        """
        if label not in self.sections:
            return False

        self.sections[label].update(
            header=header,
            header_level=header_level,
            content=content
        )

        self.updated_at = datetime.now().isoformat()
        return True

    def to_markdown(self) -> str:
        """Convert the report to markdown format."""
        markdown_content = f"# {self.title}\n\n"

        if self.abstract:
            markdown_content += f"*{self.abstract}*\n\n"
            markdown_content += "---\n\n"

        for section_label in self.section_order:
            if section_label in self.sections:
                markdown_content += self.sections[section_label].to_markdown()

        # Add a footer
        markdown_content += f"\n\n---\n\n*Generated by Bobby on {datetime.now().strftime('%Y-%m-%d %H:%M')}*"

        return markdown_content

    def to_dict(self) -> Dict[str, Any]:
        """Convert the report to a dictionary for serialization."""
        return {
            "title": self.title,
            "label": self.label,
            "abstract": self.abstract,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "section_order": self.section_order,
            "sections": {
                label: section.to_dict()
                for label, section in self.sections.items()
            }
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Report':
        """Create a Report instance from a dictionary."""
        report = cls(
            title=data["title"],
            label=data["label"],
            abstract=data["abstract"]
        )

        report.created_at = data["created_at"]
        report.updated_at = data["updated_at"]
        report.section_order = data["section_order"]

        for label, section_data in data["sections"].items():
            report.sections[label] = ReportSection.from_dict(section_data)

        return report


class ReportManager:
    """Manages the creation, updating, and generation of reports."""

    def __init__(self, reports_dir: str = REPORTS_DIR):
        """Initialize the report manager.

        Args:
            reports_dir: Directory to store reports
        """
        self.reports_dir = reports_dir
        self.reports: Dict[str, Report] = {}

        # Ensure the reports directory exists
        os.makedirs(self.reports_dir, exist_ok=True)

        # Load existing reports
        self._load_reports()

        # Set up console for rich output
        self.console = Console()

    def _load_reports(self):
        """Load all existing reports from disk."""
        report_files = Path(self.reports_dir).glob("*.json")

        for report_file in report_files:
            try:
                with open(report_file, 'r') as f:
                    report_data = json.load(f)

                report = Report.from_dict(report_data)
                self.reports[report.label] = report
            except Exception as e:
                logger.error(f"Error loading report {report_file}: {e}")

    def _save_report(self, report: Report):
        """Save a report to disk.

        Args:
            report: The report to save
        """
        report_path = os.path.join(self.reports_dir, f"{report.label}.json")

        try:
            with open(report_path, 'w') as f:
                json.dump(report.to_dict(), f, indent=2)
        except Exception as e:
            logger.error(f"Error saving report {report.label}: {e}")

    def create_report(self, title: str, label: str, abstract: str = "") -> Report:
        """Create a new report or update an existing one.

        Args:
            title: The report title
            label: Unique label for this report (format: label_here)
            abstract: A brief description of the report

        Returns:
            The created or updated report
        """
        # Validate the label format
        if not self._is_valid_label(label):
            label = self._convert_to_valid_label(label)

        # Check if report already exists
        if label in self.reports:
            report = self.reports[label]
            report.update(title=title, abstract=abstract)
        else:
            report = Report(title=title, label=label, abstract=abstract)
            self.reports[label] = report

        # Save the report
        self._save_report(report)

        return report

    def create_report_section(
        self,
        report_label: str,
        header: str,
        header_level: int,
        content: str,
        section_label: str
    ) -> bool:
        """Create or update a section within a report.

        Args:
            report_label: Label of the report to add the section to
            header: The section header text
            header_level: The header level (1-6)
            content: The section content in markdown format
            section_label: Unique label for this section

        Returns:
            True if successful, False otherwise
        """
        # Check if report exists
        if report_label not in self.reports:
            return False

        report = self.reports[report_label]

        # Validate the section label format
        if not self._is_valid_label(section_label):
            section_label = self._convert_to_valid_label(section_label)

        # Create the section
        section = ReportSection(
            header=header,
            header_level=header_level,
            content=content,
            label=section_label
        )

        # Add to report
        is_new = report.add_section(section)

        # Save the report
        self._save_report(report)
        return True

    def update_report_section(
        self,
        report_label: str,
        section_label: str,
        header: Optional[str] = None,
        header_level: Optional[int] = None,
        content: Optional[str] = None
    ) -> bool:
        """Update an existing section within a report.

        Args:
            report_label: Label of the report containing the section
            section_label: Label of the section to update
            header: New header text (if None, keeps current)
            header_level: New header level (if None, keeps current)
            content: New content text (if None, keeps current)

        Returns:
            True if successful, False otherwise
        """
        # Check if report exists
        if report_label not in self.reports:
            return False

        report = self.reports[report_label]

        # Update the section
        success = report.update_section(
            label=section_label,
            header=header,
            header_level=header_level,
            content=content
        )

        if not success:
            return False

        # Save the report
        self._save_report(report)

        return True

    def get_report_as_markdown(self, report_label: str) -> Optional[str]:
        """Get a report as markdown.

        Args:
            report_label: Label of the report to get

        Returns:
            The report in markdown format, or None if not found
        """
        if report_label not in self.reports:
            return None

        report = self.reports[report_label]
        return report.to_markdown()

    def generate_report_pdf(
        self, 
        report_label: str, 
        output_path: Optional[str] = None
    ) -> Optional[str]:
        """Generate a PDF from a report using markdown-pdf.
        
        Args:
            report_label: Label of the report to convert
            output_path: Path to save the PDF (if None, saves to desktop)
            
        Returns:
            The path to the generated PDF, or None if generation failed
        """
        # Check if report exists
        if report_label not in self.reports:
            logger.error(f"Report not found: {report_label}")
            return None
            
        report = self.reports[report_label]
        
        # Generate markdown
        markdown_content = report.to_markdown()
        
        # Determine output path
        if output_path is None:
            desktop_path = DEFAULT_DESKTOP_PATH
            if not os.path.exists(desktop_path):
                desktop_path = os.path.expanduser("~")  # Fallback to home directory
                
            # Create safe filename
            safe_title = "".join(c for c in report.title if c.isalnum() or c in " _-").rstrip()
            safe_title = safe_title.replace(" ", "_")
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            filename = f"{safe_title}_{timestamp}.pdf"
            
            output_path = os.path.join(desktop_path, filename)
        
        try:
            # Try using markdown-pdf
            try:
                from markdown_pdf import MarkdownPdf, Section
                
                # Create a PDF with table of contents from headings up to level 3
                pdf = MarkdownPdf(toc_level=3)
                
                # First create a combined markdown document with proper heading hierarchy
                complete_markdown = f"# {report.title}\n\n"
                
                if report.abstract:
                    complete_markdown += f"*{report.abstract}*\n\n---\n\n"
                
                # Add each section of the report, ensuring proper heading hierarchy
                for section_label in report.section_order:
                    if section_label in report.sections:
                        section = report.sections[section_label]
                        # Need to ensure sections start at level 2 or higher, since title is level 1
                        header_level = max(2, section.header_level)
                        header_markers = "#" * header_level
                        section_content = f"{header_markers} {section.header}\n\n{section.content}\n\n"
                        complete_markdown += section_content
                
                # Add footer
                complete_markdown += f"\n\n---\n\n*Generated by Bobby on {datetime.now().strftime('%Y-%m-%d %H:%M')}*"
                
                # Add the entire document as a single section to avoid hierarchy issues
                pdf.add_section(Section(complete_markdown))
                
                # Set metadata
                pdf.meta["title"] = report.title
                pdf.meta["subject"] = report.abstract if report.abstract else "Report generated by Bobby"
                pdf.meta["creator"] = "Bobby Report Manager"
                
                # Save the PDF
                pdf.save(output_path)
                
                return output_path
                
            except ImportError:
                logger.warning("markdown-pdf not available, trying to install it...")
                
                # Try to install markdown-pdf using pip
                import subprocess
                try:
                    subprocess.run(
                        [sys.executable, "-m", "pip", "install", "markdown-pdf"],
                        check=True,
                        capture_output=True
                    )
                    
                    # Save markdown file as fallback
                    md_path = output_path.replace(".pdf", ".md")
                    with open(md_path, 'w', encoding='utf-8') as f:
                        f.write(markdown_content)
                    
                    return None
                    
                except Exception as pip_error:
                    
                    # As a fallback, just save the markdown file
                    md_path = output_path.replace(".pdf", ".md")
                    with open(md_path, 'w', encoding='utf-8') as f:
                        f.write(markdown_content)
                    
                    return md_path
        
        except Exception as e:
            
            # Fallback: save as markdown if PDF generation fails
            md_path = output_path.replace(".pdf", ".md")
            with open(md_path, 'w', encoding='utf-8') as f:
                f.write(markdown_content)
            return md_path

    def get_report(self, report_label: str) -> Optional[Report]:
        """Get a report by label.

        Args:
            report_label: Label of the report to get

        Returns:
            The Report object, or None if not found
        """
        return self.reports.get(report_label)

    def _is_valid_label(self, label: str) -> bool:
        """Check if a label has the correct format.

        Args:
            label: The label to check

        Returns:
            True if valid, False otherwise
        """
        # Label should be in snake_case format
        return label.isascii() and all(c.islower() or c.isdigit() or c == '_' for c in label)

    def _convert_to_valid_label(self, label: str) -> str:
        """Convert a string to a valid label format.

        Args:
            label: The string to convert

        Returns:
            A valid label
        """
        # Convert to lowercase, replace non-alphanumeric with underscore
        valid_label = ''.join(
            c.lower() if c.isalnum() else '_'
            for c in label if c.isascii()
        )

        # Remove consecutive underscores
        while '__' in valid_label:
            valid_label = valid_label.replace('__', '_')

        # Remove leading/trailing underscores
        valid_label = valid_label.strip('_')

        # If empty, use default
        if not valid_label:
            valid_label = f"report_{int(time.time())}"

        return valid_label

    def delete_report(self, report_label: str) -> bool:
        """Delete a report.

        Args:
            report_label: Label of the report to delete

        Returns:
            True if successful, False otherwise
        """
        if report_label not in self.reports:
            return False

        # Remove from memory
        del self.reports[report_label]

        # Remove from disk
        report_path = os.path.join(self.reports_dir, f"{report_label}.json")
        try:
            os.remove(report_path)
            return True
        except Exception as e:
            return False

    def print_report_preview(self, report_label: str):
        """Print a Rich-formatted preview of the report to the console.

        Args:
            report_label: Label of the report to preview
        """
        if report_label not in self.reports:
            self.console.print(f"[bold red]Report not found: {report_label}[/bold red]")
            return

        report = self.reports[report_label]

        # Print report title and abstract
        self.console.print(Panel(
            f"[bold blue]{report.title}[/bold blue]\n\n[italic]{report.abstract}[/italic]",
            title="Report Preview",
            border_style="blue"
        ))

        # Print sections
        for section_label in report.section_order:
            if section_label in report.sections:
                section = report.sections[section_label]

                # Format section header based on level
                header_style = "bold " + ["cyan", "green", "yellow", "magenta", "red", "blue"][section.header_level - 1]
                header_text = f"[{header_style}]{section.header}[/{header_style}]"

                # Format content as markdown
                content_md = Markdown(section.content)

                self.console.print(Panel(
                    content_md,
                    title=header_text,
                    border_style="dim"
                ))