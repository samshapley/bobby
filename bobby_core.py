#!/usr/bin/env python3
"""
Police SQL Agent Core - The core functionality for the Police SQL Agent.

This module handles all database operations, Claude API interactions, tool calls,
and query processing. It's designed to be used by different UI implementations.
"""

import os
import sqlite3
import pandas as pd
import anthropic
from typing import Dict, List, Union, Any, Optional
from tabulate import tabulate

## add report creation system DONE
## report conversion to pdf from markdown (not working)
## add missing data handling and collection so agent can roast police departments - no
## add data on bootup - yes but not necessary for demo
## add ask for anthropic key if not set - why does it ask every time
## ensure all fully packaged and cleaned up for public release

## now just clean up how it look at get more dates

# Import the report tools
from bobby.report_tools import (
    create_or_update_report,
    create_or_update_report_section,
    create_report_pdf,
    list_available_reports,
    get_report_preview
)


class BobbyCore:
    """Core functionality for the Police SQL Agent."""
    
    # Database Schema Information
    DB_SCHEMA = """
Database contains UK Police data with the following consolidated tables:

1. Crimes Table:
   - id: Unique identifier for each crime record
   - city: City where the crime occurred (e.g., "london", "manchester")
   - data_date: Month in YYYY-MM format
   - force_id: Police force identifier (for crimes with no location)
   - category: Crime category (e.g., "theft-from-the-person", "violent-crime")
   - location_type: Type of location data ('street', 'specific', 'none')
   - location_latitude, location_longitude: Geographic coordinates
   - location_street_name: Street name where crime occurred
   - month: Month of crime in YYYY-MM format
   - outcome_status: Outcome status (may be NULL)
   - outcome_status_category: Category of outcome (e.g., "under-investigation")
   - outcome_status_date: Date of the outcome status

2. Outcomes Table:
   - id: Unique identifier for each outcome record
   - city: City where the outcome was recorded
   - data_date: Month in YYYY-MM format
   - force_id: Police force identifier (for outcomes with no location)
   - category_code: Outcome category code
   - category_name: Full name of outcome category
   - crime_category: Category of the related crime
   - crime_location_street_name: Street name where crime occurred
   - crime_month: Month of related crime
   - date: Date of the outcome

3. Stops Table (Stop and Search records):
   - id: Unique identifier for each stop and search record
   - city: City where the stop occurred (may be NULL for force-level stops)
   - force_id: Police force that conducted the stop
   - data_date: Month in YYYY-MM format
   - stop_type: Type of stop data ('standard', 'area', 'location', 'no_location')
   - age_range: Age range of the person stopped
   - gender: Gender of the person stopped
   - object_of_search: Reason for the stop
   - datetime: Date and time of the stop
   - location_latitude, location_longitude: Geographic coordinates (may be NULL)
   - outcome: Outcome of the stop and search
   - type: Type of stop and search
   - officer_defined_ethnicity: Ethnicity as defined by the officer
   - self_defined_ethnicity: Ethnicity as defined by the person stopped

4. Reference and Entity Tables:
   - police_forces: List of police forces with id and name
   - senior_officers: Information about senior officers in each force
   - neighborhoods: Neighborhoods under specific police forces
   - neighborhood_boundaries: Geographic boundaries of neighborhoods
   - neighborhood_teams: Team members of neighborhoods
   - neighborhood_events: Events in neighborhoods
   - neighborhood_priorities: Priorities for neighborhoods
   - crime_categories: Standard reference for crime categories
   - data_updates: Information about when data was last updated

Cities available in the database: london, manchester, birmingham, leeds, liverpool, glasgow, newcastle, cardiff

Query Examples:
- To get crimes in London for a specific date:
  SELECT * FROM crimes WHERE city = 'london' AND data_date = '2023-01' LIMIT 10;

- To get counts of different crime types in Manchester:
  SELECT category, COUNT(*) as count FROM crimes 
  WHERE city = 'manchester' AND data_date = '2023-01'
  GROUP BY category ORDER BY count DESC;

- To find stops by a specific force:
  SELECT * FROM stops WHERE force_id = 'metropolitan' AND data_date = '2023-01' LIMIT 10;
"""
    
    def __init__(self, db_path: str, api_key: Optional[str] = None):
        """Initialize the agent core with database path and API key."""
        self.db_path = db_path
        self.verify_db_access()
        
        # Use provided API key or environment variable
        if api_key:
            self.client = anthropic.Anthropic(api_key=api_key)
        else:
            self.client = anthropic.Anthropic()  # Uses ANTHROPIC_API_KEY from environment
        
        self.model = "claude-3-7-sonnet-20250219"
        self._define_tools()
        
        # Initialize conversation memory
        self.conversation_memory = []
        self.system_prompt = None
    
    def _define_tools(self):
        """Define tools for Claude to use."""
        self.tools = [
            {
                "name": "query_database",
                "description": "Execute a single SQL query against the UK Police database",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "SQL query to execute (must be valid SQLite SQL)"
                        }
                    },
                    "required": ["query"]
                }
            },
            {
                "name": "batch_query",
                "description": "Execute multiple SQL queries in batch against the UK Police database",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "queries": {
                            "type": "array",
                            "description": "List of SQL queries to execute in batch",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": {
                                        "type": "string",
                                        "description": "Descriptive name for this query"
                                    },
                                    "query": {
                                        "type": "string",
                                        "description": "SQL query to execute (must be valid SQLite SQL)"
                                    }
                                },
                                "required": ["name", "query"]
                            }
                        }
                    },
                    "required": ["queries"]
                }
            },
            {
                "name": "create_or_update_report",
                "description": "Create a new report or update an existing one with the same label",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "title": {
                            "type": "string",
                            "description": "The title of the report"
                        },
                        "label": {
                            "type": "string",
                            "description": "Unique label for this report (format: label_here, using snake_case)"
                        },
                        "abstract": {
                            "type": "string",
                            "description": "Brief description or summary of the report contents"
                        }
                    },
                    "required": ["title", "label"]
                }
            },
            {
                "name": "create_or_update_report_section",
                "description": "Create a new section in a report or update an existing section with the same label",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "report_label": {
                            "type": "string",
                            "description": "Label of the report to which this section belongs"
                        },
                        "header": {
                            "type": "string",
                            "description": "The header text for this section"
                        },
                        "header_level": {
                            "type": "integer",
                            "description": "The level of the header (1-6, where 1 is the highest level)",
                            "minimum": 1,
                            "maximum": 6
                        },
                        "content": {
                            "type": "string",
                            "description": "The content of the section in markdown format"
                        },
                        "section_label": {
                            "type": "string",
                            "description": "Unique label for this section (format: section_label_here, using snake_case)"
                        }
                    },
                    "required": ["report_label", "header", "header_level", "content", "section_label"]
                }
            },
            {
                "name": "create_report_pdf",
                "description": "Generate a PDF from a report and save it",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "report_label": {
                            "type": "string",
                            "description": "Label of the report to convert to PDF"
                        },
                        "output_path": {
                            "type": "string",
                            "description": "Optional path where the PDF should be saved. If not provided, it will be saved to the user's desktop."
                        }
                    },
                    "required": ["report_label"]
                }
            },
            {
                "name": "list_available_reports",
                "description": "Get a list of all available reports",
                "input_schema": {
                    "type": "object",
                    "properties": {}
                }
            },
            {
                "name": "get_report_preview",
                "description": "Get a preview of a report as markdown",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "report_label": {
                            "type": "string",
                            "description": "Label of the report to preview"
                        }
                    },
                    "required": ["report_label"]
                }
            }
        ]
    
    def verify_db_access(self) -> None:
        """Verify that the database exists and can be accessed."""
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"Database not found at {self.db_path}")
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1")
            tables = cursor.fetchone()
            conn.close()
            
            if not tables:
                raise ValueError(f"No tables found in database at {self.db_path}")
        except sqlite3.Error as e:
            raise RuntimeError(f"Error connecting to database: {e}")
    
    def get_available_tables(self) -> List[str]:
        """Get a list of all available tables in the database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        return tables
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a SQL query and return the results as a pandas DataFrame."""
        try:
            conn = sqlite3.connect(self.db_path)
            result = pd.read_sql_query(query, conn)
            conn.close()
            return result
        except sqlite3.Error as e:
            raise RuntimeError(f"Error executing query: {e}")
    
    def execute_batch_queries(self, queries: List[Dict[str, str]]) -> Dict[str, Union[pd.DataFrame, str]]:
        """Execute multiple SQL queries in batch and return the results."""
        results = {}
        for query_info in queries:
            name = query_info.get('name', f"Query_{len(results)}")
            query = query_info.get('query')
            if not query:
                continue
            
            try:
                results[name] = self.execute_query(query)
            except Exception as e:
                results[name] = f"Error executing query: {str(e)}"
        
        return results
    
    def format_results(self, results: Union[pd.DataFrame, Dict[str, pd.DataFrame]]) -> str:
        """Format query results for display to the user."""
        if isinstance(results, pd.DataFrame):
            if len(results) == 0:
                return "No results found."
            return tabulate(results, headers='keys', tablefmt='psql')
        
        elif isinstance(results, dict):
            formatted_results = []
            for name, result in results.items():
                formatted_results.append(f"--- {name} ---")
                if isinstance(result, pd.DataFrame):
                    if len(result) == 0:
                        formatted_results.append("No results found.")
                    else:
                        formatted_results.append(tabulate(result, headers='keys', tablefmt='psql'))
                else:
                    formatted_results.append(str(result))
                formatted_results.append("")
            
            return "\n".join(formatted_results)
        
        else:
            return str(results)
    
    def process_tool_call(self, tool_name: str, tool_input: Any) -> str:
        """Process a tool call and return the result."""
        if tool_name == "query_database":
            query = tool_input["query"]
            try:
                results = self.execute_query(query)
                return self.format_results(results)
            except Exception as e:
                return f"Error: {str(e)}"
                
        elif tool_name == "batch_query":
            queries = tool_input["queries"]
            try:
                results = self.execute_batch_queries(queries)
                return self.format_results(results)
            except Exception as e:
                return f"Error: {str(e)}"
        
        # Report tools
        elif tool_name == "create_or_update_report":
            try:
                result = create_or_update_report(
                    title=tool_input["title"],
                    label=tool_input["label"],
                    abstract=tool_input.get("abstract", "")
                )
                return self.format_tool_result(result)
            except Exception as e:
                return f"Error creating/updating report: {str(e)}"
                
        elif tool_name == "create_or_update_report_section":
            try:
                result = create_or_update_report_section(
                    report_label=tool_input["report_label"],
                    header=tool_input["header"],
                    header_level=tool_input["header_level"],
                    content=tool_input["content"],
                    section_label=tool_input["section_label"]
                )
                return self.format_tool_result(result)
            except Exception as e:
                return f"Error creating/updating report section: {str(e)}"
                
        elif tool_name == "create_report_pdf":
            try:
                result = create_report_pdf(
                    report_label=tool_input["report_label"],
                    output_path=tool_input.get("output_path")
                )
                return self.format_tool_result(result)
            except Exception as e:
                return f"Error creating PDF: {str(e)}"
                
        elif tool_name == "list_available_reports":
            try:
                result = list_available_reports()
                return self.format_tool_result(result)
            except Exception as e:
                return f"Error listing reports: {str(e)}"
                
        elif tool_name == "get_report_preview":
            try:
                result = get_report_preview(
                    report_label=tool_input["report_label"]
                )
                return self.format_tool_result(result)
            except Exception as e:
                return f"Error getting report preview: {str(e)}"
                
        return f"Unknown tool: {tool_name}"
    
    def format_tool_result(self, result: Dict[str, Any]) -> str:
        """Format the result of a tool call."""
        if not isinstance(result, dict):
            return str(result)
            
        # Format success/failure message
        message = f"{'✅' if result.get('success', False) else '❌'} {result.get('message', 'No message')}"
        
        # Format additional information
        additional_info = []
        
        # Format report info
        report_info = result.get('report_info')
        if report_info and isinstance(report_info, dict):
            additional_info.append("Report Information:")
            for key, value in report_info.items():
                additional_info.append(f"  - {key}: {value}")
                
        # Format section info
        section_info = result.get('section_info')
        if section_info and isinstance(section_info, dict):
            additional_info.append("Section Information:")
            for key, value in section_info.items():
                additional_info.append(f"  - {key}: {value}")
                
        # Format PDF path
        pdf_path = result.get('pdf_path')
        if pdf_path:
            additional_info.append(f"PDF saved at: {pdf_path}")
            
        # Format report list
        reports = result.get('reports')
        if reports and isinstance(reports, list):
            additional_info.append(f"Available Reports ({len(reports)}):")
            for report in reports:
                additional_info.append(f"  - {report.get('title')} (label: {report.get('label')}, sections: {report.get('sections', 0)})")
                
        # Format markdown preview
        markdown = result.get('markdown')
        if markdown and isinstance(markdown, str):
            # Truncate long markdown for readability
            if len(markdown) > 500:
                markdown_preview = markdown[:500] + "...\n(truncated for readability)"
            else:
                markdown_preview = markdown
                
            additional_info.append("Markdown Preview:")
            additional_info.append("```markdown")
            additional_info.append(markdown_preview)
            additional_info.append("```")
            
        # Combine all information
        if additional_info:
            return message + "\n\n" + "\n".join(additional_info)
        else:
            return message
    
    def add_to_memory(self, message):
        """Add a message to the conversation memory."""
        self.conversation_memory.append(message)
    
    def get_full_message_history(self):
        """Get the full conversation history."""
        return self.conversation_memory
    
    def clear_memory(self):
        """Clear the conversation memory."""
        self.conversation_memory = []
    
    def get_conversation_parts(self, question: str):
        """Get Claude's response with tool calls parsed and ready for processing."""
        # Get available tables to include in the prompt
        available_tables = self.get_available_tables()
        tables_info = "\nAvailable tables in the database:\n" + "\n".join(available_tables)
        
        # Create system prompt if not already defined
        if not self.system_prompt:
            self.system_prompt = f"""
You are a helpful assistant that answers questions about UK Police data by writing and executing SQL queries.

{self.DB_SCHEMA}

{tables_info}

To answer the user's questions:
1. Analyze what data is needed to answer the question
2. Determine which database tables contain the needed information
3. Write appropriate SQL queries to extract the data
4. Use the query_database tool for single queries or batch_query tool for multiple queries
5. Analyze the results and provide a clear, concise answer

Guidelines for SQL queries:
- Use proper SQLite syntax
- Use SELECT COUNT(*) for counting records
- Use GROUP BY for aggregating data
- Use JOIN to combine data from different tables
- Use WHERE clauses to filter data
- Limit results to a reasonable number with LIMIT when returning large datasets

You can also generate reports from your analyses using the available report tools:
- create_or_update_report: Create a new report with a title, label, and abstract
- create_or_update_report_section: Add or update a section within a report
- create_report_pdf: Generate a PDF from a report and save it to the user's desktop
- list_available_reports: List all available reports
- get_report_preview: Preview a report's markdown content

The user is not a database expert, so explain your approach in simple terms. Always provide context for your answers.
"""
        
        # Use the existing conversation memory if available, otherwise start fresh
        if not self.conversation_memory:
            messages = []
        else:
            messages = self.conversation_memory.copy()
        
        # Add the new user question to messages
        messages.append({"role": "user", "content": question})
        
        # Add the new question to the conversation memory
        self.add_to_memory({"role": "user", "content": question})
        
        # Get initial response from Claude
        response = self.client.messages.create(
            model=self.model,
            system=self.system_prompt,
            messages=messages,
            max_tokens=4000,
            temperature=0,
            tools=self.tools
        )
        
        # Return the parts of the conversation
        return {
            "messages": messages,
            "response": response,
            "system_prompt": self.system_prompt
        }
    
    def process_next_turn(self, messages, response, system_prompt):
        """Process the next turn of the conversation after tool results."""
        # Update conversation memory with assistant response and tool results
        # Note: These should be the last two messages in the messages list
        if len(messages) >= 2:
            self.add_to_memory(messages[-2])  # Assistant's response with tool call
            self.add_to_memory(messages[-1])  # Tool result
        
        return self.client.messages.create(
            model=self.model,
            system=system_prompt,
            messages=messages,
            max_tokens=4000,
            temperature=0,
            tools=self.tools
        )
