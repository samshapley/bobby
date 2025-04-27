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

## add report creation system
## report conversion to pdf from markdown
## add missing data handling and collection so agent can roast police departments
## add data on bootup
## add ask for anthropic key if not set
## ensure all fully packaged and cleaned up for public release


class BobbyCore:
    """Core functionality for the Police SQL Agent."""
    
    # Database Schema Information
    DB_SCHEMA = """
Database contains UK Police data with the following main tables:

1. Crimes Tables - Named like crimes_[city]_[date]:
   - category: Crime category (e.g., "theft-from-the-person", "violent-crime")
   - location_latitude, location_longitude: Geographic coordinates
   - location_street_name: Street name where crime occurred
   - month: Month of crime in YYYY-MM format
   - outcome_status: Outcome status (may be NULL)
   - outcome_status_category: Category of outcome (e.g., "under-investigation")
   - outcome_status_date: Date of the outcome status

2. Outcomes Tables - Named like outcomes_[city]_[date]:
   - category_code: Outcome category code
   - category_name: Full name of outcome category
   - crime_category: Category of the related crime
   - crime_location_street_name: Street name where crime occurred
   - crime_month: Month of related crime
   - date: Date of the outcome

3. Force-related Tables:
   - police_forces: List of police forces with id and name
   - force_details_[force_id]: Details about specific police forces
   - senior_officers_[force_id]: Information about senior officers

4. Neighborhood-related Tables:
   - neighborhoods_[force_id]: Neighborhoods under specific police forces
   - neighborhood_details_[force_id]_[neighborhood_id]: Details about specific neighborhoods

5. Crime Categories Table:
   - name: Name of crime category
   - url: API URL for the category

Cities available in the database: london, manchester, birmingham, leeds, liverpool
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
                
        return f"Unknown tool: {tool_name}"
    
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