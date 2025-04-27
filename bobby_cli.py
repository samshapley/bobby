#!/usr/bin/env python3

import argparse
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.table import Table
from rich.syntax import Syntax
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Prompt
from rich.markdown import Markdown
from rich.live import Live
from rich import box
import time
import os

from bobby_core import BobbyCore

console = Console()

import logging
logger = logging.getLogger("bobby_cli")
logger.setLevel(logging.DEBUG)


class BobbyCLI:
    """Command Line Interface for the Bobby agent."""
    
    def __init__(self, db_path: str):
        """Initialize the CLI with database path."""
        self.core = BobbyCore(db_path)
        
    def display_animation(self):
        """Display a cool title animation with police lights."""
        import itertools

        os.system('cls' if os.name == 'nt' else 'clear')

        ascii_art = [
            "██████╗  ██████╗ ██████╗ ██████╗ ██╗   ██╗",
            "██╔══██╗██╔═══██╗██╔══██╗██╔══██╗╚██╗ ██╔╝",
            "██████╔╝██║   ██║██████╔╝██████╔╝ ╚████╔╝ ",
            "██╔══██╗██║   ██║██╔══██╗██╔══██╗  ╚██╔╝  ",
            "██████╔╝╚██████╔╝██████╔╝██████╔╝   ██║   ",
            "╚═════╝  ╚═════╝ ╚═════╝ ╚═════╝    ╚═╝   ",
        ]
        title = "Police Agent 001"
        catchphrase = "Fighting crime with data, one query at a time"

        # Police light animation: alternate red/blue on left/right
        lights = [
            ("[bold red]■[/bold red]", "[bold blue]■[/bold blue]"),
            ("[bold blue]■[/bold blue]", "[bold red]■[/bold red]"),
        ]

        with Live(auto_refresh=True, refresh_per_second=8) as live:
            for i in range(12):  # Number of animation frames
                left_light, right_light = lights[i % 2]
                art_with_lights = "\n".join(
                    f"{left_light} {line} {right_light}" for line in ascii_art
                )
                panel = Panel.fit(
                    Text.from_markup(
                        f"{art_with_lights}\n\n[bold blue]Bobby[/bold blue]: {title}\n[italic]{catchphrase}[/italic]",
                        justify="center"
                    ),
                    border_style="blue",
                    box=box.DOUBLE
                )
                live.update(panel)
                time.sleep(0.13)

            # Final display, steady lights
            art_with_lights = "\n".join(
                f"[bold red]■[/bold red] {line} [bold blue]■[/bold blue]" for line in ascii_art
            )
            panel = Panel.fit(
                Text.from_markup(
                    f"{art_with_lights}\n\n[bold blue]Bobby[/bold blue]: {title}\n[italic]{catchphrase}[/italic]",
                    justify="center"
                ),
                border_style="blue",
                box=box.DOUBLE
            )
            live.update(panel)
            time.sleep(0.5)
    
    def display_tool_call(self, tool_name: str, tool_input: dict):
        """Display a tool call with syntax highlighting."""
        if tool_name == "query_database":
            syntax = Syntax(tool_input['query'], "sql", theme="monokai", line_numbers=False)
            console.print(Panel(syntax, title="[bold green]Executing SQL Query[/bold green]", border_style="green"))
        elif tool_name == "batch_query":
            console.print(f"[bold blue]Executing batch of {len(tool_input['queries'])} queries:[/bold blue]")
            for query_info in tool_input['queries']:
                query = query_info['query']
                syntax = Syntax(query, "sql", theme="monokai", line_numbers=False)
                console.print(Panel(syntax, title=f"[green]{query_info['name']}[/green]", border_style="green"))
    
    def display_results(self, results: str):
        """Display results in a formatted panel."""
        console.print(Panel(results, title="[bold cyan]Results[/bold cyan]", border_style="cyan"))
    
    def display_agent_thought(self, text: str):
        """Display agent's thinking process."""
        if text.strip():
            console.print(Panel(text, title="[bold blue]Bobby[/bold blue]", border_style="blue"))
    
    def _handle_stream_events(self, stream):
        """Handle all streaming events and return the response content and tool calls."""
        current_text_buffer = ""
        tool_calls = []
        current_tool = None
        in_tool_block = False
        tool_json_buffer = ""
        
        # Setup for live display
        from rich.live import Live
        from rich.align import Align
        
        # Function to get the current panel
        def get_panel():
            return Panel(
                current_text_buffer if current_text_buffer.strip() else Align.center("[dim]Waiting for response...[/dim]"),
                title="[bold blue]Bobby[/bold blue]", 
                border_style="blue"
            )
        
        # Start with an empty panel
        with Live(get_panel(), refresh_per_second=8, console=console) as live:
            for event in stream:
                
                # Handle different types of streaming events
                if event.type == "message_start":
                    # Nothing specific to do here
                    pass
                
                elif event.type == "content_block_start":
                    if event.content_block.type == "text":
                        # Continue normal text display
                        pass
                    elif event.content_block.type == "tool_use":
                        # Start tracking a tool use block
                        in_tool_block = True
                        current_tool = {
                            "id": event.content_block.id,
                            "name": event.content_block.name,
                            "input": {},
                            "partial_json": ""
                        }
                
                elif event.type == "content_block_delta":
                    if event.delta.type == "text_delta":
                        # Only update text buffer if we're not in a tool block
                        if not in_tool_block:
                            current_text_buffer += event.delta.text
                            live.update(get_panel())
                    
                    elif event.delta.type == "input_json_delta" and current_tool and in_tool_block:
                        # Accumulate tool input JSON
                        current_tool["partial_json"] += event.delta.partial_json
                
                elif event.type == "content_block_stop":
                    if in_tool_block and current_tool:
                        # Process the complete JSON for the tool
                        try:
                            import json
                            current_tool["input"] = json.loads(current_tool["partial_json"])
                            tool_calls.append(current_tool)
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse tool JSON: {e}")
                        
                        in_tool_block = False
                        current_tool = None
                
                elif event.type == "message_stop":
                    # Final update
                    live.update(get_panel())
        
        return current_text_buffer, tool_calls, not in_tool_block
    
    def process_question_streaming(self, question: str):
        """Process a user question and display the analysis process with streaming."""
        from rich.align import Align
        from rich.live import Live

        # Custom blue/red flashing loader with timer
        def get_loader_panel(frame: int, elapsed: float):
            # Swap colors every frame
            if frame % 2 == 0:
                left, right = "[bold red]■[/bold red]", "[bold blue]■[/bold blue]"
            else:
                left, right = "[bold blue]■[/bold blue]", "[bold red]■[/bold red]"
            loader = f"{left} [bold]Thinking...[/bold] {right} [dim]{elapsed:.1f}s[/dim]"
            return Panel(
                Align.center(Text.from_markup(loader), vertical="middle"),
                border_style="blue",
                box=box.ROUNDED,
                height=3
            )

        # Run the animation while waiting for streaming setup
        import threading
        result = [None]

        def worker():
            result[0] = self.core.get_conversation_parts_streaming(question)

        t = threading.Thread(target=worker)
        t.start()
        
        # Track thinking time while thread is running
        start_time = time.time()
        frame = 0
        
        # Only keep the most recent thinking panel in view
        with Live(get_loader_panel(0, 0.0), refresh_per_second=10, console=console) as live:
            while t.is_alive():
                elapsed = time.time() - start_time
                live.update(get_loader_panel(frame, elapsed))
                time.sleep(0.1)
                frame += 1
            
            # Thread is done, join it
            t.join()
            
            # Just stop showing the thinking timer once thread is done
            # Don't add the "Thought for X seconds" panel here
            # The live display will end naturally and the stream content
            # will be displayed in _handle_stream_events
        
        conversation_parts = result[0]
        messages = conversation_parts['messages']
        stream = conversation_parts['stream']
        system_prompt = conversation_parts['system_prompt']
        
        # Process the streaming response
        try:
            while True:  # Loop to handle iterative tool calls
                
                # Get the streamed content and any tool calls
                # IMPORTANT: We're now relying on this method to display the content
                # and NOT re-displaying it afterward
                current_text_buffer, tool_calls, is_complete = self._handle_stream_events(stream)
                
                # Handle any tool calls
                if tool_calls:
                    
                    # Package the response content for message update
                    response_content = []
                    if current_text_buffer.strip():
                        response_content.append({"type": "text", "text": current_text_buffer})
                    
                    # Process each tool call
                    tool_results = []
                    for tool in tool_calls:
                        response_content.append({
                            "type": "tool_use",
                            "id": tool["id"],
                            "name": tool["name"],
                            "input": tool["input"]
                        })
                        
                        # Display and execute the tool
                        self.display_tool_call(tool["name"], tool["input"])
                        
                        with console.status(f"[cyan]Executing {tool['name']}...", spinner="arc"):
                            tool_result = self.core.process_tool_call(tool["name"], tool["input"])
                        
                        self.display_results(tool_result)
                        
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": tool["id"],
                            "content": tool_result
                        })
                    
                    # Add the assistant's response with tool calls to messages
                    messages.append({"role": "assistant", "content": response_content})
                    
                    # Add all tool results in a single user message
                    messages.append({"role": "user", "content": tool_results})
                    
                    # Get next streaming response to continue the conversation
                    with console.status("[bold white]Analyzing results...", spinner="dots"):
                        stream = self.core.process_next_turn_streaming(messages, response_content, system_prompt)
                    
                    # Continue with the next iteration to process the new stream
                    continue
                else:
                    # No tool calls, just end the conversation turn
                    if current_text_buffer.strip():
                        # Add final response to memory
                        final_response = {"role": "assistant", "content": [{"type": "text", "text": current_text_buffer}]}
                        self.core.add_to_memory(final_response)
                    break
                    
        except Exception as e:
            logger.exception("Error in streaming process")
            console.print(f"\n[bold red]Streaming error:[/bold red] {str(e)}")
    
    def run_interactive_mode(self):
        """Run the interactive mode with a colorful prompt."""
        self.display_animation()
        console.print()
        console.print("[white]Type 'exit' or 'quit' to end the session[/white]")
        console.print("[white]Press Ctrl+C to interrupt[/white]")
        console.print("[white]Type 'clear' to clear conversation history[/white]")
        console.print()
        
        while True:
            try:
                question = Prompt.ask("\n[bold pink]Ask[/bold pink]")
                
                if question.lower() in ["exit", "quit"]:
                    console.print("\n[yellow]Goodbye![/yellow]")
                    break
                
                if question.lower() == "clear":
                    self.core.clear_memory()
                    console.print("\n[yellow]Conversation history cleared![/yellow]")
                    continue
                
                if not question.strip():
                    continue
                
                console.print()
                self.process_question_streaming(question)
                console.print(f"\n[dim]{'─' * console.width}[/dim]")
            except KeyboardInterrupt:
                console.print("\n[yellow]Interrupted by user[/yellow]")
                continue
            except Exception as e:
                console.print(f"\n[bold red]Error:[/bold red] {str(e)}")


def main():
    """Main entry point for the colorful CLI."""
    parser = argparse.ArgumentParser(description="Bobby CLI - A command line interface for the Bobby agent")
    parser.add_argument('--db-path', type=str, required=True, help='Path to the SQLite database')
    parser.add_argument('--interactive', action='store_true', help='Run in interactive mode')
    parser.add_argument('--question', type=str, help='Question to answer (if not in interactive mode)')
    
    args = parser.parse_args()

    # Check for Anthropic API key
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        from rich.prompt import Prompt
        api_key = Prompt.ask("[bold yellow]Enter your Anthropic API key[/bold yellow]")
        os.environ["ANTHROPIC_API_KEY"] = api_key  # Set for current process

    try:
        cli = BobbyCLI(args.db_path)
        # Optionally, you could pass api_key to BobbyCore if you want to support explicit key passing
        # cli = BobbyCLI(args.db_path, api_key=api_key)
        
        if args.interactive:
            cli.run_interactive_mode()
        elif args.question:
            cli.process_question_streaming(args.question)
        else:
            parser.print_help()
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())