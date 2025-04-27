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


class BobbyCLI:
    """Command Line Interface for the Bobby agent."""
    
    def __init__(self, db_path: str):
        """Initialize the CLI with database path."""
        self.core = BobbyCore(db_path)
        
# ... existing code ...
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
    
    def process_question(self, question: str):
        """Process a user question and display the analysis process."""
        from rich.align import Align

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

        # Panel to show after thinking is done
        def get_thought_panel(elapsed: float):
            msg = f"[bold white]Thought for {elapsed:.1f} seconds[/bold white]"
            return Panel(
                Align.center(Text.from_markup(msg), vertical="middle"),
                border_style="white",
                box=box.ROUNDED,
                height=3
            )

        # Run the animation while waiting for get_conversation_parts
        with Live(get_loader_panel(0, 0.0), refresh_per_second=10, console=console) as live:
            done = False
            result = [None]
            import threading

            def worker():
                result[0] = self.core.get_conversation_parts(question)

            t = threading.Thread(target=worker)
            t.start()
            frame = 0
            start_time = time.time()
            while t.is_alive():
                elapsed = time.time() - start_time
                live.update(get_loader_panel(frame, elapsed))
                time.sleep(0.1)
                frame += 1
            t.join()
            total_elapsed = time.time() - start_time
            conversation_parts = result[0]
            # Show "Thought in N.N seconds" for 1 second
            live.update(get_thought_panel(total_elapsed))
            time.sleep(1)

        messages = conversation_parts['messages']
        response = conversation_parts['response']
        system_prompt = conversation_parts['system_prompt']
        
        # Process the response
        while True:
            has_tool_calls = False
            
            for block in response.content:
                if block.type == "text":
                    self.display_agent_thought(block.text)
                elif block.type == "tool_use":
                    has_tool_calls = True
                    tool_name = block.name
                    tool_input = block.input
                    tool_id = block.id
                    
                    # Display the tool call
                    self.display_tool_call(tool_name, tool_input)
                    
                    # Execute the tool with a progress indicator
                    with console.status(f"[cyan]Executing {tool_name}...", spinner="arc"):
                        tool_result = self.core.process_tool_call(tool_name, tool_input)
                    
                    # Display results
                    self.display_results(tool_result)
                    
                    # Add to messages for next turn
                    messages.append({"role": "assistant", "content": response.content})
                    messages.append({
                        "role": "user",
                        "content": [{
                            "type": "tool_result",
                            "tool_use_id": tool_id,
                            "content": tool_result
                        }]
                    })
                    
                    # Get next response
                    with console.status("[bold white]Digesting results...", spinner="dots"):
                        response = self.core.process_next_turn(messages, response, system_prompt)
                    break
            
            if not has_tool_calls:
                # Add final response to memory
                if response.content:
                    final_response = {"role": "assistant", "content": response.content}
                    self.core.add_to_memory(final_response)
                break
    
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
                self.process_question(question)
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
    
    try:
        cli = BobbyCLI(args.db_path)
        
        if args.interactive:
            cli.run_interactive_mode()
        elif args.question:
            cli.process_question(args.question)
        else:
            parser.print_help()
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())