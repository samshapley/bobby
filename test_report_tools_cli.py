#!/usr/bin/env python3
"""
Test script for Bobby's Report Tools integration with the CLI

This script tests the integration of the report tools in Bobby's CLI,
allowing creation and management of reports within the agent interface.
"""

import os
import argparse
import sqlite3
from datetime import datetime
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.prompt import Prompt
from rich.markdown import Markdown

from bobby_core import BobbyCore