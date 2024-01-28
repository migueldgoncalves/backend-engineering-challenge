import sys

from core import event_processor

"""
Welcome to the Translation Event Processor. This module is the entry point to this application.

Given a path to a file with translation events as JSON strings, and the desired window size in minutes, it calculates for every minute
    between the first and the last events a moving average of the translation delivery times for the past X minutes, where X is the requested window size.

Command-line: python3 translation_event_processor.py --input_file <file_path> --window_size <window_size_in_minutes>
    Example: python3 translation_event_processor.py --input_file events.json --window_size 10
"""

processor = event_processor.EventProcessor()
processor.start(sys.argv)
