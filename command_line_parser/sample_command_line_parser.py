import os

from command_line_parser import abstract_command_line_parser

"""
This class represents a sample command-line parser
It is part of a simplified Factory design pattern
"""

class SampleCommandLineParser(abstract_command_line_parser.AbstractCommandLineParser):
    @staticmethod
    def process_command_line_args(args: list[str]) -> tuple[str, int]:
        """
        Processes command-line args and returns the relevant elements: the input file path and the window size in minutes
        :param args: The command-line args as received by this application. Ex: ["unbabel_cli", "--input_file", "events.json", "--window_size", "10"]
        :return: Tuple with the path of the input file, and the requested window size. Raises error if args are invalid
        """
        # Indexes of the relevant args
        script_path_index = 0
        # Argument in index 1 is the parameter name for the input file path - Not needed
        input_file_path_index = 2
        # Argument in index 3 is the parameter name for the window size - Not needed
        window_size_index = 4

        # Have enough args been provided?
        expected_args_number = 5
        if len(args) < expected_args_number:
            raise Exception("Not enough args provided")

        # The script that starts this application is in the root folder. Ex: "D:\PycharmProjects\translation-event-processor\translation_event_processor.py" -> "D:\PycharmProjects\translation-event-processor"
        base_path = os.path.split(args[script_path_index])[0]

        # Has the path for the input file been provided?
        input_file_path: str = args[input_file_path_index]
        if not input_file_path:
            raise Exception("Empty input file path provided")

        # Is the path for the input file a relative path? If so, convert it to an absolute path
        if not os.path.isfile(input_file_path):
            # Ex: "D:\PycharmProjects\translation-event-processor" + "translation_event_processor.py" = "D:\PycharmProjects\translation-event-processor\translation_event_processor.py"
            input_file_path = os.path.join(base_path, input_file_path)

        # Is the absolute path valid?
        if not os.path.isfile(input_file_path):
            raise Exception(f"Input file provided does not exist: {input_file_path}")

        # At this point, input_file_path is known to be valid
        # Proceeding to validate window size

        # Is the window size an integer?
        try:
            window_size: int = int(args[window_size_index])
        except:  # Error - Not an integer
            raise Exception(f"Window size provided is not an integer: {args[window_size_index]}")

        # Is the window size a positive integer
        if window_size <= 0:
            raise Exception(f"Window size provided is too small: {window_size}")

        # At this point, window size is also known to be valid
        # Both values can now be returned

        return input_file_path, window_size
