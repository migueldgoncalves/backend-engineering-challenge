from command_line_parser import sample_command_line_parser

"""
This class processes incoming translation events arriving from the Input Reader package, and forwards the crunched information 
    to the Output Generator package
All business logic related to translation event processing should be added here
"""

class EventProcessor:
    @staticmethod
    def start(args: list[str]) -> None:
        """
        Main routine to invoke
        Incoming command-line args are processed, to extract the input file path and the window size in minutes
        The Input Reader and the Output Generator are then started in dedicated threads
        Then, this class processes incoming translation events, forwarding the crunched data, until receiving an indication that
            there are no more events to process
        :param args: The command-line args as received by this application
        :return:
        """
        command_line_parser = sample_command_line_parser.SampleCommandLineParser()
        input_file_path, window_size = command_line_parser.process_command_line_args(args)  # Fast operation - Can be synchronous
