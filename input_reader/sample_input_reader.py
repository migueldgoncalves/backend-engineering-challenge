import queue

from input_reader import abstract_input_reader
from utils import constants

"""
This class represents a sample input reader
It is part of a simplified Factory design pattern
"""

class SampleInputReader(abstract_input_reader.AbstractInputReader):
    @staticmethod
    def process_input_stream(outgoing_queue: queue.Queue, input_file_path: str) -> None:
        """
        Processes the input file, line by line, and places the extracted content in a thread-safe queue for asynchronous processing
        Once it detects the end of the file, this routine places a special marker in the queue, and exits
        This routine should be run in a thread separated from the main thread
        :param outgoing_queue: The queue in which the file content should be placed
        :param input_file_path: The path of the file to analyse. It is assumed that this path has been validated elsewhere
        :return:
        """
        try:
            mode = 'r'  # The goal is to read from the file while keeping it intact. There is nothing to execute inside it
            with open(input_file_path, mode) as f:

                # It is assumed that each line contains a JSON representing a translation event, and only that JSON
                #   That is, the file itself does not contain a single JSON, but one JSON per line
                # It is also assumed that all events arrive by chronological order, from oldest to newest
                # Ex: {
                #       "timestamp": "2018-12-26 18:11:08.509654",
                #       "translation_id": "5aa5b2f39f7254a75aa5",
                #       "source_language": "en",
                #       "target_language": "fr",
                #       "client_name": "airliberty",
                #       "event_name": "translation_delivered",
                #       "nr_words": 30,
                #       "duration": 20
                #     }

                # Keep reading from the input file until the end
                while True:
                    translation_event: str = f.readline()
                    if not translation_event:  # No empty lines are expected. If translation_event is '', EOF has been reached
                        SampleInputReader._notify_all_events_read(outgoing_queue)
                        return  # Nothing else to do
                    else:  # There is a new translation event to report
                        SampleInputReader._notify_new_event(outgoing_queue, SampleInputReader._parse_event(translation_event))

        except:
            print("An error has occurred while reading translation events from the output file. Input Reader will exit")
            return

    @staticmethod
    def _notify_new_event(outgoing_queue: queue.Queue, new_event: str) -> None:
        """
        Notifies a new translation event to process
        :param outgoing_queue: The queue in which translation events should be put
        :param new_event: The new translation event that has been read
        :return:
        """
        outgoing_queue.put(new_event)

    @staticmethod
    def _notify_all_events_read(outgoing_queue: queue.Queue) -> None:
        """
        Notifies that all events have been read from the input file, by adding a special marker to the outgoing queue
        :param outgoing_queue: The queue in which translation events must be put
        :return:
        """
        outgoing_queue.put(constants.END_OF_FILE_MARKER)

    @staticmethod
    def _parse_event(new_raw_event: str) -> str:
        """
        Parses an incoming raw translation event. It is assumed that the provided string is not null and not empty
        :param new_raw_event: A raw event read from the input file
        :return: The parsed translation event
        """
        if new_raw_event[-1] == '\n':  # If the raw line content has a line break, remove it
            processed_event: str = new_raw_event[:-1]
        else:  # If there is no line break (ex: last line of the file), return raw event as is
            processed_event: str = new_raw_event
        return processed_event
