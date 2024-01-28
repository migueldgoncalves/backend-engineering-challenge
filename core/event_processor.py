from typing import Union
import queue
from datetime import datetime, timedelta
import json
import threading

from command_line_parser import sample_command_line_parser
from input_reader import sample_input_reader
from utils import constants

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


    @staticmethod
    def _get_next_minute_of_datetime(datetime_object: datetime) -> datetime:
        """
        Calculates the next minute of a given datetime object
        Edge case: objects that represent the exact end/start of a minute (ex: datetime.datetime(2020, 1, 2, 13, 24, 00, 000000))
            In this case, the object is returned as is
        :param datetime_object: The datetime object to be rounded to the next minute
        :return: A datetime object representing the next minute. An edge case is described above
        """
        # If datetime object is already a rounded minute, return it as is
        if datetime_object.second == 0 and datetime_object.microsecond == 0:  # Ex: datetime.datetime(2020, 1, 2, 13, 24, 00, 000000)
            return datetime_object

        datetime_previous_minute: datetime = datetime(datetime_object.year, datetime_object.month, datetime_object.day, datetime_object.hour, datetime_object.minute)
        minute_timedelta: timedelta = timedelta(minutes=1)
        datetime_next_minute = datetime_previous_minute + minute_timedelta

        # Ex: datetime.datetime(2020, 1, 2, 13, 23, 59, 999999) -> datetime.datetime(2020, 1, 2, 13, 24)
        # Ex: datetime.datetime(2020, 1, 2, 13, 24, 00, 0) -> datetime.datetime(2020, 1, 2, 13, 24)
        # Ex: datetime.datetime(2020, 1, 2, 13, 24, 00, 1) -> datetime.datetime(2020, 1, 2, 13, 25)
        return datetime_next_minute

    @staticmethod
    def _notify_new_information(outgoing_queue: queue.Queue, datetime_object: datetime, average_delivery_time: float) -> None:
        """
        Given the required parameters, assembles a new piece of information and reports it by adding the resulting string
            to the outgoing queue
        :param outgoing_queue: The queue in which pieces of information must be put
        :param datetime_object: Timestamp of the piece of information. Example: datetime.datetime(2020, 1, 2, 13, 24)
        :param average_delivery_time: Being X the window size, it is the average delivery time of all translations in the last X minutes (i.e. [date-10; date] if window size == 10).
            Please note that the exact instant date-10 is not taken into account, while the instant date-9:59.999999 is
            This is done to simplify the management of the edge case in which an event arrives exactly at the end/start of a minute
        :return:
        """
        date_field = 'date'
        average_delivery_time_field = 'average_delivery_time'

        date_string = str(datetime_object)  # datetime.datetime(2020, 1, 2, 13, 24) -> '2020-01-02 13:24:00'. Output example provided by Unbabel excludes microseconds, so the same is done here
        # Average delivery time must be presented as 45 and not as 45.0 if it is an integer. Fractional numbers (ex: 45.5) must be presented as they are, without rounding
        if EventProcessor._is_integer(average_delivery_time):
            average_delivery_time = int(average_delivery_time)  # Ex: 45.0 -> 45

        information: dict[str, Union[str, int]] = {date_field: date_string, average_delivery_time_field: average_delivery_time}
        information_string: str = json.dumps(information)
        outgoing_queue.put(information_string)

    @staticmethod
    def _notify_end_of_information(outgoing_queue: queue.Queue) -> None:
        """
        Notifies that there are no more pieces of information to report, by adding a special marker to the outgoing queue
        :param outgoing_queue: The queue in which translation events must be put
        :return:
        """
        outgoing_queue.put(constants.END_OF_FILE_MARKER)

    @staticmethod
    def _get_translation_event_from_incoming_queue(incoming_queue: queue.Queue) -> str:
        """
        Reads a translation event from the queue, if available. If queue is empty, blocks until the queue has elements to extract
        It can be assumed that the Input Reader will keep sending elements to the queue in quick succession until reporting
            that all translation events have been read, therefore there is no risk of starvation
        :param incoming_queue: The queue to read from
        :return: A raw translation event, or a marker stating that all translation events have been read
        """
        raw_translation_event: str = incoming_queue.get(block=True, timeout=None)  # No timeout - Block until new event is available
        return raw_translation_event  # Can be == constants.END_OF_FILE_MARKER

    @staticmethod
    def _is_integer(number: Union[int, float]) -> bool:
        """
        Returns True if provided number is an integer (ex: 45), False if it is a fractional number (ex: 45.5)
        :param number: The number to analise
        :return: True if the provided number is an integer, False otherwise
        """
        if round(number) == number:
            return True
        else:
            return False
