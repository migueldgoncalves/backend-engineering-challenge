from typing import Union
import queue
from datetime import datetime, timedelta
import json
import threading

from command_line_parser import sample_command_line_parser
from input_reader import sample_input_reader
from output_generator import sample_output_generator
from utils import constants

"""
This class processes incoming translation events arriving from the Input Reader package, and forwards the processed information 
    to the Output Generator package
All business logic related to translation event processing should be added here
"""

class EventProcessor:
    CHRONOLOGY_DATE_KEY = 'date'
    CHRONOLOGY_NUMBER_OF_EVENTS_KEY = 'number_of_events'
    CHRONOLOGY_TOTAL_DELIVERY_TIME_KEY = 'total_delivery_time'

    # Max digits of outputted translation delivery times. This allows to increase clarity by reducing the number of displayed digits. This number was not provided by Unbabel, instead it is defined here
    AVERAGE_DELIVERY_TIME_MAX_DIGITS = 3

    @staticmethod
    def start(args: list[str]) -> None:
        """
        Main routine to invoke
        Incoming command-line args are processed, to extract the input file path and the window size in minutes
        The Input Reader and the Output Generator are then started in dedicated threads
        Then, this class processes incoming translation events, forwarding the processed information, until receiving an indication that
            there are no more events to process
        :param args: The command-line args as received by this application
        :return:
        """
        # Gets startup parameters
        command_line_parser = sample_command_line_parser.SampleCommandLineParser()
        input_file_path, window_size = command_line_parser.process_command_line_args(args)  # Fast operation - Can be synchronous

        # Creates the requires queues
        incoming_queue = queue.Queue()
        outgoing_queue = queue.Queue()

        # Initializes the Input Reader and Output Generator threads
        # These will terminate on their own when all translation events have been processed
        input_reader = sample_input_reader.SampleInputReader()
        output_generator = sample_output_generator.SampleOutputGenerator()
        threading.Thread(target=input_reader.process_input_stream, args=(incoming_queue, input_file_path)).start()
        threading.Thread(target=output_generator.process_incoming_information, args=(outgoing_queue,)).start()

        # Starts processing events - When the routine exits, all translation events have been processed and the main thread may exit
        EventProcessor.event_processor(incoming_queue, outgoing_queue, window_size)
        exit(0)  # Success

    @staticmethod
    def event_processor(incoming_queue: queue.Queue, outgoing_queue: queue.Queue, window_size: int) -> None:
        """
        Major routine - Processes translation events, generating and forwarding the aggregated output for each minute of information
            Exits when receiving a marker stating that all translation events have been read
        This is the core of the application - It calculates, for every minute, a moving average of the translation delivery time for the last window_size minutes
        :param incoming_queue: The queue from which raw translation events should be read
        :param outgoing_queue: The queue in which generated pieces of information should be put
        :param window_size: Requested window size, in minutes. For each minute, the previous window_size minutes will be analysed
        :return:
        """
        def on_first_event(current_chronology: list[dict[str, Union[datetime, int]]], next_minute: datetime, delivery_time: int, target_queue: queue.Queue):
            # Inserts the first minute in the chronology, the timestamp of the incoming translation event rounded to the next minute
            minute_info: dict[str, Union[datetime, int]] = {
                EventProcessor.CHRONOLOGY_DATE_KEY: next_minute,
                EventProcessor.CHRONOLOGY_NUMBER_OF_EVENTS_KEY: 1,
                EventProcessor.CHRONOLOGY_TOTAL_DELIVERY_TIME_KEY: delivery_time
            }
            current_chronology.insert(0, minute_info)  # Chronology order is from latest to earliest minute

            # First minute to output is the full minute before the creation of the first translation event - This is an empty minute as it has no translation events
            timestamp_previous_minute: datetime = next_minute - timedelta(minutes=1)  # Ex: 12:33:00 -> 12:32:00, for a first event created at 12:32:05
            EventProcessor._notify_new_information(target_queue, datetime_object=timestamp_previous_minute, average_delivery_time=0)  # Can be outputted immediately

        def on_event_for_same_minute(current_chronology: list[dict[str, Union[datetime, int]]], delivery_time: int):
            # Current minute is stored at index 0 of chronology - Once closed, it will regularly be pushed to higher indexes
            number_of_events: int = current_chronology[0].get(EventProcessor.CHRONOLOGY_NUMBER_OF_EVENTS_KEY)
            total_delivery_time: int = current_chronology[0].get(EventProcessor.CHRONOLOGY_TOTAL_DELIVERY_TIME_KEY)

            number_of_events += 1
            total_delivery_time += delivery_time

            # Update info for current minute with values for incoming translation event
            current_chronology[0][EventProcessor.CHRONOLOGY_NUMBER_OF_EVENTS_KEY] = number_of_events
            current_chronology[0][EventProcessor.CHRONOLOGY_TOTAL_DELIVERY_TIME_KEY] = total_delivery_time

            # At this instant, current minute is still open as new events can belong to the same minute as this event. Therefore, there is no new information to notify

        def on_event_for_new_consecutive_minute(current_chronology: list[dict[str, Union[datetime, int]]], next_minute: datetime, delivery_time: int, window: int, target_queue: queue.Queue):
            # The current minute is now closed, with the arrival of a translation event that occurred in the next minute
            # The info for the current minute should therefore be outputted
            current_minute = current_chronology[0].get(EventProcessor.CHRONOLOGY_DATE_KEY)  # The current minute until the incoming event arrived
            average_delivery_time: float = EventProcessor._get_average_delivery_time_from_chronology(current_chronology, window)
            EventProcessor._notify_new_information(target_queue, datetime_object=current_minute, average_delivery_time=average_delivery_time)

            # The incoming translation event should open a new minute in the chronology
            minute_info: dict[str, Union[datetime, int]] = {
                EventProcessor.CHRONOLOGY_DATE_KEY: next_minute,
                EventProcessor.CHRONOLOGY_NUMBER_OF_EVENTS_KEY: 1,
                EventProcessor.CHRONOLOGY_TOTAL_DELIVERY_TIME_KEY: delivery_time
            }
            current_chronology.insert(0, minute_info)  # Previous current minute becomes the second most recent minute in the chronology

            # If chronology has unnecessary info, trim it
            if len(current_chronology) > window_size:
                current_chronology.pop(-1)

        def on_event_for_new_non_consecutive_minute(current_chronology: list[dict[str, Union[datetime, int]]], next_minute: datetime, delivery_time: int, window: int, target_queue: queue.Queue):
            # The current minute is now closed, with the arrival of a translation event that occurred in a minute after the next minute
            # The info for the current minute should therefore be outputted
            current_minute = current_chronology[0].get(EventProcessor.CHRONOLOGY_DATE_KEY)  # The current minute until the incoming event arrived
            average_delivery_time: float = EventProcessor._get_average_delivery_time_from_chronology(current_chronology, window)
            EventProcessor._notify_new_information(target_queue, datetime_object=current_minute, average_delivery_time=average_delivery_time)

            # Handles the empty minutes between the minute of the current event and the minute of the incoming event
            # Ex: If current event occurred at 12:34:56 and incoming event occurred at 12:36:56, minute 12:36:00 (12:35:00.000001 -> 12:36:00) is empty

            minute_difference_timedelta: timedelta = next_minute - current_minute  # Ex: 12:37:00 - 12:35:00 = 00:02:00
            minute_difference: int = minute_difference_timedelta.seconds // 60  # timedelta objects allow to get seconds, from which minutes can be deducted

            for i in range(1, minute_difference):  # A difference of X minutes leads to X-1 empty minutes (in the example above, 2 minutes of difference leads to 1 empty minute)
                # Adds a new empty minute in the chronology as the current minute
                new_minute: datetime = current_minute + timedelta(minutes=i)  # In this for-loop, i starts at 1
                minute_info: dict[str, Union[datetime, int]] = {
                    EventProcessor.CHRONOLOGY_DATE_KEY: new_minute,
                    EventProcessor.CHRONOLOGY_NUMBER_OF_EVENTS_KEY: 0,
                    EventProcessor.CHRONOLOGY_TOTAL_DELIVERY_TIME_KEY: 0
                }
                current_chronology.insert(0, minute_info)  # Previous current minute becomes the second most recent minute in the chronology

                # Notifies a new piece of information taking into account the new empty minute
                average_delivery_time = EventProcessor._get_average_delivery_time_from_chronology(current_chronology, window)
                EventProcessor._notify_new_information(target_queue, datetime_object=new_minute, average_delivery_time=average_delivery_time)

                # If chronology has unnecessary info, trim it
                if len(current_chronology) > window_size:
                    current_chronology.pop(-1)

            # At this point, all empty minutes have been added to the chronology (and maybe even removed), and the output has been updated accordingly
            # The incoming translation event may now open a new minute in the chronology with an associated event
            minute_info = {
                EventProcessor.CHRONOLOGY_DATE_KEY: next_minute,
                EventProcessor.CHRONOLOGY_NUMBER_OF_EVENTS_KEY: 1,
                EventProcessor.CHRONOLOGY_TOTAL_DELIVERY_TIME_KEY: delivery_time
            }
            current_chronology.insert(0, minute_info)  # Previous current minute becomes the second most recent minute in the chronology

            # If chronology has unnecessary info, trim it
            if len(current_chronology) > window_size:
                current_chronology.pop(-1)

        # Stores the information regarding the incoming translation events, from latest to earliest minute
        # The provided window size, in minutes, determines how long this chronology is. Ex: A window size of 10 will lead to a max size of 11 (eleven) elements in the list
        # Each element has the information for one full minute. They will have the following format:
        #   {'date': datetime.datetime(2020, 1, 2, 13, 24), 'number_of_events': 232, 'total_delivery_time': 4561}
        # The current minute is always the first entry of the list. Once an event is received for another minute (leveraging on the
        #   assumption that all events arrive in chronological order, from first to last), the new minute is inserted at the front of the list
        # Empty minutes are inserted as needed, if no events appear for them (ex: if one event arrives 10 minutes after the previous event)
        # Older elements that are no longer needed should be removed manually
        chronology: list[dict[str, Union[datetime, int]]] = []

        while True:
            raw_event: str = EventProcessor._get_translation_event_from_incoming_queue(incoming_queue)
            if raw_event == constants.END_OF_FILE_MARKER:  # All translation events have been read
                break

            event_timestamp, event_delivery_time = EventProcessor._get_parameters_from_raw_translation_event(raw_event)

            # The minute that will be associated with this event - The event timestamp, rounded to the next minute
            # No overlaps are allowed - An event always belongs to one and only one minute
            # Ex: datetime.datetime(2020, 1, 2, 13, 23, 59, 999999) -> datetime.datetime(2020, 1, 2, 13, 24)
            # Ex: datetime.datetime(2020, 1, 2, 13, 24, 00, 0) -> datetime.datetime(2020, 1, 2, 13, 24) - Edge case, associated minute will be an exact copy of the event timestamp
            # Ex: datetime.datetime(2020, 1, 2, 13, 24, 00, 1) -> datetime.datetime(2020, 1, 2, 13, 25)
            timestamp_next_minute: datetime = EventProcessor._get_next_minute_of_datetime(event_timestamp)

            # How should the new incoming event be processed?
            # 4 possible scenarios:
            #   - Event is the first in the input file
            #   - Event belongs to the same minute as other previous event(s)
            #       - Ex: timestamp of incoming event is 12:34:56, and timestamp of previous event is 12:34:01
            #   - Event is the first to be created in its minute AND previous event was created in the previous minute
            #       - Ex: incoming event was created at 12:34:56, and previous event was created at 12:33:56
            #   - Event is the first to be created in its minute AND previous event was created before the previous minute
            #       - At least a full minute (ex: 12:33:00 -> 12:34:00) has passed between the creation of the two events
            #       - Ex: incoming event was created at 12:34:56, and previous event was created at 12:32:56
            #           - In this case, minute 12:34:00 has no associated events

            if not chronology:  # Event is the first in the input file
                on_first_event(chronology, timestamp_next_minute, event_delivery_time, outgoing_queue)

            elif timestamp_next_minute == chronology[0].get(EventProcessor.CHRONOLOGY_DATE_KEY):  # Event belongs to the same minute as other previous event(s)
                on_event_for_same_minute(chronology, event_delivery_time)

            elif timestamp_next_minute - chronology[0].get(EventProcessor.CHRONOLOGY_DATE_KEY) == timedelta(minutes=1):  # Event is the first to be created in its minute AND previous event was created in the previous minute
                on_event_for_new_consecutive_minute(chronology, timestamp_next_minute, event_delivery_time, window_size, outgoing_queue)

            elif timestamp_next_minute - chronology[0].get(EventProcessor.CHRONOLOGY_DATE_KEY) > timedelta(minutes=1):  # Event is the first to be created in its minute AND previous event was created before the previous minute
                on_event_for_new_non_consecutive_minute(chronology, timestamp_next_minute, event_delivery_time, window_size, outgoing_queue)

        # All translation events have been read - Notify information for current minute (as it is now closed), and notify end of information to report
        final_average_delivery_time: float = EventProcessor._get_average_delivery_time_from_chronology(chronology, window_size)
        final_current_minute: datetime = chronology[0].get(EventProcessor.CHRONOLOGY_DATE_KEY)
        EventProcessor._notify_new_information(outgoing_queue, datetime_object=final_current_minute, average_delivery_time=final_average_delivery_time)
        EventProcessor._notify_end_of_information(outgoing_queue)  # Will cause Output Generator thread to exit on its own
        return

    @staticmethod
    def _get_average_delivery_time_from_chronology(chronology: list[dict[str, Union[datetime, int]]], window_size: int) -> float:
        """
        Given the chronology object with the processed information regarding incoming events, and the desired window size,
            returns the average translation delivery time for the last window_size minutes
        Time interval is [latest full minute in chronology - 9:59.999999; latest full minute in chronology]
        :param chronology: The chronology maintained by the event_processor() routine of this class. Full description available at the routine docstring. It is assumed that it has no gaps, that is, that it has all minutes between start and end
        :param window_size: Requested window size, in minutes. This value determines how many minutes of information will be analysed
        :return: The average delivery time of all translations in the past window_size minutes
        """
        def _get_average_delivery_time(number_of_events: int, delivery_time: int) -> float:
            return delivery_time / number_of_events

        if not chronology:
            return 0  # Nothing to analyse

        total_number_of_events: int = 0
        total_delivery_time: int = 0

        for i in range(window_size):
            # If the window size is larger than the chronology
            if i >= len(chronology):
                return _get_average_delivery_time(total_number_of_events, total_delivery_time)  # All information has been processed - Average delivery time can be computed and returned

            minute_number_of_events: int = chronology[i].get(EventProcessor.CHRONOLOGY_NUMBER_OF_EVENTS_KEY, 0)
            minute_delivery_time: int = chronology[i].get(EventProcessor.CHRONOLOGY_TOTAL_DELIVERY_TIME_KEY, 0)

            total_number_of_events += minute_number_of_events
            total_delivery_time += minute_delivery_time

        # If the chronology is larger than the window size
        return _get_average_delivery_time(total_number_of_events, total_delivery_time)  # All information has been processed for the desired minutes - Average delivery time can be computed and returned

    @staticmethod
    def _get_parameters_from_raw_translation_event(raw_translation_event: str) -> tuple[datetime, int]:
        """
        Given a raw translation event (full example can be found in SampleInputReader class), returns the relevant parameters
        :param raw_translation_event: The raw translation event. A single type of event is expected: translation_delivered
        :return: The event timestamp as a datetime object, and the duration/delivery time in ms as an integer
        """
        # It is assumed that:
        #   1 - Provided raw translations events are correct (i.e. not empty, all expected fields are present, correct values)
        #   2 - There is a single event type: translation_delivered

        translation_event_dict: dict[str, Union[str, int]] = json.loads(raw_translation_event)
        timestamp_key = 'timestamp'  # Event timestamp, in ISO format. Can be read by Python datetime library. Ex: "2018-12-26 18:12:19.903159"
        delivery_time_key = 'duration'  # Translation delivery time in ms

        event_timestamp: datetime = datetime.fromisoformat(translation_event_dict.get(timestamp_key))
        event_delivery_time: int = translation_event_dict.get(delivery_time_key)

        return event_timestamp, event_delivery_time

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
        if EventProcessor._is_integer(average_delivery_time):  # Ex: 45.0
            average_delivery_time = int(average_delivery_time)  # Ex: 45.0 -> 45
        else:  # Ex: 45.5
            average_delivery_time = round(average_delivery_time, EventProcessor.AVERAGE_DELIVERY_TIME_MAX_DIGITS)  # Increases clarity. Ex: 45.55555555 -> 45.555

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
