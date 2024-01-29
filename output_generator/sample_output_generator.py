import queue
import os

from output_generator import abstract_output_generator
from utils import constants

"""
This class represents a sample output generator
It is part of a simplified Factory design pattern
"""


class SampleOutputGenerator(abstract_output_generator.AbstractOutputGenerator):
    @staticmethod
    def process_incoming_information(incoming_queue: queue.Queue) -> None:
        """
        Receives pieces of information encoded as strings, and writes them in the output file.
            This file will be located at the root folder of this application.
            Each piece of information contains an average of the translation delivery times for the last X minutes starting from
                the information timestamp, where X is a value chosen by the user in minutes
        Once this routine is notified that all information has been processed, it exits
        This routine should be run in a thread separated from the main thread
        :param incoming_queue: The queue from which pieces of information should be read
        :return:
        """
        try:
            # The output file will be located at the root folder of the application. Unlike for the input file, the output filename is fixed
            base_folder: str = os.getcwd()  # Ex: 'D:\\PycharmProjects\\translation-event-processor'
            output_filepath: str = os.path.join(base_folder, constants.OUTPUT_FILENAME)

            # If the output file already exists (likely created by a previous execution of the application), deletes it
            if os.path.isfile(output_filepath):
                os.unlink(output_filepath)

            mode = 'a'
            first_line: bool = True
            with open(output_filepath, mode) as f:
                while True:
                    information: str = SampleOutputGenerator._get_information_from_incoming_queue(incoming_queue)
                    if information == constants.END_OF_FILE_MARKER:
                        return  # All information has been written into the output file - Thread may exit

                    # As the information is being read from a file rather than from a network stream, processing it is very quick
                    # As such, it is preferable to open the output file only once, keep it open until all information has been written,
                    #   and only then close the file, rather than opening and closing the file for each incoming piece of information

                    # Ensures that file never ends with a '\n'. File line number will match number of receives pieces of information
                    if first_line:
                        f.writelines([information])
                        first_line = False
                    else:
                        f.writelines([f'\n{information}'])  # writelines() does not add line separators on its own

        except:
            print("An error has occurred while writing information into the output file. Output Generator will exit")
            return

    @staticmethod
    def _get_information_from_incoming_queue(incoming_queue: queue.Queue) -> str:
        """
        Reads a piece of information from the queue, if available. If queue is empty, blocks until the queue has elements to extract
        It can be assumed that the Event Processor will keep sending elements to the queue in quick succession until notifying
            that all information has been sent, therefore there is no risk of starvation
        :param incoming_queue: The queue to read from
        :return: A piece of information as a string, or a marker stating that all information has been sent
        """
        information: str = incoming_queue.get(block=True, timeout=None)  # No timeout - Block until new event is available
        return information  # Can be == constants.END_OF_FILE_MARKER
