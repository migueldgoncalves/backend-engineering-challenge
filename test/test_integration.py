import unittest
import os
import time

from utils import constants
from core import event_processor


class TestIntegration(unittest.TestCase):

    # The input file provided by Unbabel
    UNIT_TEST_1_INPUT = \
        '{"timestamp": "2018-12-26 18:11:08.509654","translation_id": "5aa5b2f39f7254a75aa5","source_language": "en","target_language": "fr","client_name": "airliberty","event_name": "translation_delivered","nr_words": 30, "duration": 20}\n' \
        '{"timestamp": "2018-12-26 18:15:19.903159","translation_id": "5aa5b2f39f7254a75aa4","source_language": "en","target_language": "fr","client_name": "airliberty","event_name": "translation_delivered","nr_words": 30, "duration": 31}\n' \
        '{"timestamp": "2018-12-26 18:23:19.903159","translation_id": "5aa5b2f39f7254a75bb3","source_language": "en","target_language": "fr","client_name": "taxi-eats","event_name": "translation_delivered","nr_words": 100, "duration": 54}'
    UNIT_TEST_1_FILENAME = 'events.json'

    BASE_FOLDER = os.getcwd()
    OUTPUT_FILEPATH = os.path.join(BASE_FOLDER, constants.OUTPUT_FILENAME)
    ALL_INPUTS = [UNIT_TEST_1_INPUT]
    ALL_FILENAMES = [UNIT_TEST_1_FILENAME]

    WAIT_FOR_APP_TO_EXIT = 0.5  # Seconds. After starting the application, wait this amount of time before checking output file, to allow app to exit

    @classmethod
    def setUpClass(cls):
        # Writes input files to use in the tests
        for i in range(len(TestIntegration.ALL_INPUTS)):
            file_content: str = TestIntegration.ALL_INPUTS[i]
            file_name: str = TestIntegration.ALL_FILENAMES[i]
            file_path: str = os.path.join(TestIntegration.BASE_FOLDER, file_name)

            mode = 'w'
            with open(file_path, mode) as f:
                f.writelines(file_content)  # Single string, already with line separators

    def test_unbabel(self):
        """
        Unit Test 1
        Provided by Unbabel
        """
        args = [
            "unbabel_cli", "--input_file", "events.json", "--window_size", "10"
        ]
        event_processor.EventProcessor.start(args)
        time.sleep(TestIntegration.WAIT_FOR_APP_TO_EXIT)

        expected_output: str = \
            '{"date": "2018-12-26 18:11:00", "average_delivery_time": 0}\n' \
            '{"date": "2018-12-26 18:12:00", "average_delivery_time": 20}\n' \
            '{"date": "2018-12-26 18:13:00", "average_delivery_time": 20}\n' \
            '{"date": "2018-12-26 18:14:00", "average_delivery_time": 20}\n' \
            '{"date": "2018-12-26 18:15:00", "average_delivery_time": 20}\n' \
            '{"date": "2018-12-26 18:16:00", "average_delivery_time": 25.5}\n' \
            '{"date": "2018-12-26 18:17:00", "average_delivery_time": 25.5}\n' \
            '{"date": "2018-12-26 18:18:00", "average_delivery_time": 25.5}\n' \
            '{"date": "2018-12-26 18:19:00", "average_delivery_time": 25.5}\n' \
            '{"date": "2018-12-26 18:20:00", "average_delivery_time": 25.5}\n' \
            '{"date": "2018-12-26 18:21:00", "average_delivery_time": 25.5}\n' \
            '{"date": "2018-12-26 18:22:00", "average_delivery_time": 31}\n' \
            '{"date": "2018-12-26 18:23:00", "average_delivery_time": 31}\n' \
            '{"date": "2018-12-26 18:24:00", "average_delivery_time": 42.5}'

        mode = 'r'
        with open(TestIntegration.OUTPUT_FILEPATH, mode) as f:
            actual_output: str = f.read()

        self.assertEqual(expected_output, actual_output)

    @classmethod
    def tearDownClass(cls):
        # Deletes output file, if it exists
        if os.path.isfile(TestIntegration.OUTPUT_FILEPATH):
            os.unlink(TestIntegration.OUTPUT_FILEPATH)

        # Deletes input files
        for i in range(len(TestIntegration.ALL_INPUTS)):
            input_filename: str = TestIntegration.ALL_FILENAMES[i]
            input_filepath: str = os.path.join(TestIntegration.BASE_FOLDER, input_filename)
            if os.path.isfile(input_filepath):
                os.unlink(input_filepath)
