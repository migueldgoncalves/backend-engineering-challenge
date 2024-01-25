from abc import ABC, abstractmethod
import queue

"""
Abstract class representing an input reader
This class is part of a simplified Factory design pattern
"""

class AbstractInputReader(ABC):
    @staticmethod
    @abstractmethod
    def process_input_stream(outgoing_queue: queue.Queue, input_file_path: str) -> None:
        raise NotImplemented
