from abc import ABC, abstractmethod
import queue

"""
Abstract class representing an output generator
This class is part of a simplified Factory design pattern
"""

class AbstractOutputGenerator(ABC):
    @staticmethod
    @abstractmethod
    def process_incoming_information(incoming_queue: queue.Queue) -> None:
        raise NotImplemented
