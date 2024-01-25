from abc import ABC, abstractmethod
"""
Abstract class representing a command-line parser
This class is part of a simplified Factory design pattern
"""

class AbstractCommandLineParser(ABC):
    @staticmethod
    @abstractmethod
    def process_command_line_args(args: list[str]) -> tuple[str, int]:
        raise NotImplemented
