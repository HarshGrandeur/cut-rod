import os
import json
import settings
from multiprocessing import Process

class FileHandler:

    def __init__(self, input_file_path, output_dir):
        """
        Note: the input file path should be given for splitting.
        The output directory is needed for joining the outputs.

        :param input_file_path: input file path
        :param output_dir: output directory path

        """
        self.input_file_path = input_file_path
        self.output_dir = output_dir

class Scheduler:
    """"
    Initialize scheduler with default paramerters
    """
    def __init__(self, n_mappers=settings.default_n_mappers, n_reducers=settings.default_n_reducers,
    input_dir="input_files",output_dir="output_files"):
        self.n_mappers = n_mappers
        self.n_reducers = n_reducers
        self.input_dir = input_dir
        self.output_dir = output_dir

    """
    Split input file into no of mappers
    """
    def split_input_files(self):
        
