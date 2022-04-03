import os
import math
import sys
import json
import settings
from multiprocessing import Process

class Scheduler:
    """"
    Initialize scheduler with default paramerters
    """
    def __init__(self, n_mappers=settings.default_n_mappers, n_reducers=settings.default_n_reducers,
    input_dir="input_files",output_dir="output_files", file_name=None):
        self.n_mappers = n_mappers
        self.n_reducers = n_reducers
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.file_name = file_name

    """
    Split input file into no of mappers
    """
    def split_input_files(self):
        try:
            self.file_path = self.input_dir + "/" + self.file_name
            self.file_size = os.stat(self.file_path)
            # self.n_file_split = math.floor(self.file_size / self.n_mappers) 
            print("file_size: ", self.file_size.st_size)

        except FileNotFoundError:
            print(f"File {self.file_name} does not exist.  Aborting")
            sys.exit(1)

        ## read the file contents
        print("path" , self.file_path)
        ## print the file contents
        with open(self.file_path, 'r') as f:
            contents = f.readlines()
            for line in contents:
                print(line)

        
        
