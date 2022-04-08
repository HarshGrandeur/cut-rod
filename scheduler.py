import os
import math
import sys
import json
import settings
from multiprocessing import Process, Pool
from multiprocessing.connection import Listener
from array import array
from map_reduce import *

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
            
        except FileNotFoundError:
            print(f"File {self.file_name} does not exist.  Aborting")
            sys.exit(1)

        ## read the file contents
        contents = ""
        n_lines = 0
        with open(self.file_path, 'r') as f:
            contents = f.readlines()

        ## split the file into n_mappers 
        for lines in contents:
            n_lines += 1

        n_lines_in_each_split = math.floor(n_lines / self.n_mappers)
        line_number = 0

        file_descriptors = [''] * self.n_mappers
        for mapper in range(self.n_mappers):
            file_path = self.input_dir + "/" + str(mapper) + ".txt"
            file =  open(file_path, 'w')
            file_descriptors[mapper] = file

        start = 0
        for line in contents:
            index = start % self.n_mappers
            file_descriptors[index].write(line)
            start += 1

        ## close all file descriptors
        for mapper in range(self.n_mappers):
            file_descriptors[mapper].close()

    def launch_mappers(self):
        for i in range(self.n_mappers):
            file_path = self.input_dir + "/" + str(i) + ".txt"
            p = Process(target=self.mapper, args=(file_path,map))
            p.start()
            p.join()
        

    def mapper(self, file_path, map):
        print('Inside mapper', os.getpid())
        address = ('localhost', settings.port)
        contents = ''
        with open(self.file_path, 'r') as f:
                contents = f.readlines()

        with Listener(address, authkey=b'secret password') as listener:
                with listener.accept() as conn:
                    for line in contents:
                        map_output = map(line)
                        conn.send(map_output)


                    

