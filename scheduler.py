import os, math, settings, sys, time
from map_reduce import *
from math import ceil
from multiprocessing import Process, Queue
from utils import chunked

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
        self.queue = Queue()
        self.combined = {}

    """
    Split input file into no of mappers
    """
    def split_input_files(self):
        try:
            self.file_path = self.input_dir + "/" + self.file_name
            self.file_size = os.stat(self.file_path)
        except FileNotFoundError:
            print(f"File {self.file_name} does not exist. Aborting")
            sys.exit(1)

        # Read the file contents
        contents = ""
        n_lines = 0
        with open(self.file_path, 'r') as f:
            contents = f.readlines()

        # Split the file into n_mappers 
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

        # Close all file descriptors
        for mapper in range(self.n_mappers):
            file_descriptors[mapper].close()

    def launch_mappers(self):
        processes = []
        
        # Start all the mappers in parallel
        for i in range(self.n_mappers):
            file_path = self.input_dir + "/" + str(i) + ".txt"
            p = Process(target=self.mapper, args=(file_path, map, self.queue))
            p.start()
            processes.append(p)

        # Next we perform sorting (this is the eager setting)
        # For the lazy setting, we need to join all the mappers
        # in the list `processes` (this means that sorting
        # will have to wait until all mappers are done)
        output = Queue()
        p = Process(target=self.sort, args = [self.queue, output])
        p.start()
        processes.append(p)

        for p in processes:
            p.join()
        processes = []

        self.combined = output.get()
        print(len(self.combined))
        print()
        
        # Now here we start the reducers
        i = 0
        for chunk in chunked(self.combined.items(), ceil(len(self.combined) / self.n_reducers)):
            print(len(chunk))
            file_path = self.output_dir + "/" + str(i) + ".txt"
            i += 1
            p = Process(target=self.reducer, args=(file_path, chunk))
            p.start()
            processes.append(p)

        for p in processes:
            p.join()

    def mapper(self, file_path, map, q):
        contents = ''
        
        with open(file_path, 'r') as f:
            contents = f.readlines()

        for line in contents:
            map_output = map(line)
            for m in map_output:
                q.put(m)

        q.put(('EOF', 1))

    def sort(self, q, output):
        try:
            while True:
                s = q.get()
                if not s or s[0] == 'EOF':
                    break

                key, value = s
                if key in self.combined:
                    self.combined[key].append(value)
                else:
                    self.combined[key] = [value]
                
        except EOFError:
            pass

        output.put(self.combined)

    def reducer(self, file_path, chunk):
        s = ""
        for c in chunk:
            word, count = reduce(c[0], c[1])
            s += word + ": " + str(count) + "\n"
        
        try:
            f = open(file_path, "w+")
            f.write(s)
            f.close()
        except Exception as e:
            print(e)