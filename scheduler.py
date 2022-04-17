import os, math, settings, sys, time
from map_reduce import *
from math import ceil
from multiprocessing import Process, JoinableQueue
from utils import chunked
import signal
from collections import deque
from multiprocessing import Manager

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
        self.manager = Manager()
        self.queue = self.manager.Queue()
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
        ## Using manager, as it manager queue in a separate process and it's not flused 
        ## after the child process exits
        with Manager() as manager:
            queue = manager.Queue()
            processes = []
            start_time = time.time()
            # Start all the mappers in parallel
            for i in range(self.n_mappers):
                file_path = self.input_dir + "/" + str(i) + ".txt"
                p = Process(target=self.mapper, args=(file_path, map, queue))
                p.start()
                processes.append(p)

            # Next we perform sorting (this is the eager setting)
            # For the lazy setting, we need to join all the mappers
            # in the list `processes` (this means that sorting
            # will have to wait until all mappers are done)

            ## use manager manged queue
            output = manager.Queue()

            ## start the sorting process
            p = Process(target=self.sort, args = [queue, output])
            p.start()
            processes.append(p)

            ## join all the processes so that main process does not exit before child completes
            for p in processes:
                if p.is_alive():
                    exitcode = p.join()
                    # print("Skip join for process", p.pid, "exitcode", exitcode)
                        
            processes = []
            self.combined = output.get()

            # Now here we start the reducers
            i = 0
            for chunk in chunked(self.combined.items(), ceil(len(self.combined) / self.n_reducers)):
                file_path = self.output_dir + "/" + str(i) + ".txt"
                i += 1
                p = Process(target=self.reducer, args=(file_path, chunk))
                p.start()
                processes.append(p)

            for p in processes:
                p.join()

            end_time = time.time()
            print("Running time : " + str(end_time - start_time))

    def mapper(self, file_path, map, q):
        # q.cancel_join_thread()
        contents = ''
        with open(file_path, 'r') as f:
            contents = f.readlines()

        for line in contents:
            map_output = map(line)
            # print("Map output: ", map_output, "process", os.getpid())
            for m in map_output:
                q.put(m)

        q.put(("DONE", 1))
        # print("Size of the dq", q.qsize())
        ## terminate the process here
        # os.kill(os.getpid(), signal.SIGTERM)

    def sort(self, q, output):
        # q.cancel_join_thread()
        s = ("DONEE", 1)
        done_count = 0
        while done_count < self.n_mappers:
            s = q.get(True)
            key, value = s
            if key in self.combined:
                self.combined[key].append(value)
            else:
                self.combined[key] = [value]
            # print("Queue size :: ", q.qsize(), " is queue empty :: ", q.empty(), os.getpid(), s)
            if s[0] == "DONE":
                done_count += 1
        ## delete the "DONE" key
        self.combined.pop('DONE', None)
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