import os, math, settings, sys, time
from map_reduce import *
from math import ceil
from multiprocessing import Process, JoinableQueue
from utils import chunked
import signal, numpy as np
from collections import deque
from multiprocessing import Manager
from time import sleep
import random

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
        self.map_st_time=self.manager.dict()
        self.map_end_time=self.manager.dict()
        self.sort_st_time=self.manager.dict()
        self.sort_end_time=self.manager.dict()
        self.reduce_st_time=self.manager.dict()
        self.reduce_end_time=self.manager.dict()
        self.map_duration=0
        self.reduce_duration=0

    """
    Split input file into no of mappers
    """
    def split_input_files(self):
        try:
            self.file_path = self.input_dir + "/" + self.file_name
            self.file_size = os.stat(self.file_path)
        except FileNotFoundError:
            print("File {} does not exist. Aborting".format(self.file_name))
            sys.exit(1)

        # Read the file contents
        contents = []
        n_lines = 0
        with open(self.file_path, 'r') as f:
            contents = f.readlines()

        n_lines = len(contents)

        R = np.random.RandomState(11) # To ensure consistent results, ideally random state should not be set
        s = []
        prev = 0
        for m in range(self.n_mappers - 1):
            s.append(R.randint(prev, n_lines - 1 + m -self.n_mappers + 1))
            prev = s[-1]

        s.append(n_lines)
        print('Distribution of files: ', s)

        start = 0
        for mapper, lines in enumerate(s):
            file_path = self.input_dir + "/" + str(mapper) + ".txt"
            file = open(file_path, 'w')
            file.writelines(contents[start:lines])
            start = lines
            file.close()

    def launch_mappers(self, setting):
        ## Using manager, as it manager queue in a separate process and it's not flused 
        ## after the child process exits
        with Manager() as manager:
            queue = manager.Queue()

            map_st_time=manager.dict()
            map_end_time=manager.dict()
            reduce_st_time=manager.dict()
            reduce_end_time=manager.dict()
            sort_st_time=self.manager.dict()
            sort_end_time=self.manager.dict()

            processes = []
            start_time = time.time()
            # Start all the mappers in parallel
            for i in range(self.n_mappers):
                file_path = self.input_dir + "/" + str(i) + ".txt"
                p = Process(target=self.mapper, args=(file_path, map, queue,  map_st_time,map_end_time))
                p.start()
                processes.append(p)

            output = manager.Queue()

            if setting == 'Lazy':
                for p in processes:
                    if p.is_alive():
                        exitcode = p.join()

            if setting == 'Nimble':
                delay = 2.3
                time.sleep(delay)
            
            p = Process(target=self.sort, args=[queue, output, sort_st_time, sort_end_time])
            p.start()
            processes.append(p)
            
            ## join all the processes so that main process does not exit before child completes
            for p in processes:
                if p.is_alive():
                    exitcode = p.join()
                        
            processes = []
            self.combined = output.get()

            # Now here we start the reducers
            i = 0
            for chunk in chunked(self.combined.items(), ceil(len(self.combined) / self.n_reducers)):
                file_path = self.output_dir + "/" + str(i) + ".txt"
                i += 1
                p = Process(target=self.reducer, args=(file_path, chunk, reduce_st_time,reduce_end_time))
                p.start()
                processes.append(p)

            for p in processes:
                p.join()

            end_time = time.time()
            duration1, duration2, duration3 = 0 , 0 ,0
            cost_map,cost_sort,cost_reduce=0,0,0
            ########Printing#########
            print('Setting: ', setting, '\n')
            print("================Mapper Information====================")
            for k,v in map_st_time.items():
                duration1=map_end_time[k]-v
                print("Process: ",k,"start time ",v, "end time ",map_end_time[k], "duration",duration1)
                cost_map+=duration1
            print("================Sorting Information====================")
            for k,v in sort_st_time.items():
                duration2=sort_end_time[k]-v
                print("Process: ",k,"start time ",v, "end time ",sort_end_time[k], "duration",duration2)
                cost_sort+=duration2
            print("================Mapper Information====================")
            for k,v in reduce_st_time.items():
                duration3=reduce_end_time[k]-v
                print("Process: ",k,"start time ",v, "end time ",reduce_end_time[k], "duration",duration3)
                cost_reduce+=duration3
            
            print("JCT Running time : " + str(end_time - start_time))
            print("Cost: "+str(cost_map+cost_sort+cost_reduce))
            print('\n\n\n')


    def mapper(self, file_path, map, q, map_st_time,map_end_time):
        # q.cancel_join_thread()
        map_st_time[os.getpid()]=time.time()
        contents = ''
        with open(file_path, 'r') as f:
            contents = f.readlines()

        for line in contents:
            map_output = map(line)
            for m in map_output:
                q.put(m)


        q.put(("DONE", 1))
        ## terminate the process here
        map_end_time[os.getpid()]=time.time()

    def sort(self, q, output, sort_st_time,sort_end_time):
        # q.cancel_join_thread()
        
        s = ("DONEE", 1)
        done_count = 0
        sort_st_time[os.getpid()]=time.time()
        while done_count < self.n_mappers:
            s = q.get(True)
            key, value = s
            if key in self.combined:
                self.combined[key].append(value)
            else:
                self.combined[key] = [value]

            if s[0] == "DONE":
                done_count += 1
        ## delete the "DONE" key
        self.combined.pop('DONE', None)
        output.put(self.combined)
        sort_end_time[os.getpid()]=time.time()

    def reducer(self, file_path, chunk, reduce_st_time,reduce_end_time):
        reduce_st_time[os.getpid()]=time.time()
        s = ""
        for c in chunk:
            word, count = reduce(c[0], c[1])
            s += word + ": " + str(count) + "\n"
        reduce_end_time[os.getpid()]=time.time()
        
        try:
            f = open(file_path, "w+")
            f.write(s)
            f.close()
        except Exception as e:
            print(e)
