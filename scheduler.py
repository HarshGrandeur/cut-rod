import os, math, settings, sys, time
from map_reduce import *
from math import ceil
from multiprocessing import Process, JoinableQueue
from utils import chunked
import signal, numpy as np
from collections import deque,defaultdict
from multiprocessing import Manager
from time import sleep


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
        # try:
        #     self.file_path = self.input_dir + "/" + self.file_name
        #     self.file_size = os.stat(self.file_path)
        # except FileNotFoundError:
        #     print("File {} does not exist. Aborting".format(self.file_name))
        #     sys.exit(1)

        # # Read the file contents
        # contents = ""
        # n_lines = 0
        # with open(self.file_path, 'r') as f:
        #     contents = f.readlines()

        # # Split the file into n_mappers 
        # for lines in contents:
        #     n_lines += 1

        # n_lines_in_each_split = math.floor(n_lines / self.n_mappers)
        # line_number = 0

        # file_descriptors = [''] * self.n_mappers
        # for mapper in range(self.n_mappers):
        #     file_path = self.input_dir + "/" + str(mapper) + ".txt"
        #     file =  open(file_path, 'w')
        #     file_descriptors[mapper] = file

        # start = 0
        # for line in contents:
        #     index = start % self.n_mappers
        #     file_descriptors[index].write(line)
        #     start += 1

        # # Close all file descriptors
        # for mapper in range(self.n_mappers):
        #     file_descriptors[mapper].close()
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

    def launch_mappers(self,lazy):
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
            num_part=self.n_mappers
            # Start all the mappers in parallel
            # if lazy is false:
            #     sleep(0.01)
            for i in range(self.n_mappers):
                file_path = self.input_dir + "/" + str(i) + ".txt"
                p = Process(target=self.mapper, args=(file_path, i,map, queue,  map_st_time,map_end_time,num_part))
                p.start()
                processes.append(p)

            ## use manager manged queue
            output = manager.Queue()
            # Next we perform sorting (this is the eager setting)
            # For the lazy setting, we need to join all the mappers
            # in the list `processes` (this means that sorting
            # will have to wait until all mappers are done)
            #rc = 610
            #sleep(14000/350 - 14000/610)
            if lazy is False:
                for i in range(num_part):
                    p = Process(target=self.sort, args = [queue, output,sort_st_time,sort_end_time,i,self.input_dir])
                    p.start()
                    processes.append(p)

            
            ## join all the processes so that main process does not exit before child completes
            for p in processes:
                if p.is_alive():
                    exitcode = p.join()
                    # print("Skip join for process", p.pid, "exitcode", exitcode)
            if lazy is True:
            ## start the sorting process
                processes = []
                for i in range(num_part):
                    p = Process(target=self.sort, args = [queue, output,sort_st_time,sort_end_time,i,self.input_dir])
                    p.start()
                    processes.append(p)

                for p in processes:
                    if p.is_alive():
                        exitcode = p.join()
                        # print("Skip join for process", p.pid, "exitcode", exitcode)
                            
            processes = []
            # self.combined = output.get()
            # print("combined : ", self.combined)

            # Now here we start the reducers
            i = 0
            for i in range(num_part):
                file_path = self.output_dir + "/" + str(i) + ".txt"
                sort_output = output.get()
                # print("Sort output", sort_output)
                p = Process(target=self.reducer, args=(file_path, sort_output, reduce_st_time,reduce_end_time))
                p.start()
                processes.append(p)

            for p in processes:
                p.join()

            end_time = time.time()
            duration1, duration2, duration3 = 0 , 0 ,0
            cost_map,cost_sort,cost_reduce=0,0,0
            ########Printing#########
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
            print("================Reducer Information====================")
            for k,v in reduce_st_time.items():
                duration3=reduce_end_time[k]-v
                print("Process: ",k,"start time ",v, "end time ",reduce_end_time[k], "duration",duration3)
                cost_reduce+=duration3
            
            print("JCT Running time : " + str(end_time - start_time))
            print("Cost: "+str(cost_map+cost_sort+cost_reduce))
            
            
    def mapper(self, file_path, arg,map, q, map_st_time,map_end_time,num_partitions=2):
        # q.cancel_join_thread()
        print("Mapper"+str(os.getpid()))
        myfiles=list()
        for i in range(0,num_partitions):
            myfiles.append(open(self.input_dir + "/map"+str(arg)+"-part" +str(i)+'.txt', 'w+'))
        
        def compute_hash(st):
            h = 0
            for c in st:
                h = (31 * h + ord(c)) & 0xFFFFFFFF
            return ((h + 0x80000000) & 0xFFFFFFFF) - 0x80000000

        map_st_time[os.getpid()]=time.time()
        contents = ''
        with open(file_path, 'r') as f:
            contents = f.readlines()

        for line in contents:
            map_output = map(line)
            # print("Map output: ", map_output, "process", os.getpid())
            for m in map_output:
                if(m[0][0]=='-'):
                    continue
                hash=compute_hash(m[0])
                val=hash%num_partitions
                myfiles[val].write(str(m[0])+","+str(m[1]))
                myfiles[val].write("\n")
                myfiles[val].flush()
                q.put(m)

                ## added delay to allow queue to consume the items
                # sleep(0.001)
                # print("adding to queue")
        for i in range(0,num_partitions):
            myfiles[i].write("end\n")
                #myfiles[i].close()
            myfiles[i].flush()
            myfiles[i].close()

        q.put(("DONE", 1))
        # print("Size of the dq", q.qsize())
        ## terminate the process here
        # os.kill(os.getpid(), signal.SIGTERM)
        map_end_time[os.getpid()]=time.time()

    def sort(self, q, output, sort_st_time,sort_end_time,part,file_path):

        sort_st_time[os.getpid()]=time.time()
        # print("No of mappers ", self.n_mappers)
        combined=defaultdict()
        for map in range(self.n_mappers):
            # print(file_path+"/map"+str(map)+"-part" +str(part)+'.txt')
            if os.path.exists(file_path+"/map"+str(map)+"-part" +str(part)+'.txt'):
                f=open(file_path+"/map"+str(map)+"-part" +str(part)+'.txt','r')
                content=f.readline()
                # print("contents", content, "inside process ", os.getpid())
                while 1:
                    # time.sleep(0.01)
                    # print("Read content ", content)
                    # print(str(os.getpid())+"-"+ "filesize : ", os.path.getsize(file_path+"/map"+str(map)+"-part" +str(part) + '.txt'))
                    if(content=='end\n'):
                        print("Found end while reading the file  ", file_path+"/map"+str(map)+"-part" +str(part))
                        break
                    else:
                        try:
                            val=content.split(',')
                            key,value=val[0],val[1].rstrip("\n")
                            if key in combined:
                                combined[key].append(value)
                            else:
                                combined[key] = [value]
                        except Exception as e:
                            # print("Exception",e)
                            pass
                    content=f.readline()
                    
                ## insert the dic into the output queue
        output.put(combined)
        sort_end_time[os.getpid()]=time.time()

    def reducer(self, file_path, chunk, reduce_st_time,reduce_end_time):
        reduce_st_time[os.getpid()]=time.time()
        s = ""
        for key in chunk:
            word, count = reduce(key, chunk[key])
            s += word + ": " + str(count) + "\n"
        reduce_end_time[os.getpid()]=time.time()
        
        try:
            f = open(file_path, "w+")
            f.write(s)
            f.close()
        except Exception as e:
            print(e)
