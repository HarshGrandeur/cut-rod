#https://docs.python.org/3/library/multiprocessing.html#multiprocessing.pool.Pool

import sys, getopt
from scheduler import Scheduler
from reducer import Reducer

def main(argv):
    file_name = argv[1]
    n_mappers = int(argv[2])
    n_reducers =  int(argv[3])
    print("================Lazy Task====================")
    lazy=True
    nimble=False
    sc = Scheduler(n_mappers=n_mappers, file_name=file_name, n_reducers=n_reducers)
    sc.split_input_files()
    duration=sc.launch_mappers(lazy,sleep_val=0)
    # print("================Eager Task====================")
    # lazy=False
    # nimble=False
    # sc = Scheduler(n_mappers=n_mappers, file_name=file_name, n_reducers=n_reducers)
    # sc.split_input_files()
    # finish_time=sc.launch_mappers(lazy,sleep_val=0)
    # print("================Nimble Task====================")
    # lazy=False
    # sleep_val=finish_time-duration
    # print("Delay for: "+str(sleep_val))
    # sc = Scheduler(n_mappers=n_mappers, file_name=file_name, n_reducers=n_reducers)
    # sc.split_input_files()
    # sc.launch_mappers(lazy,sleep_val)
        

if __name__ == "__main__":
   main(sys.argv)
