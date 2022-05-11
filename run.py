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
    rc=sc.launch_mappers(lazy,nimble)
    print("================Eager Task====================")
    lazy=False
    nimble=False
    sc = Scheduler(n_mappers=n_mappers, file_name=file_name, n_reducers=n_reducers)
    sc.split_input_files()
    rc=sc.launch_mappers(lazy,nimble)
    print("================Nimble Task====================")
    lazy=False
    nimble=True
    sc = Scheduler(n_mappers=n_mappers, file_name=file_name, n_reducers=n_reducers)
    sc.split_input_files()
    rc=sc.launch_mappers(lazy,nimble)
        

if __name__ == "__main__":
   main(sys.argv)
