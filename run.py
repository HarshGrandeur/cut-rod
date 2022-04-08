#https://docs.python.org/3/library/multiprocessing.html#multiprocessing.pool.Pool

import sys, getopt
from scheduler import Scheduler
from reducer import Reducer

def main(argv):
    file_name = argv[1]
    n_mappers = int(argv[2])
    n_reducers =  int(argv[3])
    sc = Scheduler(n_mappers=n_mappers, file_name=file_name, n_reducers=n_reducers)
    sc.split_input_files()
    sc.launch_mappers()


if __name__ == "__main__":
   main(sys.argv)