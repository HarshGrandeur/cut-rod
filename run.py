#https://docs.python.org/3/library/multiprocessing.html#multiprocessing.pool.Pool

import sys, getopt
from scheduler import Scheduler

def main(argv):
    file_name = argv[1]
    n_mappers = int(argv[2])
    n_reducers =  int(argv[3])
    sc = Scheduler(n_mappers=n_mappers, file_name=file_name, n_reducers=n_reducers)
    sc.split_input_files()


def map(line):
    arr = line.split(',')
    return (arr[1], 1)

def reduce(key, list):
    count = 0
    for val in list:
        count += val

    return (key, count)



if __name__ == "__main__":
   main(sys.argv)