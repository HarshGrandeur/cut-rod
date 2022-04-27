import re

def map(line):
    arr = re.split(r",", line)
    return [(arr[0],1)]

def reduce(key, list):
    count = 0
    for val in list:
        count += val

    return (key, count)