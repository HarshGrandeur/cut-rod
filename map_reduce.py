import re

def map(line):
    arr = re.split(r"\W+", line)
    return [(key.lower(), 1) for key in arr]

def reduce(key, list):
    count = 0
    for val in list:
        count += val

    return (key, count)