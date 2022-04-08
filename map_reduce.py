def map(line):
    arr = line.split(',')
    return (arr[1], 1)

def reduce(key, list):
    count = 0
    for val in list:
        count += val

    return (key, count)