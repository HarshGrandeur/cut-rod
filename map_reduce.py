def map(line):
    arr = line.split()
    if not arr:
        return (0, 0)

    return [(key, 1) for key in arr]

def reduce(key, list):
    count = 0
    for val in list:
        count += val

    return (key, count)