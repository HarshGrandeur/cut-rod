import re

def map(line):
    arr = re.split(r",", line)
    output=[]
    for elem in arr:
        output.append((elem,1))
    return output

def reduce(key, list):
    count = 0
    for val in list:
        try:
            count += val
        except:
            pass

    return (key, count)