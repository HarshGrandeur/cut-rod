from multiprocessing.connection import Client
from array import array

class Reducer:

    def __init__(self, port=6000):
        self.address = ('localhost', port)
        

    def reducer(self):
        with Client(self.address, authkey=b'secret password') as conn:
            print(conn.recv())                  # => [2.25, None, 'junk', float]
            reducer_input =  conn.recv_bytes()
            print(reducer_input.decode())
