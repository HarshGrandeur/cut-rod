from multiprocessing.connection import Client
from array import array

class Reducer:

    def __init__(self, port=6000):
        self.address = ('localhost', port)
        

    def reducer(self):
        with Client(self.address, authkey=b'secret password') as conn:
            try:
                while(True):
                    print(conn.recv())
            except EOFError:
                print("EOF")


if __name__ == "__main__":
   reducer = Reducer()
   reducer.reducer()