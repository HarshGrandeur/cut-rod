# cut-rod
This is a simple Map Reduce framework which has been implemented to demo Nimble algorithm.

https://www.usenix.org/conference/nsdi21/presentation/zhang-hong

The final code resides in the `eager-new` branch.
* How to run the code:
Use the command `python run.py <Input file location> <Number of mappers> <Number of reducers>`

First ensure that the folders 'input_files' and 'output_files' are present in the root directory of this repo. Then, add a sample test file in the 'input_files' folder.
Then run the above command. An example would be:

`python run.py test.txt 3 3`

The files with the word counts would be generated in the folder 'output_files'.

Note: This map reduce implementation can also be extended to other tasks by simply replaceing the map and reduce functions defined in `map_and_reduce.py`. Currently the map reduce functions defined calculate the word count for a given text file. 
