# cut-rod
This is a simple Map Reduce framework which has been implemented to demo Nimble algorithm.

https://www.usenix.org/conference/nsdi21/presentation/zhang-hong

* How to run the code:
Use the command `python run.py <Input file location> <Number of mappers> <Number of reducers>`

First ensure that the folders 'input_files' and 'output_files' are present in the root directory of this repo. Then, add a sample test file in the 'input_files' folder.
Then run the above command. An example would be:

`python run.py test.txt 3 3`

The files with the word counts would be generated in the folder 'output_files'.

Note: This map reduce implementation can also be extended to other tasks by simply replaceing the map and reduce functions defined in `map_and_reduce.py`. Currently the map reduce functions defined calculate the word count for a given text file. 

Example of Nimble run, where Nimble has optimal cost and JCT.
(bdml_3.6) richaranderia@10-16-96-49 cut-rod % python run.py devicestatus.txt 2 2
Distribution of files:  [10137, 100000]
Mapper90577
Mapper90578
Found end while reading the file   input_files/map0-part0
Found end while reading the file   input_files/map0-part1
Found end while reading the file   input_files/map1-part0
Found end while reading the file   input_files/map1-part1
================Mapper Information====================
Process:  90577 start time  1652287674.048661 end time  1652287683.199583 duration 9.150922060012817
Process:  90578 start time  1652287674.05031 end time  1652287727.580523 duration 53.53021311759949
================Sorting Information====================
Process:  90587 start time  1652287727.6021912 end time  1652287730.599633 duration 2.9974417686462402
Process:  90588 start time  1652287727.603049 end time  1652287730.6961632 duration 3.093114137649536
================Reducer Information====================
Process:  90591 start time  1652287730.8263848 end time  1652287731.08647 duration 0.2600851058959961
Process:  90592 start time  1652287730.887839 end time  1652287731.2189472 duration 0.33110809326171875
JCT Running time : 57.19511127471924
Cost: 69.3628842830658

Eager:
(bdml_3.6) richaranderia@10-16-96-49 cut-rod % python run.py devicestatus.txt 2 2
Distribution of files:  [10137, 100000]
Mapper90624
Mapper90625
Found end while reading the file   input_files/map0-part0
Found end while reading the file   input_files/map0-part1
Found end while reading the file   input_files/map1-part0
Found end while reading the file   input_files/map1-part1
================Mapper Information====================
Process:  90624 start time  1652287807.2091038 end time  1652287814.94678 duration 7.73767614364624
Process:  90625 start time  1652287807.210504 end time  1652287861.346293 duration 54.135788917541504
================Sorting Information====================
Process:  90626 start time  1652287807.2140858 end time  1652287861.458418 duration 54.24433207511902
Process:  90627 start time  1652287807.2143118 end time  1652287861.556639 duration 54.34232711791992
================Reducer Information====================
Process:  90636 start time  1652287861.6823099 end time  1652287861.935314 duration 0.2530040740966797
Process:  90637 start time  1652287861.7381032 end time  1652287862.070472 duration 0.3323688507080078
JCT Running time : 54.890316009521484
Cost: 171.04549717903137

Nimble:
(bdml_3.6) richaranderia@10-16-96-49 cut-rod % python run.py devicestatus.txt 2 2
Distribution of files:  [10137, 100000]
Mapper90710
Mapper90711
Found end while reading the file   input_files/map0-part0
Found end while reading the file   input_files/map0-part1
Found end while reading the file   input_files/map1-part0
Found end while reading the file   input_files/map1-part1
================Mapper Information====================
Process:  90710 start time  1652288093.7587378 end time  1652288101.067392 duration 7.308654308319092
Process:  90711 start time  1652288093.759344 end time  1652288145.169188 duration 51.40984392166138
================Sorting Information====================
Process:  90717 start time  1652288145.015563 end time  1652288147.997288 duration 2.981724977493286
Process:  90718 start time  1652288145.015906 end time  1652288148.0923488 duration 3.0764427185058594
================Reducer Information====================
Process:  90721 start time  1652288148.2187788 end time  1652288148.471379 duration 0.25260019302368164
Process:  90722 start time  1652288148.277142 end time  1652288148.60608 duration 0.32893800735473633
JCT Running time : 54.872753858566284
Cost: 65.35820412635803
