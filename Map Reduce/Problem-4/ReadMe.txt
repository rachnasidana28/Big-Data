Problem Description:

List the 'user id' and 'rating' of users that reviewed businesses located in Stanford using In-Memory join technique by loading the business data in the distributed cache.

--------------------------------------------------------------------
********************************************************************
--------------------------------------------------------------------


How To Run
--------------------------------------------------------------------
Use following command to run 

$ hadoop jar A01_04.jar Assignment01_04 /input/business.csv /input/review.csv /output

Distributed cache file: /input/business.csv 
input file(S): /input/review.csv 
Output directory: /output

---------------------------------------------------------------------

Output of the program can be seen using the following command

$ hdfs dfs -cat /output/*

----------------------------------------------------------------------

Folder contains
.java files ---> Source Code
.jar file
Output file
ReadMe