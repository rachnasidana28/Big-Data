Problem Description:

List the  business_id , full address and categories of the Top 10 businesses using the average ratings by using Reduce side join technique and job chaining.

--------------------------------------------------------------------
********************************************************************
--------------------------------------------------------------------



How To Run
--------------------------------------------------------------------
Use following command to run 

$ hadoop jar A01_03.jar Assignment01_03 /input/review.csv /input/business.csv /intermediatePath /output


input file(S): /input/business.csv /input/review.csv 
intermediate file directory: /intermediatePath
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

