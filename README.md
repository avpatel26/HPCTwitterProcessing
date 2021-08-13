# HPC Twitter Processing

This project uses Python – MPI on the HPC Spartan(University of Melbourne Research cloud) to extract the most commonly used hashtags on Twitter, and the languages used to Tweet.

In this application to achieve parallelization, we decided to go with Master-slave architecture in which process 0 works as master process. It performs data collection through extracting necessary data rather than loading whole file into memory. Then divides those data according to number of process and scatters those data to different processes. All the processes will execute different data set and count number of hashtags and languages and send result to master process. After collecting all the results, master process performs final merging and sorting of data and provides proper output.

## Comparative Analysis

This script has executed over different number and core of processors on SPARTAN (HPC system of UniMelb) and used large datasets(20.7 GB) of tweets to analyse the performance.

![alt_text](https://github.com/avpatel26/HPCTwitterProcessing/blob/main/result.jpg?raw=true)

