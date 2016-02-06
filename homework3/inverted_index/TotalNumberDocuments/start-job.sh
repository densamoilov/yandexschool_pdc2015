#!/bin/sh

hadoop jar ./totalnumberdocuments.jar shad.homework3.task2.TotalNumberDocuments -D mapreduce.job.reduces=1 ./inverted_index/debug.xml ./inverted_index/output



    
    