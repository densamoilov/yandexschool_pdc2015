#!/bin/sh

hadoop jar ./invertedindex.jar shad.homework3.task2.InvertedIndex -D mapreduce.job.reduces=1 ./inverted_index/debug.xml ./inverted_index/output



    
    