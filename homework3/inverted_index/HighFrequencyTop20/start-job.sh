#!/bin/sh

hadoop jar ./highfrequencytop.jar shad.homework3.task2.HighFrequencyTop -D mapreduce.job.reduces=1 ./inverted_index/debug.xml ./inverted_index/output



    
    