#!/bin/sh

 hdfs dfs -get ./followers/output/part* ./top50_result.txt
 hdfs dfs -rm -r ./followers/output