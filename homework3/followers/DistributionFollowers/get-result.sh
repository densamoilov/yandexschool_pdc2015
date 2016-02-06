#!/bin/sh

 hdfs dfs -get ./followers/output/part* ./distribution_followers_result.txt
 hdfs dfs -rm -r ./followers/output