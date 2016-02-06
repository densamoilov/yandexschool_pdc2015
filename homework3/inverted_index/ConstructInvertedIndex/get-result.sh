#!/bin/sh

hdfs dfs -get ./inverted_index/output/part* ./inverted_index_result.txt
hdfs dfs -rm -r ./inverted_index/output/