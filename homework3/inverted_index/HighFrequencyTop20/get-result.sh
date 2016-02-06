#!/bin/sh

hdfs dfs -get ./inverted_index/output/part* ./high_frequency_top_result.txt
hdfs dfs -rm -r ./inverted_index/output/