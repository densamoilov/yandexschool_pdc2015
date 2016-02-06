#!/bin/sh

hdfs dfs -get ./inverted_index/output/part* ./total_num_docs_result.txt
hdfs dfs -rm -r ./inverted_index/output/