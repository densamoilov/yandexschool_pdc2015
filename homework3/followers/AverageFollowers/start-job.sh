#!/bin/sh

hadoop jar ./averagefollowers.jar AverageFollowers -D mapred.reduce.tasks=1 /pub/followers.db ./followers/output  
