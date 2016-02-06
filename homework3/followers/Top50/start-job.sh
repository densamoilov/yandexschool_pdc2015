#!/bin/sh

hadoop jar ./top50.jar Top50 -D mapred.reduce.tasks=1 /pub/followers.db ./followers/output  
