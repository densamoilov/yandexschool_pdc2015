#!/bin/sh

hadoop jar ./distributionfollowers.jar DistributionFollowers -D mapred.reduce.tasks=1 /pub/followers.db ./followers/output  
