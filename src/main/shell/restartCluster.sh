#!/bin/bash
# Restart Hadoop and Spark
/usr/local/hadoop/sbin/stop-yarn.sh
/usr/local/hadoop/sbin/stop-all.sh
/usr/local/hadoop/sbin/start-yarn.sh 
/usr/local/hadoop/sbin/start-all.sh

echo ">> Cluster restarted"
echo ">> Wait for 60 seconds ..."
sleep 60 
echo ">> OK, time's up"