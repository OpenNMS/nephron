#!/bin/bash
# script that can be run before the benchmark runs a pipeline with a specific set of arguments
# (cf. the -before option of the benchmark launcher)
# -> restart flink cluster because after some time it does not accept new jobs
echo "restart flink cluster"
cd /opt/flink
bin/stop-cluster.sh
bin/start-cluster.sh
