#!/bin/sh 
dir=`dirname "${BASH_SOURCE-$0}"`
cd $dir
export MasterSlaveHome=`cd ..; pwd .`
echo ${MasterSlaveHome}
# The Hadoop distribution to use. Required.
export HADOOP_HOME=/home/jiangbing/hadoop-0.21.0

#export MasterSlaveHome=/home/jiangbing/eclipse_java/java_workspace/Master-Slave

# Where log files are stored.  $MasterSlaveHome/logs by default.
#export MasterSlave_LOG_DIR=${MasterSlaveHome}/logs

# File naming remote slave hosts.  $MasterSlaveHome/conf/slaves by default.
# export Master_SLAVES=${MasterSlaveHome}/conf/slaves

# File naming master host.  $MasterSlaveHome/conf/master by default.
# export Master_MASTER=${MasterSlaveHome}/conf/master


