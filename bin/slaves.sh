#!/usr/bin/env bash
bin=`dirname "${BASH_SOURCE-$0}"`
cd $bin
parent_path=`cd ..;pwd .`
parent_conf=$parent_path/conf
parent_bin=$parent_path/bin
#echo $parent_path
SlaveClass=com.klose.Slave.Slave 
execute_file=run.sh  
LOG_DIR=$parent_path/logs
if [ ! -d $LOG_DIR ]; then
	mkdir -p $LOG_DIR >/dev/null 2>&1
fi
time=`date "+%G%m%d%H%M%S"`
#Master=`cat $parent_conf/master`
MasterHost=`awk -F ":" '{print $1}' $parent_conf/master`
MasterPort=`awk -F ":" '{print $2}' $parent_conf/master`
if [ ! -n $MasterHost ] ;then
	exit -1
else 
	if [ "l"$MasterPort != "l" ]; then 
		MasterURL="JLoop://0@"$Master 
	else 
		MasterURL="JLoop://0@"$MasterHost":6060" 
	fi
fi
#echo $MasterURL
if [ $# = 2 ]; then 
	time=$2"_"$time
	$parent_bin/$execute_file $SlaveClass --url=$MasterURL $1 >$LOG_DIR/slave_$time.log 2>&1 &
fi 
if [ $# = 1 ]; then
	time=$1"_"$time
	$parent_bin/$execute_file $SlaveClass --url=$MasterURL  >$LOG_DIR/slave_$time.log 2>&1 & 
fi
