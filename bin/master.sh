#!/usr/bin/env bash
bin=`dirname "${BASH_SOURCE-$0}"`
cd $bin
parent_path=`cd ..;pwd .`
parent_conf=$parent_path/conf
parent_bin=$parent_path/bin
execute_file=run.sh 
MasterClass=com.klose.Master.Master
LOG_DIR=$parent_path/logs
if [ ! -d $LOG_DIR ]; then
	mkdir -p $LOG_DIR >/dev/null 2>&1
fi
time=`cat $parent_conf/master`"_"`date "+%G%m%d%H%M%S"`
$parent_bin/$execute_file $MasterClass $@ > $LOG_DIR/master_$time.log 2>&1 &
