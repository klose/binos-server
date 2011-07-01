#!/bin/sh 
bin=`dirname "${BASH_SOURCE-$0}"`
cd $bin
parent_path=`cd ..;pwd .`
parent_conf=$parent_path/conf
parent_bin=$parent_path/bin 
for slave in `cat $parent_conf/slaves`;
do 
	eachSlave=`echo $slave | awk -F ":" '{print $1}'`
	for slaveDaemon in `ssh $eachSlave jps | grep Slave | awk -F ' ' '{print $1}'`;
	do 
		ssh $eachSlave kill -9 $slaveDaemon 
	done
done
for master in `cat $parent_conf/master`;
do 
	eachMaster=`echo $master | awk -F ":" '{print $1}'`
	for masterDaemon in `ssh $eachMaster jps | grep Master | awk -F ' ' '{print $1}'`;
	do 
		ssh $eachMaster kill -9 $masterDaemon
	done 
done 
