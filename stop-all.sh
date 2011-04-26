#!/bin/sh 
bin=`dirname "${BASH_SOURCE-$0}"`
cd $bin
parent_path=`pwd .`
for slave in `cat $parent_path/slaves`;
do 
	eachSlave=`echo $slave | awk -F ":" '{print $1}'`
	for slaveDaemon in `ssh $eachSlave jps | grep Slave | awk -F ' ' '{print $1}'`;
	do 
		ssh $eachSlave kill -9 $slaveDaemon 
	done
done
for master in `cat $parent_path/master`;
do 
	eachMaster=`echo $master | awk -F ":" '{print $1}'`
	for masterDaemon in `ssh $eachMaster jps | grep Master | awk -F ' ' '{print $1}'`;
	do 
		ssh $eachMaster kill -9 $masterDaemon
	done 
done 
