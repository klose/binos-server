#!/bin/sh 
bin=`dirname "${BASH_SOURCE-$0}"`
cd $bin
parent_path=`cd ..;pwd .`
#echo $parent_path
parent_conf=$parent_path/conf
parent_bin=$parent_path/bin
for slave in `cat  $parent_conf/slaves`;
do 
	 eachSlave=`echo $slave | awk -F ":" '{print $1}'`
	 eachPort=`echo $slave | awk -F ":" '{print $2}'`
	 #echo $eachSlave 
	 #echo $eachPort
	 if [ -n $eachSlave  ] ; then
		 if [ "l" = "l"$eachPort ] ; then
		 	ssh $eachSlave -C "($parent_bin/slaves.sh $slave\"_6061\")" 
			echo "${eachSlave}:6061"
		else 
			ssh $eachSlave -C "($parent_bin/slaves.sh --port=$eachPort $eachSlave"_"$eachPort)" 
			echo $slave
		fi
	fi
done
