#!/bin/sh 
bin=`dirname "${BASH_SOURCE-$0}"`
cd $bin
parent_path=`pwd .`
echo $parent_path
for slave in `cat  $parent_path/slaves`;
do 
	 eachSlave=`echo $slave | awk -F ":" '{print $1}'`
	 eachPort=`echo $slave | awk -F ":" '{print $2}'`
	 echo $eachSlave 
	 echo $eachPort
	 if [ -n $eachSlave  ] ; then
		 if [ "l" = "l"$eachPort ] ; then
		 	ssh $eachSlave -C "($parent_path/slaves.sh $slave\"_6061\")" 
		else 
			ssh $eachSlave -C "($parent_path/slaves.sh --port=$eachPort $eachSlave"_"$eachPort)" 
		fi
	fi
done
