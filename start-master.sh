#!/bin/sh 
bin=`dirname "${BASH_SOURCE-$0}"`
cd $bin
parent_path=`pwd .`
echo $parent_path
for master in `cat  $parent_path/master`;
do 
	 eachMaster=`echo $master | awk -F ":" '{print $1}'`
	 eachPort=`echo $master | awk -F ":" '{print $2}'| sed -e '/^$/d'`
	 echo $eachMaster 
	 if [ -n $eachMaster  ] ; then
		 if [ "l"$eachPort = "l" ] ; then
		 	 echo "well 0"
			 ssh $eachMaster -C "($parent_path/master.sh)" 
		else 
			ssh $eachMaster -C "($parent_path/master.sh --port=$eachPort)" 
		fi
	fi
done
