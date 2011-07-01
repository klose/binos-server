#!/bin/sh 
bin=`dirname "${BASH_SOURCE-$0}"`
cd $bin
parent_path=`cd ..;pwd .`
#echo $parent_path
parent_conf=$parent_path/conf
parent_bin=$parent_path/bin
for master in `cat  $parent_conf/master`;
do 
	 eachMaster=`echo $master | awk -F ":" '{print $1}'`
	 eachPort=`echo $master | awk -F ":" '{print $2}'| sed -e '/^$/d'`
	 echo $eachMaster 
	 if [ -n $eachMaster  ] ; then
		 if [ "l"$eachPort = "l" ] ; then
			 ssh $eachMaster -C "($parent_bin/master.sh)" 
			 echo "MasterURL: JLoop://0@${eachMaster}:6060"
		 else 
			ssh $eachMaster -C "($parent_bin/master.sh --port=$eachPort)"
			echo "MasterURL: JLoop://0@${master}"
		fi
	fi
done
