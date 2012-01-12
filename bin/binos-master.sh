#!/bin/sh
#the script is used for binos-yarn.
bin=`dirname "${BASH_SOURCE-$0}"`
cd $bin
parent_path=`cd ..;pwd .`
parent_conf=$parent_path/conf
parent_bin=$parent_path/bin
execute_file=run.sh 
MasterClass=com.klose.Master.Master
port=6060 #default port
logdir=${parent_path}/logs #default logdir
logfile=${logdir}/binos-master.log # default logfile
function printHelp {
	echo "Usage:  [--port=PORT] [--log_dir=LOG_DIR] [...] "
	echo "The script is only used for binos-yarn."
	echo "Support options:"
    echo "	--help                   display this help and exit."
   	echo "	--port=VAL               port to listen on (default: 6060)."
    echo "	--log_dir=VAL            log directory for Binos-Master Container."
}

if [ $# != 2 ] ; then
	printHelp
	exit 1
else 
	for args in $1 $2
	do
		prefix=`echo $args | cut -d '=' -f 1`
		case $prefix in 
			--port)
				p=`echo $args | cut -d '=' -f 2`
				if [ $p -gt 0 ] & [ $p -le 65535 ]; then
					port=$p
				else 
					echo "--port=VAL VAL should be set in number (0~65535)"
					exit -1
				fi
			;;	
			--log_dir)
				l=`echo $args | cut -d '=' -f 2`
				if [ ! -d $l ]; then
					mkdir -p $l >/dev/null 2>&1
				#else 
					#echo "--log_dir=VAL VAL should be a path for diectory"
					#exit -2
				fi		
				logfile=$l"/binos-master.log"
			;;	 
				*)
					printHelp
					exit 1	
			;;
		esac		 
	done    			
fi
#$parent_bin/$execute_file $MasterClass --port=$port > $logdir/binos-master.log 2>&1 &
$parent_bin/$execute_file $MasterClass --port=$port > $logfile  2>&1 &
