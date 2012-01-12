#!/bin/sh
bin=`dirname "${BASH_SOURCE-$0}"`
cd $bin
parent_path=`cd ..;pwd .`
parent_conf=$parent_path/conf
parent_bin=$parent_path/bin
#echo $parent_path
SlaveClass=com.klose.Slave.Slave 
execute_file=run.sh
logdir=$parent_path/logs
function printHelp {
	echo "Usage: [--url=MasterURL] [--port=slavePort] [--log_dir=LOG_DIR] [...]"
	echo "The Script is only used for binos-yarn."
	echo "Support options:"
	echo "	--help                      display this help and exit"
	echo "	--url=VAL                   connect to a MasterURL"
    echo "	--port=VAL                  port to listen on (default: 6061)."
	echo "	--log_dir=VAL               log directory for Binos-Slave Container."
}
if [ $# != 3 ] ; then
	printHelp
	exit 1
else 
	for args in $1 $2 $3
	do
		prefix=`echo $args | cut -d '=' -f 1`
		case $prefix in 
			--url)
				url=`echo $args | cut -d '=' -f 2`
				if [ ${url%%/*} = "JLoop:" ]; then
					masterURL=$url
				else 
					echo "--url=VAL VAL should be set as MasterURL"
					exit -1
				fi
			;;	
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
				fi		
				logdir=$l
			;;	 
				*)
					printHelp
					exit 1	
			;;
		esac		 
	done    			
fi
#$parent_bin/$execute_file $SlaveClass --url=$masterURL > $logdir/binos-slave.log 2>&1 &
$parent_bin/$execute_file $SlaveClass --port=$port --url=$masterURL  > $logdir/binos-slave.log.$port  2>&1 
