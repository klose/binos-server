#!/bin/sh 
bin=`dirname "${BASH_SOURCE-$0}"`
echo $bin
cd $bin
BIN=`pwd`
MS_HOME=`cd ../../../; pwd`
echo $MS_HOME
. ${MS_HOME}/conf/MS-config.sh 


#1)generate largdata.txt , please set file size:$1 
cd $BIN
$BIN/procData $1


#2) generate classpath for java -cp
cd $MS_HOME

classpath="$CLASSPATH"
for path in `find -name "*.jar"`;
do 	
	classpath=${classpath}:`pwd $path`/$path
done
classpath=$classpath:$HADOOP_HOME/conf
echo $classpath
#3) split file, and put the file into hdfs
cd $BIN
hadoop_state=`jps| grep "[DataNode|NameNode]"`
if [ -z "${hadoop_state}" ]; then
	echo "hadoop hdfs is not open."
fi
java -cp $classpath com.lyz.test.compiler.FileSplit ./largdata.txt

#4) run testWordCount.jar
cd $MS_HOME 
$MS_HOME/bin/run.sh com.klose.JLoopClient --


