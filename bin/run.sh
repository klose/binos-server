#!/bin/sh  
bin=`dirname "${BASH_SOURCE-$0}"`
cd $bin
parent_path=`cd ..;pwd .`
parent_conf=$parent_path/conf 

. $parent_conf/MS-config.sh


java -cp $CLASSPATH:$parent_path/lib/transCompiler.jar:$parent_path/lib/log4j-1.2.15.jar:$parent_path/lib/binos.jar:$parent_path/lib/protobuf-java-2.3.0.jar:$parent_path/lib/protobuf_socketrpc-1.3.2_DIY.jar:$parent_path/lib/sigar.jar:$parent_path/lib/libsigar-x86-linux.so:$parent_path/lib/libsigar-amd64-linux.so:$parent_path/lib/libsigar-x86-winnt.lib:$parent_path/lib/libsigar-x86-winnt.dll:$parent_path/lib/hadoop-common-0.21.0.jar:$parent_path/lib/hadoop-hdfs-0.21.0.jar:$parent_path/lib/commons-logging-api-1.1.jar:$parent_path/lib/httpcore-4.0.1.jar:$parent_path/lib/dom4j-2.0.0-ALPHA-2.jar:${HADOOP_HOME}/conf:${parent_path}/lib/jetty-6.1.14.jar:${parent_path}/lib/jetty-util-6.1.14.jar:${parent_path}/lib/servlet-api-2.5-6.1.14.jar:${parent_path}/lib/memcached-2.6.jar:${parent_path}/lib/commons-logging-1.1.1.jar:${parent_path}/classes -Djava.library.path=${parent_path}/lib $@
