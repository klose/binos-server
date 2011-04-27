#!/bin/sh  
parent_path=`pwd .`
echo $parent_path
java -cp $CLASSPATH:$parent_path/bin/lib/transCompiler.jar:$parent_path/bin/lib/protobuf-java-2.3.0.jar:$parent_path/bin/lib/protobuf_socketrpc-1.3.2_DIY.jar:$parent_path/bin/lib/sigar.jar:$parent_path/bin/lib/libsigar-x86-linux.so:$parent_path/bin/lib/libsigar-amd64-linux.so:$parent_path/bin/lib/libsigar-x86-winnt.lib:$parent_path/bin/lib/libsigar-x86-winnt.dll:$parent_path/bin/lib/hadoop-common-0.21.0.jar:$parent_path/bin/lib/hadoop-hdfs-0.21.0.jar:$parent_path/bin/lib/commons-logging-api-1.1.jar:$parent_path/bin/lib/httpcore-4.0.1.jar:$parent_path/bin/lib/dom4j-2.0.0-ALPHA-2.jar:/home/jiangbing/hadoop-0.21.0/conf/:$parent_path/bin/ -Djava.library.path=$parent_path/bin/lib $@ 
