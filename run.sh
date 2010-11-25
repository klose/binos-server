#!/bin/sh 
parent_path=`pwd .`
java -cp $CLASSPATH:$parent_path/bin/lib/protobuf-java-2.3.0.jar:$parent_path/bin/lib/protobuf_socketrpc-1.3.2.jar:$parent_path/bin/ $@ 


# test comment

