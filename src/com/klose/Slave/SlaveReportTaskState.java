package com.klose.Slave;

import java.util.logging.Logger;

import com.googlecode.protobuf.socketrpc.SocketRpcChannel;
import com.googlecode.protobuf.socketrpc.SocketRpcController;
import com.klose.MsConnProto.TaskChangeState;
import com.klose.MsConnProto.TaskStateChangeService;

public class SlaveReportTaskState extends Thread{
	private SlaveArgsParser parser;
	private SocketRpcChannel channel;
	private SocketRpcController controller;
	private Logger LOG = Logger.getLogger(SlaveReportTaskState.class.getName());
	public SlaveReportTaskState(SlaveArgsParser parser, SocketRpcChannel channel, 
			SocketRpcController controller) {
		this.parser = parser;
		this.channel = channel;
		this.controller = controller;
	}
	public void run() {
		
		TaskStateChangeService stateChange = 
			TaskStateChangeService.newStub(this.channel);
		TaskChangeState request = TaskChangeState.newBuilder().
				setState(value)
		

				
				
	}
}
