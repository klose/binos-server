package com.klose.Slave;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.RpcCallback;
import com.googlecode.protobuf.socketrpc.SocketRpcChannel;
import com.googlecode.protobuf.socketrpc.SocketRpcController;
import com.klose.MsConnProto.ConfirmMessage;
import com.klose.MsConnProto.TaskChangeState;
import com.klose.MsConnProto.TaskStateChangeService;

public class SlaveReportTaskState {
	private SlaveArgsParser parser;
	private SocketRpcChannel channel;
	private SocketRpcController controller;
	private volatile static  boolean reportResponse = false;
	private Logger LOG = Logger.getLogger(SlaveReportTaskState.class.getName());
	public SlaveReportTaskState(SlaveArgsParser parser) {
		this.parser = parser;
		this.channel = new SocketRpcChannel(this.parser.getMasterIp(), 
				this.parser.getMasterPort());
		this.controller = this.channel.newRpcController();
	}
	public void report(String taskId, String state) {
		TaskStateChangeService stateChange = 
			TaskStateChangeService.newStub(this.channel);
		final TaskChangeState request = TaskChangeState.newBuilder()
					.setTaskId(taskId).setState(state).build();
		stateChange.stateChange(controller, 
				request, new scRpcCallback(request.getTaskId(), request.getState()));				
	}
	
	class scRpcCallback implements RpcCallback<com.klose.MsConnProto.ConfirmMessage> {
		private String taskId;
		private String state;
		scRpcCallback(String taskId, String state) {
			this.taskId = taskId;
			this.state = state;
		}
		public void run(ConfirmMessage response) {
			boolean reportResponse1 = response.getIsSuccess();
			if(reportResponse1) { 
				LOG.log(Level.INFO, 
					"task-"+ this.taskId + " STATE CHANGE: "+ this.state);
			}
			else {
				
			}
		}
	}
}
