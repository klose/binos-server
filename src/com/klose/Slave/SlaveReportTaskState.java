package com.klose.Slave;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.RpcCallback;
import com.googlecode.protobuf.socketrpc.SocketRpcChannel;
import com.googlecode.protobuf.socketrpc.SocketRpcController;
import com.klose.MsConnProto.ConfirmMessage;
import com.klose.MsConnProto.TaskChangeState;
import com.klose.MsConnProto.TaskChangeState.Builder;
import com.klose.MsConnProto.TaskChangeState.outputProperty;
import com.klose.MsConnProto.TaskStateChangeService;
import com.klose.common.TaskDescriptor;
import com.klose.common.TaskState;

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
		if(taskId != null && state != null) {
			final TaskChangeState request;
			Builder builder = TaskChangeState.newBuilder().setTaskId(taskId).setState(state);
			if (state.equals(TaskState.STATES.FINISHED.toString())) {
				TaskDescriptor taskDes = SlaveExecutorManager.getTaskDescriptor(taskId);
				String[] outputPath = taskDes.getOutputPaths().trim().split(" ");
				String taskIdOutput = taskId.split(":")[1] + "::outputPath::";
				for (int i = 0; i < outputPath.length; i++) {
					outputProperty property = outputProperty.newBuilder()
						.setKey(taskIdOutput+i).setValue(outputPath[i]).build();
					builder = builder.addOutput(property);
					System.out.println("kloseklose:::"+ taskIdOutput + i + outputPath[i]);
				}
				
			}
			request = builder.build();
			stateChange.stateChange(controller, 
					request, new scRpcCallback(request.getTaskId(), request.getState()));				
		}
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
