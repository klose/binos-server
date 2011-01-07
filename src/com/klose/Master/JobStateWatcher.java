package com.klose.Master;

import java.util.HashMap;
import java.util.LinkedList;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.googlecode.protobuf.socketrpc.SocketRpcServer;
import com.klose.Master.TaskState.STATES;
import com.klose.MsConnProto.ConfirmMessage;
import com.klose.MsConnProto.TaskChangeState;
import com.klose.MsConnProto.TaskStateChangeService;

public class JobStateWatcher extends Thread{
	private MasterArgsParser confParser;
	private SocketRpcServer masterServer;
	public JobStateWatcher(MasterArgsParser confParser, SocketRpcServer masterServer){
		this.confParser = confParser;
		this.masterServer = masterServer;
		TaskChangeWatcher watcherService = new TaskChangeWatcher(JobScheduler.getWatingQueue(), JobScheduler.getRunningQueue());
		this.masterServer.registerService(watcherService);
	}
	public void run() {
		JobScheduler.transWaitingToRunning();
	}
	/**
	 *TaskChangeWatcher is responsible for receiving the information of 
	 *task's state change, and it revises the related records. 
	 */
	class TaskChangeWatcher extends TaskStateChangeService {
		private LinkedList<String> waitingQueue;
		private HashMap<String, JobDescriptor> runningQueue;
		TaskChangeWatcher(LinkedList<String> listQueue,  HashMap<String, JobDescriptor> map) {
			this.waitingQueue = listQueue;
			this.runningQueue = map;
		}
		@Override
		public void stateChange(RpcController controller,
				TaskChangeState request, RpcCallback<ConfirmMessage> done) {
			// TODO Auto-generated method stub
			String taskid = request.getTaskId();
			String taskState = request.getState();
			// get the position in the JobQueue.
			String taskidPos = JobScheduler.searchTaskIdInRunningQueue(taskid); 
			TaskStates state = JobScheduler.getTaskStates(taskidPos);
			state.setStates(TaskState.STATES.valueOf(request.getState()));
			if (request.getState().equals("FINISHED")) {
				JobScheduler.addTaskidFinishedList(taskidPos);
			}
			
		}
		
	}
}
