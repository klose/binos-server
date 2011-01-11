package com.klose.Master;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.googlecode.protobuf.socketrpc.SocketRpcServer;
import com.klose.MsConnProto.ConfirmMessage;
import com.klose.MsConnProto.TaskChangeState;
import com.klose.MsConnProto.TaskStateChangeService;
import com.klose.common.TaskState;
import com.klose.common.TaskState.STATES;
/**
 * JobStateWatcher is an inner class responsible for watching the running 
 * job's changes, and revising the according jobs' queue.
 * The JobStateWatcher watches the current all jobs in running queue.
 */
public class JobStateWatcher extends Thread{
	private MasterArgsParser confParser;
	private SocketRpcServer masterServer;
	private Logger LOG = Logger.getLogger(JobStateWatcher.class.getName());
	public JobStateWatcher(MasterArgsParser confParser, SocketRpcServer masterServer){
		this.confParser = confParser;
		this.masterServer = masterServer;
	}
	public void run() {
		try {
			TaskChangeWatcher watcherService = new TaskChangeWatcher(JobScheduler.getWatingQueue(), 
					JobScheduler.getRunningQueue());
			this.masterServer.registerService(watcherService);
			while(true) {
				//LOG.log(Level.INFO, "this is a test.");
				JobScheduler.transWaitingToRunning();
				this.sleep(2000);
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
			state.setStates(TaskState.STATES.valueOf(taskState));
			if (request.getState().equals("FINISHED")) {
				JobScheduler.addTaskidFinishedList(taskidPos);
				String [] tmp = taskidPos.split(":");
				if(tmp.length != 2) {
					LOG.log(Level.WARNING, taskidPos + " is not correct.");
				}
				else {
					if(runningQueue.get(tmp[0]).isSuccessful()) {
						runningQueue.remove(tmp[0]);
					}
				}
			}
		}
		
	}
}
