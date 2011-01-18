package com.klose.Master;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
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
			LOG.log(Level.INFO, "JobStateWatcher starts running...");
			while(true) {
				//LOG.log(Level.INFO, "this is a test.");
				JobScheduler.transWaitingToRunning();
				this.sleep(5000);
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
		private ConcurrentHashMap<String, JobDescriptor> runningQueue;
		TaskChangeWatcher(LinkedList<String> listQueue,  ConcurrentHashMap<String, JobDescriptor> map) {
			this.waitingQueue = listQueue;
			this.runningQueue = map;
		}
		@Override
		public void stateChange(RpcController controller,
				TaskChangeState request, RpcCallback<ConfirmMessage> done) {
			// TODO Auto-generated method stub
			ConfirmMessage confirmMessage = null;
			String taskid = request.getTaskId();
			String taskState = request.getState();
			// get the position in the JobQueue.
			System.out.println("-----------------------------"+taskid+"------");
			System.out.println("-----------------------------"+taskState+"------");
			String taskidPos = JobScheduler.searchTaskIdInRunningQueue(taskid); 
			System.out.println("-----------------------------"+taskidPos+"------");
			TaskStates state = JobScheduler.getTaskStates(taskidPos);
			state.setStates(TaskState.STATES.valueOf(taskState));
			confirmMessage = confirmMessage.newBuilder().setIsSuccess(true)
			.build();
			String [] tmp = taskidPos.split(":");
			if(tmp.length != 2) {
				LOG.log(Level.WARNING, taskidPos + " is not correct.");
				confirmMessage = confirmMessage.newBuilder().setIsSuccess(false)
				.build();
			}
			else {
				if (taskState.equals("FINISHED")) {
					JobScheduler.addTaskidFinishedList(taskidPos);
					if (runningQueue.get(tmp[0]).isSuccessful()) {
						LOG.log(Level.INFO, tmp[0] + ": FINISHED.");
						JobScheduler.setFinishedTime(tmp[0]);
						JobScheduler.printUsedTime(tmp[0]);
						runningQueue.remove(tmp[0]);
					}
				} else if (taskState.equals("WARNING")
						|| taskState.equals("ERROR")) {
					System.out.println("wwwwwwwwwwwwwwwwwwww " + tmp[0] + " "
							+ taskState + "ssssssssssssss");
					JobScheduler.handleExceptionJob(tmp[0]);
				}
			}
			done.run(confirmMessage);
		}
	}
}
