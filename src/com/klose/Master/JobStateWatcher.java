package com.klose.Master;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.googlecode.protobuf.socketrpc.SocketRpcServer;
import com.klose.MsConnProto.ConfirmMessage;
import com.klose.MsConnProto.TaskChangeState;
import com.klose.MsConnProto.TaskChangeState.outputProperty;
import com.klose.MsConnProto.TaskStateChangeService;
import com.klose.common.MSConfiguration;
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
	private Log LOG = LogFactory.getLog(JobStateWatcher.class);
	private static final int jobStateWatcherThreadWaitTime 
			= MSConfiguration.getJobStateWatcherThreadWaitTime();
	public JobStateWatcher(MasterArgsParser confParser, SocketRpcServer masterServer){
		this.confParser = confParser;
		this.masterServer = masterServer;
	}
	public void run() {
		//try {
			TaskChangeWatcher watcherService = new TaskChangeWatcher(JobScheduler.getWatingQueue(), 
					JobScheduler.getRunningQueue());
			this.masterServer.registerService(watcherService);
			LOG.info("JobStateWatcher starts running...");
			while(true) {
				//LOG.log(Level.INFO, "this is a test.");
				JobScheduler.transWaitingToRunning();
				try {
					this.sleep(jobStateWatcherThreadWaitTime);
					this.yield();
				} catch (InterruptedException e) {
				// TODO Auto-generated catch block
					e.printStackTrace();
				}
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
			String taskidPos = request.getTaskId();
			String taskState = request.getState();
			// get the position in the JobQueue.
			
			LOG.debug(taskidPos + " state:" + taskState);
			if(taskidPos != null) {
				TaskStates state = JobScheduler.getTaskStates(taskidPos);
				state.setStates(TaskState.STATES.valueOf(taskState));
				confirmMessage = ConfirmMessage.newBuilder().setIsSuccess(true)
						.build();
				String[] tmp = taskidPos.split(":");
				if (tmp.length != 2) {
					LOG.warn(taskidPos + " is not correct.");
					confirmMessage = ConfirmMessage.newBuilder()
							.setIsSuccess(false).build();
				} else {
					//check whether the job exists. 
					//if the job has already removed from running queue, it will jump out of next action about job. 
					try{
						if (null == runningQueue.get(tmp[0])) {
							LOG.info(tmp[0] + " has already removed from the running queue for Exception." );	
							confirmMessage = ConfirmMessage.newBuilder().setIsSuccess(false)
							.build();
							done.run(confirmMessage);
							return;  
						}
					}catch (NullPointerException e) {
						LOG.error(tmp[0] + " NOT EXISTS!");
						return;
					}
					if (taskState.equals("FINISHED")) {
						if (request.getOutputCount() > 0) {
							for (outputProperty property: request.getOutputList()) {
								JobScheduler.addProperty(tmp[0], property.getKey(), property.getValue());
							}
						}
						JobScheduler.addTaskidFinishedList(taskidPos);
						if (runningQueue.get(tmp[0]).isSuccessful()) {
							LOG.info(tmp[0] + ": FINISHED.");
							JobScheduler.setFinishedTime(tmp[0]);
							JobScheduler.printUsedTime(tmp[0]);
							runningQueue.remove(tmp[0]);
						}
					} else if (taskState.equals("WARNING")
							|| taskState.equals("ERROR")) {
						LOG.error(tmp[0] + "  state:" + taskState);
						JobScheduler.handleExceptionJob(tmp[0]);
					}
					confirmMessage = ConfirmMessage.newBuilder()
							.setIsSuccess(true).build();
				}
			}
			else {
				confirmMessage = ConfirmMessage.newBuilder().setIsSuccess(false)
				.build();
			}
			done.run(confirmMessage);
		}
	}
}
