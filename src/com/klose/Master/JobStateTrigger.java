package com.klose.Master;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * JobStateTrigger is a class used to detect the tasks which is dependent upon 
 * other Running tasks. If the dependent tasks has all finished in a time, it 
 * will change the state of the task from UNPREPARED to PREPARED, and submitted 
 * to the module of TaskScheduler.     
 * @author Bing Jiang
 *
 */
public class JobStateTrigger extends Thread{
	private static final Logger LOG = Logger.getLogger(JobStateTrigger.class.getName());
	JobStateTrigger() {
	
	}
//	public void triggerNode(String taskIdPos, String state) {
//		TaskStates tss = JobScheduler.getTaskStates(taskIdPos);
//		if(tss.getState().toString().equals(state)) {
//			if(state.equals("Finished")) {
//				
//			}
//		}
//	}
	
	public void run() {
		LOG.log(Level.INFO, "JobStateTrigger starts running...");
		while(true) {
			ConcurrentHashMap<String, JobDescriptor> runningQueue = JobScheduler.getRunningQueue();
			synchronized(runningQueue) {
				for(String jobId :runningQueue.keySet()) {
					JobDescriptor jobDes = runningQueue.get(jobId);
					if(jobDes != null) {
						String[] taskPrepared = runningQueue.get(jobId)
								.getPreparedTask();
						// System.out.println("########################taskPrepared"
						// + taskPrepared.length + "############");
						// System.out.println("########################taskPrepared"
						// + taskPrepared[0] + "############");
						if (taskPrepared != null) {
							for (String taskId : taskPrepared) {
								System.out.println("########################"
										+ taskId + "is prepared for scheduling...");
								try {
									TaskScheduler.transmitToSlave(jobId+":"+taskId);
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
						}
					}
				}
			}
			try {
				this.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	
}
