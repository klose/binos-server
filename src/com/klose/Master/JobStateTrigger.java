package com.klose.Master;

import java.io.IOException;
import java.util.HashMap;
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
			HashMap<String, JobDescriptor> runningQueue = JobScheduler.getRunningQueue();
			synchronized(runningQueue) {
				for(String jobId :runningQueue.keySet()) {
					String [] taskPrepared = runningQueue.get(jobId).getPreparedTask();
					for(String taskId: taskPrepared) {
						try {
							TaskScheduler.transmitToSlave(taskId);	
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
			try {
				this.sleep(3000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	
}
