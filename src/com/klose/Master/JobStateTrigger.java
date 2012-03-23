package com.klose.Master;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.klose.common.MSConfiguration;

/**
 * JobStateTrigger is a class used to detect the tasks which is dependent upon 
 * other Running tasks. If the dependent tasks has all finished in a time, it 
 * will change the state of the task from UNPREPARED to PREPARED, and submitted 
 * to the module of TaskScheduler.     
 * @author Bing Jiang
 *
 */
public class JobStateTrigger extends Thread{
	private static final Log LOG = LogFactory.getLog(JobStateTrigger.class);
	private static final int jobStateTriggerThreadWaitTime = MSConfiguration.getJobStateTriggerThreadWaitTime();
	private BinosYarnResourceReq resourceReqService;
	JobStateTrigger() {
		resourceReqService = new BinosYarnResourceReq();
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
		LOG.info("JobStateTrigger starts running...");
		while(true) {
			ConcurrentHashMap<String, JobDescriptor> runningQueue = JobScheduler.getRunningQueue();
//			LOG.log(Level.INFO, "JobStateTrigger: scheduling task.");
			synchronized(runningQueue) {
				for(String jobId :runningQueue.keySet()) {
					if (MasterArgsParser.isEnableBinosYarn() && TaskScheduler.isTaskOverload()) {
						// can't
						resourceReqService.sendRequest();
						try {
							this.sleep(10);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						break;
					}
					JobDescriptor jobDes = runningQueue.get(jobId);
					
					if(jobDes != null) {
						String[] taskPrepared = runningQueue.get(jobId)
								.getPreparedTask();
						/**
						 * when task scheduler has too many tasks on it, it will reject to task scheduling.
						 * && !TaskScheduler.getOverloadTag()
						 */
						if (taskPrepared != null ) {
							for (String taskId : taskPrepared) {
//								System.out.println("########################"
//										+ taskId + "is prepared for scheduling...");
								LOG.debug(taskId + " has been put to scheduling queue.");
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
				this.sleep(jobStateTriggerThreadWaitTime);
				this.yield();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
//			try {
//				this.sleep(100);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
		}
		
	}
	
	
}
