package com.klose.Master;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;



/**
 * JobScheduler is a core class  used to schedule job from waiting queue to running queue.
 *  
 * @author Bing Jiang
 *
 */
public class JobScheduler {
	//* waitingQueue is used to store the jobs waiting to be added to running queue.
	private static LinkedList<String> waitingQueue = new LinkedList<String>();
	private static HashMap<String, JobDescriptor> runningQueue = new HashMap<String, JobDescriptor>();
	private static final Logger LOG = Logger.getLogger(JobScheduler.class.getName()); 
	/**set maximum jobs in the running queue statically, 
	on the premise that we can't estimate job's overload and machine ability.*/
	private static final int runningQueueMaxNum = 10;
	
	/**
	 *JobClient submits the job to JobScheduler by submitJob() 
	 */
	public synchronized static void submitJob(String jobid) {
		if(!waitingQueue.contains(jobid)) {
			waitingQueue.add(jobid);
		}
		
	}
	
	/**Use FIFO schedule strategy.*/
	public synchronized static void transWaitingToRunning() {
		if(runningQueue.size() < runningQueueMaxNum) {
			if(waitingQueue.size() > 0) {
				String jobId = waitingQueue.pop();
				runningQueue.put(jobId, new JobDescriptor("job-"+jobId));
			}
		}
	}
	
	/* locate the position of the taskid in the running queue.
	 * Because of the relation between jobid and taskid, jobid is like
	 * jobclient_jobindex_creationtime, and taskid is like jobclient_jobindex_taskindex,
	 * so they have the same prefix.Based on these features, find the jobid which the taskid
	 * belongs to, and the index of taskid in the JobDescriptor, which use a colon(:) as separator. 
	 * If it cannot find the taskid ,it will return null.
	 * */
	public static String searchTaskIdInRunningQueue(String taskId) {
		
		int lastpos = taskId.lastIndexOf("_");
		String prefixTarget = taskId.substring(0, lastpos);
		Iterator<String> JobIdIter = runningQueue.keySet().iterator();
		while(JobIdIter.hasNext()) {
			String jobid = JobIdIter.next();
			if(jobid.startsWith(prefixTarget)) {
				int taskIndexInJob = runningQueue.get(jobid).searchTask(taskId);
				if(taskIndexInJob != -1) {
					return jobid + ":" + String.valueOf(taskIndexInJob);
				}
				break;
			}
		}
		return null;
	}
	/*add the finished task index to the JobDescriptor's finishedList*/
	public static void addTaskidFinishedList(String taskidPos) {
		String [] tmp = taskidPos.split(":");
		if(tmp.length != 2) {
			LOG.log(Level.WARNING, taskidPos+ " is not correct.");
			return ;
		}
		getJobDescriptor(tmp[0]).
			addFinishedTaskIndex(Integer.parseInt(tmp[1]));
	}
	/*get the Job in the running queue*/
	public static JobDescriptor getJobDescriptor(String jobId) {
		return runningQueue.get(jobId);
	}
	/**
	 * get the TaskStates as to taskidPos. 
	 * @param taskidPos: the format  is like "jobid:taskIndex( index in TasksView)"
	 * @return 
	 */
	public static TaskStates getTaskStates(String taskidPos) {
		String [] tmp = taskidPos.split(":");
		if(tmp.length != 2) {
			LOG.log(Level.WARNING, taskidPos+ " is not correct.");
			return null;
		}
		return runningQueue.get(tmp[0]).getTaskStates(Integer.parseInt(tmp[1]));
	}
	public static LinkedList<String> getWatingQueue() {
		return waitingQueue;
	}
	
	public static HashMap<String, JobDescriptor> getRunningQueue() {
		return runningQueue;
	}
	/**
	 * JobStateWatcher is an inner class responsible for watching the running 
	 * job's changes, and revising the according jobs' queue.
	 * The JobStateWatcher watches the current all jobs in running queue, and
	 * it will open an 
	 */
	private class JobStateWatcher extends Thread{
		JobStateWatcher() {
			
		}
		public void run() {
			
		}
	}
	
	
}
