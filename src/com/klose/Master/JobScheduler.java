package com.klose.Master;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
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
	private static ConcurrentHashMap<String, JobDescriptor> runningQueue = new ConcurrentHashMap<String, JobDescriptor>();
	private static final Logger LOG = Logger.getLogger(JobScheduler.class.getName()); 
	private static ConcurrentHashMap<String, String> timeStart 
						= new ConcurrentHashMap<String, String>();
	private static ConcurrentHashMap<String, String> timeFinish 
					= new ConcurrentHashMap<String, String>();
	private static CopyOnWriteArrayList<String> exceptionQueue = 
		new CopyOnWriteArrayList<String> ();

	
	/**set maximum jobs in the running queue statically, 
	on the premise that we can't estimate job's overload and machine ability.*/
	private static final int runningQueueMaxNum = 10;
	
	/**
	 *JobClient submits the job to JobScheduler by submitJob() 
	 */
	public synchronized static void submitJob(String jobid) {
		if(!waitingQueue.contains(jobid)) {
			waitingQueue.add(jobid);
			setStartTime(jobid);
		}
	}
	
	/**Use FIFO schedule strategy.*/
	public synchronized static void transWaitingToRunning() {
		if(runningQueue.size() < runningQueueMaxNum) {
			if(waitingQueue.size() > 0) {
				String jobId = waitingQueue.pop();
				runningQueue.put(jobId, new JobDescriptor(jobId));
				LOG.log(Level.INFO, "Transmit the " + jobId + 
						" from waiting queue to running one.");
			}
		}
	}
	/**
	 * return the jobId from taskIdPos
	 * @param taskIdPos
	 * @return
	 */
	/*public synchronized static String findJobIdInRunningQueue(String taskIdPos) {
		
	}*/
	
	/**
	 * @deprecated
	 * locate the position of the taskid in the running queue.
	 * Because of the relation between jobid and taskid, jobid is like
	 * jobclient_jobindex_creationtime, and taskid is like jobclient_jobindex_taskindex,
	 * so they have the same prefix.Based on these features, find the jobid which the taskid
	 * belongs to, and the index of taskid in the JobDescriptor, which use a colon(:) as separator. 
	 * If it cannot find the taskid ,it will return null.
	 * */
	public synchronized static String searchTaskIdInRunningQueue(String taskId) {
		
		int lastpos = taskId.lastIndexOf("_");
		String prefixTarget = "job-" + taskId.substring(0, lastpos);
		Iterator<String> JobIdIter = runningQueue.keySet().iterator();
		while(JobIdIter.hasNext()) {
			String jobid = JobIdIter.next();
			
			if(jobid.startsWith(prefixTarget)) {
				System.out.println("wwwwwwwwwwwwwwwwwwwwwww"+jobid+"wwwwwwwwwwwwwwwww");
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
		System.out.println("11111111111111111111addTaskidFinishedList" + taskidPos + "2222222222");
		String [] tmp = taskidPos.split(":");
		if(tmp.length != 2) {
			LOG.log(Level.WARNING, taskidPos+ " is not correct.");
			return ;
		}
		System.out.println("11111111111111111111addTaskidFinishedList" + tmp[0] + " " + tmp[1] + "2222222222");
		getJobDescriptor(tmp[0]).
			addFinishedTaskId(tmp[1]);
	}
	/*get the Job in the running queue*/
	public static JobDescriptor getJobDescriptor(String jobId) {
		System.out.println("222222222222222" + jobId + "2222222222");
		return runningQueue.get(jobId);
	}
	/**
	 *
	 * get the TaskStates as to taskidPos. 
	 * @param taskidPos: the format  is like "jobid:taskIndex( index in TasksView)"
	 * @return 
	 */
	public synchronized static TaskStates getTaskStates(String taskidPos) {
		System.out.println("getTaskStates::::"+ taskidPos);
		String [] tmp = taskidPos.split(":");
		if(tmp.length != 2) {
			LOG.log(Level.WARNING, taskidPos+ " is not correct.");
			return null;
		}
		return runningQueue.get(tmp[0]).getTaskStatesByTaskid(tmp[1]);
	}
	public synchronized static void setTaskStates(String taskIdPos, String state) {
		System.out.println("xxxxxxxxxxxxxxxxxxxxxxx" + taskIdPos + " " + state);
		//String taskidPos = searchTaskIdInRunningQueue(taskId);
		System.out.println("xxxxxxxxxxxxxxxxxxxxxxx taskIdPos:" + taskIdPos);
		String [] tmp = taskIdPos.split(":");
		if(tmp.length != 2) {
			LOG.log(Level.WARNING, taskIdPos + " is not correct.");
			return;
		}
		System.out.println(runningQueue.toString());
		System.out.println("xxxxxxxxxxxxxxxxxx" + tmp[0] + ": " + tmp[1]);
		runningQueue.get(tmp[0]).setTaskStates(tmp[1], state);
	}
	public synchronized static LinkedList<String> getWatingQueue() {
		return waitingQueue;
	}
	
	public synchronized static ConcurrentHashMap<String, JobDescriptor> getRunningQueue() {
		return runningQueue;
	}
	
	/**
	 * handle a job with STATE ERROR or WARNING.
	 * when a slave report a task with state "ERROR" or "WARNING" ,
	 * it will add the job to exceptionQueue, and remove the job from
	 * running queue. Stop the Job. 
	 * @param jobId
	 */
	public synchronized static void handleExceptionJob(String jobId) {
		exceptionQueue.add(jobId);
		runningQueue.remove(jobId);
		timeStart.remove(jobId);
	}
	
	public synchronized static void setStartTime(String jobId) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		String time =  sdf.format(new Date());
		LOG.log(Level.INFO, jobId + " start at: " + time);
		timeStart.put(jobId, time);
	}
	public synchronized static void setFinishedTime(String jobId) {
		if(!timeStart.containsKey(jobId)) {
			timeFinish.put(jobId, "statistics is not enough.");
		}
		else {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
			String time = sdf.format(new Date());
			LOG.log(Level.INFO, jobId + " finish at: " + time);
			timeFinish.put(jobId, time);
		}
	}
	
	
	public synchronized static void printUsedTime(String jobId) {
		if(timeStart.containsKey(jobId) && timeFinish.containsKey(jobId)) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
			LOG.log(Level.INFO, jobId + " start at " + timeStart.get(jobId)
					+ "\n" + "finish at " + timeFinish.get(jobId));
		}
	}
	
}
