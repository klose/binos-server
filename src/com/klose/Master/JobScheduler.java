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
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.transformer.compiler.JobProperties;



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
	private static AtomicLong finishedJobNum = new AtomicLong(0);
	private static AtomicLong finishedTaskNum = new AtomicLong(0);
	private static final Log LOG = LogFactory.getLog(JobScheduler.class); 
	/**
	 * jobRecordStartTime: Job start time format is yyyyMMddHHmmss
	 * jobRecordFinishTime: Job finish time format is yyyyMMddHHmmss
	 * 
	 * jobStartTime: record System.currentMillis()
	 * jobUsedTime: record System.currentMillos() - jobStartTime
	 * 
	 */
	private static ConcurrentHashMap<String, String> jobRecordStartTime 
						= new ConcurrentHashMap<String, String>();
	private static ConcurrentHashMap<String, String> jobRecordFinishDate 
					= new ConcurrentHashMap<String, String>();
	private static ConcurrentHashMap<String, Long> jobStartTime 
						= new ConcurrentHashMap<String, Long>();
	private static ConcurrentHashMap<String, Long> jobUsedTime 
	= new ConcurrentHashMap<String, Long>();
	private static CopyOnWriteArrayList<String> exceptionQueue = 
		new CopyOnWriteArrayList<String> ();

	
	/**set maximum jobs in the running queue statically, 
	on the premise that we can't estimate job's overload and machine ability.*/
	private static final int runningQueueMaxNum = 10;//TODO add this variable to MSConfiguration
	
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
				LOG.info("Transmit the " + jobId + 
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
		LOG.debug("11111111111111111111addTaskidFinishedList" + taskidPos + "2222222222");
		String [] tmp = taskidPos.split(":");
		if(tmp.length != 2) {
			LOG.warn(taskidPos+ " is not correct.");
			return ;
		}
		getJobDescriptor(tmp[0]).
			addFinishedTaskId(tmp[1]);
		finishedTaskNum.incrementAndGet();
	}
	/*get the Job in the running queue*/
	public static JobDescriptor getJobDescriptor(String jobId) {
		LOG.debug("222222222222222" + jobId + "2222222222");
		return runningQueue.get(jobId);
	}
	
	public synchronized static void addProperty(String jobId, String key, String value) {
		runningQueue.get(jobId).addProperty(key, value);
	}
	
	public synchronized static JobProperties getJobProperties(String jobId) {
		return runningQueue.get(jobId).getJobProperties();
	}
	/**
	 *
	 * get the TaskStates as to taskidPos. 
	 * @param taskidPos: the format  is like "jobid:taskIndex( index in TasksView)"
	 * @return 
	 */
	public synchronized static TaskStates getTaskStates(String taskidPos) {
		LOG.info("getTaskStates::::"+ taskidPos);
		String [] tmp = taskidPos.split(":");
		if(tmp.length != 2) {
			LOG.warn(taskidPos+ " is not correct.");
			return null;
		}
		return runningQueue.get(tmp[0]).getTaskStatesByTaskid(tmp[1]);
	}
	public synchronized static void setTaskStates(String taskIdPos, String state) {
		LOG.info(taskIdPos + " " + state);
		String [] tmp = taskIdPos.split(":");
		if(tmp.length != 2) {
			LOG.warn(taskIdPos + " is not correct.");
			return;
		}
		//LOG.debug(runningQueue.toString());
		runningQueue.get(tmp[0]).setTaskStates(tmp[1], state);
	}
	public synchronized static LinkedList<String> getWatingQueue() {
		return waitingQueue;
	}
	
	public synchronized static ConcurrentHashMap<String, JobDescriptor> getRunningQueue() {
		return runningQueue;
	}
	public static int getAverageTaskOneJob() {
		 return (int) (finishedTaskNum.get() / finishedJobNum.get());
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
		jobRecordStartTime.remove(jobId);
		jobStartTime.remove(jobId);
	}
	
	public synchronized static void setStartTime(String jobId) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		String time =  sdf.format(new Date());
		LOG.debug(jobId + " start at: " + time);
		jobRecordStartTime.put(jobId, time);
		jobStartTime.put(jobId, System.currentTimeMillis());
	}
	public synchronized static void setFinishedTime(String jobId) {
		if(!jobRecordStartTime.containsKey(jobId)) {
			jobRecordStartTime.put(jobId, null);
		}
		else {
			jobUsedTime.put(jobId, System.currentTimeMillis() - jobStartTime.get(jobId));
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
			String time = sdf.format(new Date());
			LOG.debug(jobId + " finish at: " + time);
			jobRecordFinishDate.put(jobId, time);
		}
		finishedJobNum.incrementAndGet();
	}
	/**
	 * calculate 
	 * @return
	 */
	public synchronized static int getWaitingTaskNum() {
		int total = 0;
		for (JobDescriptor tmp: runningQueue.values()) {
			total += tmp.unFinishedTaskNum();
		}
		return total;
	}
	public synchronized static int getWaitingJobNum() {
		return waitingQueue.size();
	}
	public synchronized static void printUsedTime(String jobId) {
		if(jobRecordStartTime.containsKey(jobId) && jobRecordFinishDate.containsKey(jobId)) {
			LOG.info(jobId + " start at " + jobRecordStartTime.get(jobId)
					 + "finish at " + jobRecordFinishDate.get(jobId) + " Time used(ms):" + jobUsedTime.get(jobId));
		}
	}
	
}
