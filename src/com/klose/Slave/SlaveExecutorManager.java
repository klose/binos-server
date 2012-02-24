package com.klose.Slave;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.googlecode.protobuf.socketrpc.SocketRpcServer;
import com.klose.MsConnProto.AllocateIdentity;
import com.klose.MsConnProto.AllocateIdentity.JobProperty;
import com.klose.MsConnProto.AllocateTaskService;
import com.klose.MsConnProto.ConfirmMessage;
import com.klose.MsConnProto.TState;
import com.klose.common.MSConfiguration;
import com.klose.common.TaskDescriptor;
import com.klose.common.TaskState;
import com.klose.common.TransformerIO.FileUtility;
import com.transformer.compiler.JobProperties;

/**
 *When master schedules tasks to slave,
 *SlaveExecutorManager is responsible for receiving the tasks allocated by master,
 * launching the task,  monitoring the state of task, and reporting tasks' state changes.      
 * @author Bing Jiang
 *
 */
public class SlaveExecutorManager extends Thread{
	/*taskExecQueue the meaning of map is <taskid:the descriptor of task>*/
	private  static final ConcurrentHashMap<String, TaskDescriptor> taskExecQueue 
			= new ConcurrentHashMap<String, TaskDescriptor>();
	private static final ConcurrentHashMap<String, JobProperties> taskProperties = 
		new ConcurrentHashMap<String, JobProperties>();
	private static final ConcurrentHashMap<String, TaskState.STATES> taskStates 
			= new ConcurrentHashMap<String, TaskState.STATES>();
	private static final ConcurrentHashMap<String, SlaveExecutor> taskExecutors
			= new ConcurrentHashMap<String, SlaveExecutor> ();
	private static final CopyOnWriteArrayList<String> finishTaskList 
			= new CopyOnWriteArrayList<String>();
	private static final CopyOnWriteArrayList<String> waitingTaskQueue 
			= new CopyOnWriteArrayList<String>();
	private static final Map<String, String> jobTmpDirs = new ConcurrentHashMap<String, String>();
	
	//private static int currentTasks = 0;// set number of the tasks
	private static final int slaveExecutorManagerThreadWaitTime 
			= MSConfiguration.getSlaveExecutorManagerThreadWaitTime();
	private static final int maxTasks = MSConfiguration.getMaxTasksOnEachSlave(); 
	private static final Logger LOG = Logger.getLogger(SlaveExecutorManager.class.getName());
	private SocketRpcServer slaveServer;
	private SlaveArgsParser confParser;
	//	private static final HashMap<String, TaskDescriptor> 
	public SlaveExecutorManager(SlaveArgsParser confParser, SocketRpcServer slaveServer) {
		this.confParser = confParser;
		this.slaveServer = slaveServer;
	}
	public void run() {
		TaskAllocateService allocateService = new TaskAllocateService();
		this.slaveServer.registerService(allocateService);
		LOG.log(Level.INFO, "SlaveExecutorManager: start managing the tasks of slave.");
		SlaveReportTaskState reportUtil = new SlaveReportTaskState(confParser); 
		while(true) {
			//try {
			if(taskExecQueue.size() < maxTasks && waitingTaskQueue.size() > 0) {
				String taskIdPos = waitingTaskQueue.remove(0);
				
				runTask(taskIdPos);
			}
			synchronized(taskExecutors) {
					Iterator<String> iter = taskExecutors.keySet().iterator();
					while(iter.hasNext()) {
						String taskId = iter.next();
						TaskState.STATES state = taskStates.get(taskId);
						if( state.equals(taskExecutors.get(taskId).getTaskState()) ) {
							continue;
						}
						else {
							state = taskExecutors.get(taskId).getTaskState();
							System.out.println("report task state:"+ taskId + " "+ state.toString());
							reportUtil.report(taskId, state.toString());
							if(state.equals(TaskState.STATES.FINISHED)) {
								removeTask(taskId);
							}
							else {
								taskStates.put(taskId, state);
							}
						}
					}
				}
			try {
				
				this.sleep(slaveExecutorManagerThreadWaitTime);
				this.yield();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
//				this.sleep(500);	
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
				
		}
	}
	public static void removeTask(String taskId) {
		synchronized(taskExecQueue) {
			taskExecQueue.remove(taskId);
		}
		synchronized(taskExecutors) {
			taskExecutors.remove(taskId);
		}
		synchronized(taskStates) {
			taskStates.remove(taskId);
		}
		synchronized(finishTaskList) {
			finishTaskList.add(taskId);
		}
		synchronized(taskProperties) {
			taskProperties.remove(taskId);
		}
	}
	/**
	 * create the directory used for storing the data.
	 */
	private static void createJobTmpDir(String jobId) {
		if (!jobTmpDirs.containsKey(jobId)) {
			String dirPath = SlaveArgsParser.getWorkDir()+ "/" + jobId;
			File dir = new File(dirPath);
			dir.mkdirs();
			jobTmpDirs.put(jobId, dirPath);
		}
	}
	private static void deleteJobTmpDir(String jobId) {
		if (jobTmpDirs.containsKey(jobId)) {
			File file = new File(jobTmpDirs.get(jobId));
			if (file.isDirectory()) {
				file.delete();
			}
		}
	}
	private static void runTask(String taskIdPos)  {
		String jobId = taskIdPos.substring(0, taskIdPos.lastIndexOf(":"));
		String id =  taskIdPos.substring(taskIdPos.lastIndexOf(":") + 1, taskIdPos.length());
		String taskIdXML = jobId + "/" + id + "/" + id + ".xml";
		LOG.log(Level.INFO, "taskIdXML:" + FileUtility.getHDFSAbsolutePath(taskIdXML));
		//create the tmp directory for the every jobId, and put the location to Properties.
		createJobTmpDir(jobId);
		taskProperties.get(taskIdPos).addProperty("tmpDir", jobTmpDirs.get(jobId) + "/" + id);
		taskProperties.get(taskIdPos).addProperty("taskID", jobId + "-" + id);
		TaskDescriptor taskDes;
		try {
			taskDes = new TaskDescriptor(FileUtility.getHDFSAbsolutePath(taskIdXML));
			taskExecQueue.put(taskIdPos, taskDes);
			SlaveExecutor executor = new SlaveExecutor(taskDes,taskProperties.get(taskIdPos));
			executor.start();
			taskExecutors.put(taskIdPos, executor);
			LOG.log(Level.INFO, "Slave is running task-"+taskIdPos);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	/**
	 * return the TaskDescriptor.
	 * @param taskIdPos : the format is  "jobId:taskId"
	 * @return
	 */
	public static TaskDescriptor getTaskDescriptor(String taskIdPos) {
		return taskExecQueue.get(taskIdPos);
	}
	/**
	 * TaskAllocateService  is a rpc service's class,
	 * and it will extends the AllocateTaskService, which is defined
	 * by google protobuf-socket-rpc service.
	 * receive the task Master allocated.  
	 * @author Bing Jiang
	 *
	 */
	class TaskAllocateService extends AllocateTaskService {
		@Override
		public void allocateTasks(RpcController controller,
				AllocateIdentity request, RpcCallback<TState> done) {
			// TODO Auto-generated method stub
			String taskId = request.getTaskIds();
			TState state = null;
			if(taskStates.containsKey(taskId)) {
				state = TState.newBuilder()
				.setTaskState(taskStates.get(taskId).toString()).build();
				//TODO the progress of task can be reported from here.
				LOG.log(Level.INFO, "Master has requested the state of task-"+taskId + " :" 
						+ state.getTaskState());
			}
			else {
				synchronized(taskStates) {
					waitingTaskQueue.add(taskId);
					taskStates.put(taskId,TaskState.STATES.WAITING);
				LOG.log(Level.INFO, "taskId:" + taskId + "  is submitting into SlaveExecutorManager.");
				JobProperties properties = new JobProperties(taskId);
				for (JobProperty tmp:request.getPropertiesList()) {
					properties.addProperty(tmp.getKey(), tmp.getValue());
				}
				properties.addProperty("self-loc", SlaveArgsParser.getIp_port());
				
				taskProperties.put(taskId, properties);
				
				//String jobId = taskId.substring(0, taskId.lastIndexOf(":"));
//				String id =  taskId.substring(taskId.lastIndexOf(":") + 1, taskId.length());
//				String taskIdXML = jobId + "/" + id + "/" + id + ".xml";
//				LOG.log(Level.INFO, "taskIdXML:" + FileUtility.getHDFSAbsolutePath(taskIdXML));
//				TaskDescriptor taskDes = new TaskDescriptor(FileUtility.getHDFSAbsolutePath(taskIdXML));
//				taskExecQueue.put(taskId, taskDes);
//				SlaveExecutor executor = new SlaveExecutor(taskDes);
//				executor.start();
//				//synchronized(taskExecutors) {
//				taskExecutors.put(taskId, executor);
//				//}
				state = TState.newBuilder()
						.setTaskState(TaskState.STATES.PREPARED.toString()).build();
				LOG.log(Level.INFO, "Slave is scheduling task-"+taskId);
				}
			}
			done.run(state);	
		}
	}	
}
