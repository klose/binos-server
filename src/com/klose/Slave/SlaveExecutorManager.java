package com.klose.Slave;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.googlecode.protobuf.socketrpc.SocketRpcServer;
import com.klose.MsConnProto.AllocateIdentity;
import com.klose.MsConnProto.AllocateTaskService;
import com.klose.MsConnProto.ConfirmMessage;
import com.klose.MsConnProto.TState;
import com.klose.common.TaskDescriptor;
import com.klose.common.TaskState;
import com.klose.common.TransformerIO.FileUtility;

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
	private static final ConcurrentHashMap<String, TaskState.STATES> taskStates 
			= new ConcurrentHashMap<String, TaskState.STATES>();
	private static final ConcurrentHashMap<String, SlaveExecutor> taskExecutors
			= new ConcurrentHashMap<String, SlaveExecutor> ();
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
						
							/*if(reportUtil.report(taskId, state.toString())){
								/*at now, once the task has been finished,
								it will remove all the details of the task.*/
								/*if(state.equals(TaskState.STATES.FINISHED)) {
									removeTask(taskId);
								}
								else {
									taskStates.put(taskId, state);
								}
							}*/
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
//				this.sleep(500);	
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
				this.yield();
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
				AllocateIdentity request, RpcCallback<TState> done) throws IOException {
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
					taskStates.put(taskId,TaskState.STATES.RUNNING);
				LOG.log(Level.INFO, "taskId:" + taskId);
				String jobId = taskId.substring(0, taskId.lastIndexOf(":"));
				String id =  taskId.substring(taskId.lastIndexOf(":") + 1, taskId.length());
				String taskIdXML = jobId + "/" + id + "/" + id + ".xml";
				LOG.log(Level.INFO, "taskIdXML:" + FileUtility.getHDFSAbsolutePath(taskIdXML));
				TaskDescriptor taskDes = new TaskDescriptor(FileUtility.getHDFSAbsolutePath(taskIdXML));
				taskExecQueue.put(taskId, taskDes);
				SlaveExecutor executor = new SlaveExecutor(taskDes);
				executor.start();
				//synchronized(taskExecutors) {
					taskExecutors.put(taskId, executor);
				//}
				state = TState.newBuilder()
						.setTaskState(TaskState.STATES.RUNNING.toString()).build();
				LOG.log(Level.INFO, "Slave is running task-"+taskId);
				}
			}
			done.run(state);	
		}
	}	
}
