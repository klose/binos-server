package com.klose.Slave;

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.googlecode.protobuf.socketrpc.SocketRpcServer;
import com.klose.MsConnProto.AllocateIdentity;
import com.klose.MsConnProto.AllocateTaskService;
import com.klose.MsConnProto.ConfirmMessage;
import com.klose.Master.TaskState;

/**
 *When master schedules tasks to slave,
 *SlaveExecutorManager is responsible for receiving the tasks allocated by master,
 * launching the task,  monitoring the state of task, and reporting tasks' state changes.      
 * @author Bing Jiang
 *
 */
public class SlaveExecutorManager extends Thread{
	/*taskExecQueue the meaning of map is <taskid:the descriptor of task>*/
	private static final HashMap<String, TaskDescriptor> taskExecQueue 
			= new HashMap<String, TaskDescriptor>();
	private static final HashMap<String, TaskState.STATES> taskStates 
			= new HashMap<String, TaskState.STATES>();
	private static final Logger LOG = Logger.getLogger(SlaveExecutorManager.class.getName());
	private SocketRpcServer slaveServer;
	private SlaveArgsParser confParser;
	//	private static final HashMap<String, TaskDescriptor> 
	public SlaveExecutorManager(SlaveArgsParser confParser, SocketRpcServer slaveServer) {
		this.confParser = confParser;
		this.slaveServer = slaveServer;
		AllocateTaskService allocateService = new TaskAllocateService();
		this.slaveServer.registerService(allocateService);
	}
	public void run() {
		while(true) {
			LOG.log(Level.INFO, "SlaveExecutorManager: manage the slave.");
			try {
				this.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public static void addTask(String taskId) {
		
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
				AllocateIdentity request, RpcCallback<ConfirmMessage> done) {
			// TODO Auto-generated method stub
			ConfirmMessage message = ConfirmMessage.newBuilder()
							.setIsSuccess(true).build();
			done.run(message);
			LOG.log(Level.INFO, "Slave can receive the tasks Master allocated.");
		}
		
	}
	
}
