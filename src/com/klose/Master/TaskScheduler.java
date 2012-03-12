package com.klose.Master;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.protobuf.RpcCallback;
import com.googlecode.protobuf.socketrpc.SocketRpcChannel;
import com.googlecode.protobuf.socketrpc.SocketRpcController;
import com.klose.MsConnProto.AllocateIdentity;
import com.klose.MsConnProto.AllocateIdentity.JobProperty;
import com.klose.MsConnProto.AllocateIdentity.Builder;
import com.klose.MsConnProto.AllocateTaskService;
import com.klose.MsConnProto.ConfirmMessage;
import com.klose.MsConnProto.TState;

public class TaskScheduler {
	private static final Log LOG = LogFactory.getLog(TaskScheduler.class);
	private static String chooseSlaveMethod = "tasks";//use the chooseSlaveByTasks as a default function 
	
	/**slaveTaskNum : make a statistics about the the tasks running in slave
	 * key:value  slaveid : the number of tasks in slave
	*/
	private static ConcurrentHashMap<String, Integer> slaveTaskNum = new ConcurrentHashMap<String, Integer>();  
	
	/**
	 * runningTaskSlave: record which machines the running tasks run.
	 * key:value   taskid:slaveId
	 */
	private static ConcurrentHashMap<String, String> runningTaskOnSlave  = new ConcurrentHashMap<String, String> (); 
	
	/**
	 * If the number of tasks in one slave does not reach the tasksOnSlaveMin,
	 * new task can be allocated to the slave.
	 * If the number of tasks in all slaves  reach the tasksOnSlaveMin, 
	 * new task will be allocated to the slave with least number of tasks. 
	 */
	private static int tasksOnSlaveMin = 1;
	private static int minTasksNum = Integer.MAX_VALUE;
	
	private TaskScheduler() {
		
	}
	
	public synchronized static void registerSlave(String slaveId) {
		if(!slaveTaskNum.containsKey(slaveId)) {
			slaveTaskNum.put(slaveId, 0);
		}
	}
	public synchronized static void addTaskNum(String slaveId) {
		Integer num = slaveTaskNum.get(slaveId);
		if(num != null) {
			num ++;
		}
		else {
			LOG.warn("Cannot find " + slaveId);
		}
		slaveTaskNum.put(slaveId, num);
	}
	public synchronized static void decreaseTaskNum(String slaveId) {
		Integer num = slaveTaskNum.get(slaveId);
		if(num != null) {
			num--;
		}
		else {
			LOG.warn("Cannot find " + slaveId);
		}
		if(num < 0) {
			LOG.warn(slaveId + " occurs a error.");
			num = 0;
		}
		slaveTaskNum.put(slaveId, num);
	}
	public synchronized static void removeSlave(String slaveId) {
		slaveTaskNum.remove(slaveId);
	}
	
	
	public synchronized static void recordTaskIdSlaveId(String taskId, String slaveId) {
		runningTaskOnSlave.put(taskId, slaveId);
	}
	
	public synchronized static void removeTaskIdOnSlave(String taskId) {
		runningTaskOnSlave.remove(taskId);
		String slaveId = getSlaveId(taskId);
		if(slaveId != null) {
			decreaseTaskNum(slaveId);
		}
	}
	
	public synchronized static String getSlaveId(String taskId) {
		return runningTaskOnSlave.get(taskId);
	}
	/**
	 * transmit the taskId to appropriate slave.
	 */
	public static void transmitToSlave(String taskId) throws IOException {
		LOG.debug("transmitToSlave"+ taskId);
		String slaveId = chooseSlave(taskId);
		String [] id = taskId.split(":");
		if(id.length != 2) {
			LOG.error("ERROR: " + taskId + " should use format jobId:taskId");
			return ;
		}
		/**
		 * use another method to get channel to compare the perfermance.
		 */
//		SocketRpcChannel channel = SlaveRPCConnPool
//				.getSocketRPCChannel(slaveId);
		String slaveIpPort [] = slaveId.split(":");
		
		if(slaveIpPort.length != 2) {
			LOG.error("ERROR: " + slaveId);
			return ;
		}
		/*add the information of scheduling to JobProperties.*/
		JobScheduler.addProperty(id[0], id[1], slaveId);
		SocketRpcChannel channel = new SocketRpcChannel(slaveIpPort[0], Integer.parseInt(slaveIpPort[1])) ;
		SocketRpcController controller = channel.newRpcController();
		AllocateTaskService atService = AllocateTaskService.newStub(channel);
		AllocateIdentity request; 
		Builder builder = AllocateIdentity.newBuilder().setSlaveIpPort(slaveId).setTaskIds(taskId);
		Map<String, String> proMap = JobScheduler.getJobProperties(id[0]).getAllProperties();
		for (String tmp : proMap.keySet()) {
			JobProperty testPro = JobProperty.newBuilder().setKey(tmp).setValue(proMap.get(tmp)).build();	
			builder = builder.addProperties(testPro);
		}
		request = builder.build();
		LOG.info("taskId:" + taskId + " scheduled to  " + "slaveId:" + slaveId);	
		atService.allocateTasks(controller, request, new RpcCallback<TState>() {
			@Override
			public void run(TState message) {
				// TODO Auto-generated method stub
				LOG.info(message.toString());
			}
		});
		recordTaskIdSlaveId(taskId, slaveId);
		addTaskNum(slaveId);
		reviseTaskState(taskId, "RUNNING");
	}
	
	/**
	 * When submitting a job 
	 * @param taskId
	 */
	public synchronized static void reviseTaskState(String taskId, String state) {
		JobScheduler.setTaskStates(taskId, state);
	}
	/**
	 * choose an appropriate slave as to chooseSlaveMethod.
	 * @param taskId
	 * @return
	 */
	public static String chooseSlave(String taskId) {
		String res = null;
		if (chooseSlaveMethod.equals("tasks")) {
			res = chooseSlaveByTasks();
		}
		else if (chooseSlaveMethod.equals("CPURatio")) {
			res = chooseSlaveByCPURatio();
		}
		else if (chooseSlaveMethod.equals("MEMUsage")) {
			res = chooseSlaveByMEMUsage();
		}
		else if (chooseSlaveMethod.equals("NetworkUsage")) {
			res = chooseSlaveByNetworkUsage();
		}
		else if (chooseSlaveMethod.equals("Random")) {
			res = chooseSlaveByRandom();
		}
		else if (chooseSlaveMethod.equals("DiskUsage")) {
			res = chooseSlaveByDiskUsage();
		}
		else if (chooseSlaveMethod.equals("DataLocality")) {
			res = chooseSlaveByDataLocality(taskId);
		}
		else if (chooseSlaveMethod.equals("CPUMHz")) {
			res = chooseSlaveByCPUMHz();
		}
		else if (chooseSlaveMethod.equals("CPUfactor")) {
			res = chooseSlaveByCPUfactor();
		}
		else {
			res = null;
		}
		return res;
	}
	/**
	 * choose a slave as to the ratio of average cpu usage.
	 * A slave with lowest ration of cpu usage will be choosen. 
	 * @return slave-ip: slave-port 
	 */
	private static String chooseSlaveByCPURatio() {
		return null;
	}
	
	/**
	 * choose a slave as to the memeory usage.
	 * A slave with most free space of memory will be choosen. 
	 * @return slave-ip: slave-port 
	 */
	private static String chooseSlaveByMEMUsage() {
		return null;
	}
	
	/**
	 * choose a slave as to the ratio of average network bandwidth usage.
	 * A slave with lowest network flow usage will be choosen. 
	 * @return slave-ip: slave-port 
	 */
	private static String chooseSlaveByNetworkUsage() {
		return null;
	}
	
	/**
	 * choose a slave as to the total number of running-task in slaves .
	 * A slave with lowest number of tasks will be choosen. 
	 * @return slave-ip: slave-port 
	 */
	private synchronized static String chooseSlaveByTasks() {
		Set<String> slaveIds = slaveTaskNum.keySet();
		
		Iterator<String> iter = slaveIds.iterator();
		String minSlaveId = iter.next();
		int minnum = slaveTaskNum.get(minSlaveId);
		while(iter.hasNext()) {
			String slaveId = iter.next();
			int num = slaveTaskNum.get(slaveId) ;
			if(num < minnum) {
				return slaveId;
			}
		}
		return minSlaveId;
	}
	
	/**
	 * It will choose a slave using a random algorithm.
	 * @return slave-ip:slave-port
	 */
	private static String chooseSlaveByRandom() {
		return null;
	}
	
	/**
	 * choose a slave as to the ratio of average disk usage.
	 * A slave with most free space of disk will be choosen. 
	 * @return slave-ip: slave-port 
	 */
	private static String chooseSlaveByDiskUsage() {
		return null;
	}
	
	/**
	 * choose a slave as to the locality of the data the task handles.
	 * The number of input file may be more than one.Using a function
	 * to calculate the most efficient slave.   
	 * @param taskId
	 * @return slave-ip:slave-port
	 */
	private static String chooseSlaveByDataLocality(String taskId) {
		return null;
	}
	
	/**
	 * In a heterogeneous environment, some nodes have different power of processing.
	 * Choose a slave as to the CPU mhz.
	 * @return slave-ip:slave-port
	 */
	private static String chooseSlaveByCPUMHz() {
		return null;
	}
	
	/**
	 * Considering cpu cores, cpu MHZ, the ration of cpu usage, and so on.
	 * Calculate a value using a formula containing the factor above, and
	 * choose a best slave.
	 * @return slave-ip:slave-port
	 */
	private static String chooseSlaveByCPUfactor() {
		return null;
	}
	
	
//	public static void transmitToSlave(String slaveIpPort) {
//		if(slaveIpPort != null) {
//			SocketRpcChannel channel = SlaveRPCConnPool
//					.getSocketRPCChannel(slaveIpPort);
//			SocketRpcController controller = channel.newRpcController();
//			AllocateTaskService atService = AllocateTaskService
//					.newStub(channel);
//			AllocateIdentity request = AllocateIdentity.newBuilder()
//					.setSlaveIpPort(slaveIpPort).setTaskIds("1_1_1").build();
//			try {
//				atService.allocateTasks(controller, request,
//						new RpcCallback<TState>() {
//							@Override
//							public void run(TState message) {
//								// TODO Auto-generated method stub
//								LOG.log(Level.INFO, message.toString());
//							}
//						});
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
//	}
//	public void run() {
		
//		while(true) {
//			try {
//				if(RegisterToMasterService.getSlavekeys().size() > 0) {
//					LOG.log(Level.INFO, "schedule the task to slave.");
//					try {
//						transmitToSlave();
//					} catch (IOException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//				}
//				this.sleep(3000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
//	}
}
