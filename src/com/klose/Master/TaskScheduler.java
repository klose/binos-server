package com.klose.Master;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.RpcCallback;
import com.googlecode.protobuf.socketrpc.SocketRpcChannel;
import com.googlecode.protobuf.socketrpc.SocketRpcController;
import com.klose.MsConnProto.AllocateIdentity;
import com.klose.MsConnProto.AllocateTaskService;
import com.klose.MsConnProto.ConfirmMessage;
import com.klose.MsConnProto.TState;

public class TaskScheduler {
	private static final Logger LOG = Logger.getLogger(TaskScheduler.class.getName());
	private static String chooseSlaveMethod = "tasks";//use the chooseSlaveByTasks as a default function 
	//	private static SocketRpcChannel[] socketRpcChannel;
//	private static SocketRpcController[] rpcController;
//	private static AllocateTaskService[]  atService;
	
	public TaskScheduler() {
//		Set<String> slaveIds = RegisterToMasterService.getSlavekeys();
//		socketRpcChannel  = new SocketRpcChannel[slaveIds.size()];
//		rpcController = new SocketRpcController[slaveIds.size()];
//		atService = new AllocateTaskService[slaveIds.size()];
//		Iterator<String> iter = slaveIds.iterator();
//		int index = 0;
//		while(iter.hasNext()) {
//			String slaveid = iter.next();
//			String ipPort[] = slaveid.split(":");
//			socketRpcChannel[index] = new SocketRpcChannel(ipPort[0], Integer.parseInt(ipPort[1]));
//			rpcController[index] = socketRpcChannel[index].newRpcController();
//			atService[index] = AllocateTaskService.newStub(socketRpcChannel[index]);
//			index++;
//		}
	}
	
	/**
	 * transmit the taskId to appropriate slave.
	 */
	public static void transmitToSlave(String taskId) throws IOException {
		Set<String> slaveIds = RegisterToMasterService.getSlavekeys();
		Iterator<String> iter = slaveIds.iterator();
		int index = 0;
		while(iter.hasNext()) {
			String slaveId = iter.next();
			SocketRpcChannel channel = SlaveRPCConnPool
			.getSocketRPCChannel(slaveId);
			SocketRpcController controller = channel.newRpcController();
			AllocateTaskService atService = AllocateTaskService.newStub(channel);
			AllocateIdentity request = AllocateIdentity.newBuilder().setSlaveIpPort(slaveId).
				setTaskIds(taskId).build();
			atService.allocateTasks(controller, request, 
					new RpcCallback<TState>(){
						@Override
						public void run(TState message) {
							// TODO Auto-generated method stub
							LOG.log(Level.INFO, message.toString());
						}
			});
			index++;
		}
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
	private static String chooseSlaveByTasks() {
		return null;
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
