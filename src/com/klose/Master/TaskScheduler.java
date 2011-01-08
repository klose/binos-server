package com.klose.Master;

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

public class TaskScheduler extends Thread{
	private static final Logger LOG = Logger.getLogger(TaskScheduler.class.getName());
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
	/*test the RPC connection */
	public static void transmitToSlave() {
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
				setTaskIds("1-1-" + index).build();
			atService.allocateTasks(controller, request, 
					new RpcCallback<ConfirmMessage>(){
						@Override
						public void run(ConfirmMessage message) {
							// TODO Auto-generated method stub
							LOG.log(Level.INFO, message.toString());
						}
			});
			index++;
		}
	}
	public static String chooseSlave() {
//		Set<String> keys = RegisterToMasterService.getSlavekeys();
//		if(keys.size() > 0) {
//			System.out.println(keys.toString());
//			Random rand = new Random();
//			int chosedIndex = rand.nextInt() % keys.size();
//			return (String) keys.toArray()[chosedIndex];
//		}
//		else {
//			return null;
//		}
		return "10.5.0.55:6061";
	}
	public static void transmitToSlave(String slaveIpPort) {
		if(slaveIpPort != null) {
			SocketRpcChannel channel = SlaveRPCConnPool
					.getSocketRPCChannel(slaveIpPort);
			SocketRpcController controller = channel.newRpcController();
			AllocateTaskService atService = AllocateTaskService
					.newStub(channel);
			AllocateIdentity request = AllocateIdentity.newBuilder()
					.setSlaveIpPort(slaveIpPort).setTaskIds("1_1_1").build();
			atService.allocateTasks(controller, request,
					new RpcCallback<ConfirmMessage>() {
						@Override
						public void run(ConfirmMessage message) {
							// TODO Auto-generated method stub
							LOG.log(Level.INFO, message.toString());
						}
					});
		}
	}
	public void run() {
		
		while(true) {
			try {
				if(RegisterToMasterService.getSlavekeys().size() > 0) {
					LOG.log(Level.INFO, "schedule the task to slave.");
					transmitToSlave();
				}
				this.sleep(3000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
