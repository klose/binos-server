package com.klose.Master;

import java.util.Iterator;
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
	private static SocketRpcChannel[] socketRpcChannel;
	private static SocketRpcController[] rpcController;
	private static AllocateTaskService[]  atService;
	public TaskScheduler() {
		Set<String> slaveIds = RegisterToMasterService.getSlavekeys();
		socketRpcChannel  = new SocketRpcChannel[slaveIds.size()];
		rpcController = new SocketRpcController[slaveIds.size()];
		atService = new AllocateTaskService[slaveIds.size()];
		Iterator<String> iter = slaveIds.iterator();
		int index = 0;
		while(iter.hasNext()) {
			String slaveid = iter.next();
			String ipPort[] = slaveid.split(":");
			socketRpcChannel[index] = new SocketRpcChannel(ipPort[0], Integer.parseInt(ipPort[1]));
			rpcController[index] = socketRpcChannel[index].newRpcController();
			atService[index] = AllocateTaskService.newStub(socketRpcChannel[index]);
			index++;
		}
	}
	//test the RPC connection 
	public static void transmitToSlave() {
		Set<String> slaveIds = RegisterToMasterService.getSlavekeys();
		Iterator<String> iter = slaveIds.iterator();
		int index = 0;
		while(iter.hasNext()) {
			AllocateIdentity request = AllocateIdentity.newBuilder().setSlaveIpPort(iter.next()).
				setTaskIds(0, "1-1-" + index).setTaskIds(1, "1-1-" + (index + 1)).build();
			atService[index].allocateTasks(rpcController[index], request, 
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
	public void run() {
		
		while(true) {
			try {
				LOG.log(Level.INFO, "schedule the task to slave.");
				transmitToSlave();
				this.sleep(3000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
