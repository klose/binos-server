package com.klose.Master;

import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Executors;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.googlecode.protobuf.socketrpc.RpcServer;
import com.googlecode.protobuf.socketrpc.SocketRpcChannel;
import com.googlecode.protobuf.socketrpc.SocketRpcController;
import com.googlecode.protobuf.socketrpc.SocketRpcServer;
import com.klose.MsConnProto.ConfirmMessage;
import com.klose.MsConnProto.InformSlaves;
import com.klose.MsConnProto.MasterUrgentExit;


class MasterRpcServerThread extends Thread {
	private MasterArgsParser confParser;
	private SocketRpcServer masterServer;
	public MasterRpcServerThread(MasterArgsParser confParser, SocketRpcServer masterServer) {
		this.confParser = confParser;
		this.masterServer = masterServer;
	}
	public void run() {
		RegisterToMasterService registerToMaster = new RegisterToMasterService();
		masterServer.registerService(registerToMaster);
		SlaveHeartbeatService heartbeatService = new SlaveHeartbeatService();
		masterServer.registerService(heartbeatService);
		SlaveExitService slaveExitService = new SlaveExitService();
		masterServer.registerService(slaveExitService);		
		TaskXMLCollector collector = new TaskXMLCollector();
		masterServer.registerService(collector);
		//this simple order executor is initial version.
		SimpleOrderExecService soes = new SimpleOrderExecService(); 
		masterServer.registerService(soes);
		masterServer.run();
	}
}
class ShutdownSlaveThread extends Thread {
	private MasterArgsParser parser;
	private Set<String> keySet = null;
	private String slave = "";
	private SocketRpcServer server;
	ShutdownSlaveThread(MasterArgsParser parser) {
		this.parser = parser;
	}
	ShutdownSlaveThread(MasterArgsParser parser, SocketRpcServer server) {
		this.parser = parser;
		this.server = server;
	}
	public void run() {
		this.server.shutDown();
	}
//	public void run() {
//		this.keySet = RegisterToMasterService.getSlavekeys();
//		Iterator<String> iter = this.keySet.iterator();
//		while( iter.hasNext() ) {
//			slave = iter.next().trim();
//			String slaveIpPort[] = slave.split(":");
	
//			SocketRpcChannel socketRpcChannel = new SocketRpcChannel(slaveIpPort[0], Integer.parseInt(slaveIpPort[1]));
//			SocketRpcController rpcController = socketRpcChannel.newRpcController();
//			
//			MasterUrgentExit masterExit = MasterUrgentExit.newStub(socketRpcChannel);
//			
//			InformSlaves inform = InformSlaves.newBuilder().setIpPort(slave).build();
//			
//			masterExit.urgentExit(rpcController, inform, new RpcCallback<com.klose.MsConnProto.ConfirmMessage>(){
//				public void run(ConfirmMessage response) {
//					System.out.println("Master Shutdown Slave Service--- Slave Exit handle:  "+ slave
//							+ " close the heartbeat service ---  "
//							+ response.getIsSuccess()  );
//					
//				}
//			});
//		}
//	}		
}	
public class Master{
	public static void main(String [] args) throws UnknownHostException {
		MasterArgsParser confParser = new MasterArgsParser(args); 
		confParser.loadValue();
		SocketRpcServer masterServer = new SocketRpcServer(confParser.getPort(),
				    Executors.newFixedThreadPool(10));
		System.out.println("Master started at "+ confParser.constructIdentity());
		MasterRpcServerThread masterServerThread = new MasterRpcServerThread(
				confParser, masterServer);
		masterServerThread.start();
		JobStateWatcher watcherThread = new JobStateWatcher(confParser, masterServer);
		watcherThread.start();
		Runtime.getRuntime().addShutdownHook(new ShutdownSlaveThread(confParser, masterServer));
	}
}
