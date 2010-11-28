package com.klose;

import java.net.UnknownHostException;
import java.util.concurrent.Executors;

import org.hyperic.sigar.SigarException;

import com.google.protobuf.RpcCallback;
import com.googlecode.protobuf.socketrpc.SocketRpcChannel;
import com.googlecode.protobuf.socketrpc.SocketRpcController;
import com.googlecode.protobuf.socketrpc.SocketRpcServer;
import com.klose.MsConnProto.HeartbeatService;
import com.klose.MsConnProto.MasterInfo;
import com.klose.MsConnProto.RegisterSlaveService;
import com.klose.MsConnProto.SlaveInfo;
import com.klose.MsConnProto.SlaveRegisterRequest;
import com.klose.MsConnProto.SlaveRegisterResponse;
class SlaveRPCServerThread extends Thread {
	private SlaveArgsParser parser;
	private SocketRpcServer rpcServer;
	SlaveRPCServerThread(SlaveArgsParser parser, SocketRpcServer rpcServer) {
		this.parser =  parser;
		this.rpcServer = rpcServer;
	}
	public void run() {
		this.rpcServer.run();
	}
}
public class Slave {
	//public
	public static boolean registerSuccess = false;
	public static boolean heartbeatSuccess = false;
	public static void main(String [] args) throws UnknownHostException, SigarException{
		
		SlaveArgsParser confParser = new SlaveArgsParser(args);
		confParser.loadValue();
		
		/*RPC server which is responsible for Master's remote execution service*/
		SocketRpcServer slaveServer = new SocketRpcServer(confParser.getPort(),
			    Executors.newFixedThreadPool(5));
		
		/*RPC client channel*/
		SocketRpcChannel socketRpcChannel = new SocketRpcChannel(confParser.getMasterIp(), confParser.getMasterPort());
		SocketRpcController rpcController = socketRpcChannel.newRpcController();
		
		/*register to Master service*/
		RegisterSlaveService registerToMaster = RegisterSlaveService.newStub(socketRpcChannel);
		SlaveRegisterRequest registerToMasterRequest =  com.klose.MsConnProto.SlaveRegisterRequest.newBuilder()
		.setIpPort(confParser.getIp_port()).setState(0).build();
		registerToMaster.slaveRegister(rpcController, registerToMasterRequest, 
				new RpcCallback<SlaveRegisterResponse>(){
					@Override
					public void run(SlaveRegisterResponse response) {
						// TODO Auto-generated method stub
						registerSuccess = response.getIsSuccess();
					}
			});
		/*If the service of registering to master is successful, it will run.*/
		if(registerSuccess) {
			SlaveRPCServerThread slaveThreadServer = new SlaveRPCServerThread(confParser, slaveServer);
			slaveThreadServer.start();
		}
		else {
			System.out.println("Slave can't register to Master.\n");
			System.exit(1);
		}
		
		/*heartbeat*/
		HeartbeatService sendHeartbeat = HeartbeatService.newStub(socketRpcChannel);
		SlaveInfo heartbeatInfo = getSlaveInfo(confParser.getIp_port(), confParser.getWorkDir()); 
		sendHeartbeat.heartbeatTrans(rpcController, heartbeatInfo, new RpcCallback<MasterInfo>(){
			@Override
			public void run(MasterInfo response) {
				// TODO Auto-generated method stub
				heartbeatSuccess = response.getIsSuccess();
				System.out.println("heartbeatSuccess ="+heartbeatSuccess);
				}
			});				
	}
	public  static SlaveInfo getSlaveInfo(String ip_port, String workDir) throws SigarException {
		return new SlaveInfoConstructor(ip_port, workDir).assemble();
	}

	
}
