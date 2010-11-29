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
import com.klose.MsConnProto.SlaveRegisterInfo;
import com.klose.MsConnProto.SlaveRegisterResponse;
import com.klose.MsConnProto.SlaveUrgentExit;
import com.klose.MsConnProto.UrgentRequest;
import com.klose.MsConnProto.UrgentResponse;
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
class SlaveSendHeartbeatThread extends Thread {
	private SlaveArgsParser parser;
	private SocketRpcChannel channel;
	private SocketRpcController controller;
	private boolean masterReply = false;
	SlaveSendHeartbeatThread(SlaveArgsParser parser, SocketRpcChannel channel, SocketRpcController controller) {
		this.parser = parser;
		this.channel = channel;
		this.controller = controller;
	}
	
	private SlaveInfo getSlaveInfo(String ip_port, String workDir) throws SigarException {
		return new SlaveInfoConstructor(ip_port, workDir).assemble();
	}
	
	public void run() {
		HeartbeatService sendHeartbeat = HeartbeatService.newStub(channel);
		//send heartbeat message periodically 'interval time:5 seconds'.
		while(true) {
			try {
				SlaveInfo heartbeatInfo;
				heartbeatInfo = getSlaveInfo(parser.getIp_port(), parser.getWorkDir());
				sendHeartbeat.heartbeatTrans(controller, heartbeatInfo, new RpcCallback<MasterInfo>(){
					@Override
					public void run(MasterInfo response) {
						// TODO Auto-generated method stub
						masterReply = response.getIsSuccess();
						System.out.println("heartbeatSuccess = "+masterReply);
					}
					});
				this.sleep(5000);
			} catch (SigarException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			
		}
	}
}

class ShutdownThread extends Thread {
	private SlaveArgsParser parser;
	private SocketRpcChannel channel;
	private SocketRpcController controller;
	ShutdownThread(SlaveArgsParser parser, SocketRpcChannel channel, SocketRpcController controller) {
		this.parser = parser;
		this.channel = channel;
		this.controller = controller;
	}
	public void run() {
		SlaveUrgentExit slaveExit = SlaveUrgentExit.newStub(channel);
    	UrgentRequest request = UrgentRequest.newBuilder().
    					setStrData(parser.getIp_port()).build();
		slaveExit.urgentExit(controller, request,  new RpcCallback<UrgentResponse>(){
		@Override
			public void run(UrgentResponse response) {
				// TODO Auto-generated method stub	
				System.out.println("Shutdown Service---Exit handle:  "+
						response.getIsSuccess() + " " + response.getStrData() );
			}
		});
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
		SlaveRegisterInfo registerToMasterRequest =  com.klose.MsConnProto.SlaveRegisterInfo.newBuilder()
		.setIpPort(confParser.getIp_port()).setState(0).build();
		registerToMaster.slaveRegister(rpcController, registerToMasterRequest, 
				new RpcCallback<SlaveRegisterResponse>(){
					@Override
					public void run(SlaveRegisterResponse response) {
						// TODO Auto-generated method stub
						registerSuccess = response.getIsSuccess();
					}
			});
		
		/*If the service of registering to master is successful, it will start the SlaveRPCServerThread
		 * and SlaveSendHeartbeatThread. It also provides the hooks of shutdown. .*/
		if(registerSuccess) {
			SlaveRPCServerThread slaveThreadServer = new SlaveRPCServerThread(confParser, slaveServer);
			slaveThreadServer.start();
			SlaveSendHeartbeatThread sendHeartbeat = new SlaveSendHeartbeatThread(confParser, socketRpcChannel, rpcController);
			sendHeartbeat.start();
			Runtime.getRuntime().addShutdownHook(new ShutdownThread(confParser, socketRpcChannel, rpcController));
		}
		else {
			System.out.println("Slave can't register to Master.\n");
			System.exit(1);
		}		
	} 
	
}
