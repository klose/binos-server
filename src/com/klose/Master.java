package com.klose;

import java.net.UnknownHostException;
import java.util.concurrent.Executors;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.googlecode.protobuf.socketrpc.RpcServer;
import com.googlecode.protobuf.socketrpc.SocketRpcServer;
import com.klose.MsConnProto.RegisterSlaveService;
import com.klose.MsConnProto.SlaveRegisterRequest;
import com.klose.MsConnProto.SlaveRegisterResponse;

public class Master{
//	public static class RegisterServiceImpl extends RegisterSlaveService{
//
//		@Override
//		public void slaveRegister(RpcController controller,
//				SlaveRegisterRequest request,
//				RpcCallback<SlaveRegisterResponse> done) {
//			// TODO Auto-generated method stub
//			// If "ip:port" doesn't exist in the record, add the slave in the list of slave.
//			// and response; on the contrary, reform the node that it has been already established 
//			// in the master.
//			SlaveRegisterResponse response = SlaveRegisterResponse.newBuilder()
//			.setIsSuccess(true).build();
//			System.out.println(request.getIpPort());
//			done.run(response);
//			
//		}
//		
//	}
	public static void main(String [] args) throws UnknownHostException {
		MasterArgsParser argsConf = new MasterArgsParser(args); 
		argsConf.loadValue();
		SocketRpcServer masterServer = new SocketRpcServer(argsConf.getPort(),
				    Executors.newFixedThreadPool(10));
		System.out.println("Master started at "+ argsConf.constructIdentity());
		masterServer.registerService(new RegisterToMasterService());
		masterServer.run();
		 
	}
}
