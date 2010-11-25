package com.klose;

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
	private final static int  port = 12267;
	public static void main(String [] args) {
		 SocketRpcServer masterServer = new SocketRpcServer(port,
				    Executors.newFixedThreadPool(10));
		 masterServer.registerService(new RegisterToMasterService());
		 masterServer.run();
		 
	}
}
