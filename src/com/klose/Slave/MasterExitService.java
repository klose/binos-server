package com.klose.Slave;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.klose.MsConnProto.ConfirmMessage;
import com.klose.MsConnProto.InformSlaves;
import com.klose.MsConnProto.MasterUrgentExit;

/**
 * MasterExitService: inform all the slaves that master is going to crash.
 * The service is registered by Slave rpc server.
 * @author Bing Jiang
 *
 */
public class MasterExitService extends MasterUrgentExit{

	private SlaveArgsParser confParser;
	private SlaveSendHeartbeatThread hbThread;
	private ConfirmMessage confirm;
	public MasterExitService(SlaveArgsParser confParser, SlaveSendHeartbeatThread hbThread) {
		this.confParser = confParser;
		this.hbThread = hbThread;
	}
	@Override
	public void urgentExit(RpcController controller, InformSlaves request,
			RpcCallback<ConfirmMessage> done) {
		// TODO Auto-generated method stub
		//check that the request's ip_port is of its own, and close the service of  heartbeat.
		if(!request.getIpPort().equals(confParser.getIp_port()) ) {
			confirm = ConfirmMessage.newBuilder().setIsSuccess(false).build();
		}
		else {
			hbThread.stop();
			Slave.registerSuccess = false;
		}
	}
	
}
