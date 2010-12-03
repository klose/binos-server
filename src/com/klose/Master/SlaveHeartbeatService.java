package com.klose.Master;

import java.util.HashMap;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.klose.MsConnProto.HeartbeatService;
import com.klose.MsConnProto.MasterInfo;
import com.klose.MsConnProto.SlaveInfo;
import com.klose.MsConnProto.SlaveRegisterResponse;

public class SlaveHeartbeatService extends HeartbeatService{
	private static HashMap<String, byte[]> slaveInfos = new HashMap<String, byte[]>();
	private MasterInfo response ;
	@Override
	public void heartbeatTrans(RpcController controller, SlaveInfo request,
			RpcCallback<MasterInfo> done) {
		// TODO Auto-generated method stub
		response = MasterInfo.newBuilder()
		.setIsSuccess(updateSlaveInfo(request)).build();
		done.run(response);
	}
	
	public static boolean updateSlaveInfo(SlaveInfo request) {
		String key = request.getIpPort();
		
		if(!RegisterToMasterService.findSlaveEntry(key)) {
			return false;
		}
		else{ 
			//System.out.println(request.toString());
			slaveInfos.put(request.getIpPort(), request.toByteArray());
			return true;
		}
	}	

}
