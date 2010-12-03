package com.klose.Master;

import java.util.HashMap;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.klose.MsConnProto.SlaveUrgentExit;
import com.klose.MsConnProto.UrgentRequest;
import com.klose.MsConnProto.UrgentResponse;


/*
 * SlaveExitService: handle connection failure of the slave.
 *  
 * */
public class SlaveExitService extends SlaveUrgentExit {

	public static HashMap<String, SlaveEntry> slaveExits = new HashMap<String, SlaveEntry>(); 
	private UrgentResponse response;
	@Override
	public void urgentExit(RpcController controller, UrgentRequest request,
			RpcCallback<UrgentResponse> done) {
		// TODO Auto-generated method stub
		String exitIpPort = request.getStrData();
		if( RegisterToMasterService.findSlaveEntry(exitIpPort) ) {
			SlaveEntry exitSlaveEntry = RegisterToMasterService.getSlaveEntry(exitIpPort);
			exitSlaveEntry.setExitTime();
			exitSlaveEntry.setInfo("exit");
			slaveExits.put(exitIpPort, exitSlaveEntry);
			RegisterToMasterService.deleteSlaveEntry(exitIpPort);
			System.out.println("SlaveExitService: "+ exitIpPort + " exit." );
			/*test whether need to be cloned*/
			response = UrgentResponse.newBuilder().setIsSuccess(true).
						setStrData(exitIpPort + " ---login time: "+ exitSlaveEntry.getLogin_time()
								+ "  ---exit time: " + exitSlaveEntry.getExit_time() ).build();
		}
		else {
			response = UrgentResponse.newBuilder().setIsSuccess(false).
				setStrData("Can't find "+exitIpPort).build();
		}
		done.run(response);
	}
	

}
