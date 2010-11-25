package com.klose;

import java.util.HashSet;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.klose.MsConnProto.RegisterSlaveService;
import com.klose.MsConnProto.SlaveRegisterRequest;
import com.klose.MsConnProto.SlaveRegisterResponse;

public class RegisterToMasterService extends RegisterSlaveService{

	private static HashSet<SlaveEntry> slaveEntrys = new HashSet<SlaveEntry> ();
	private static SlaveRegisterResponse response ;
	@Override
	public void slaveRegister(RpcController controller,
			SlaveRegisterRequest request,
			RpcCallback<SlaveRegisterResponse> done) {
		// TODO Auto-generated method stub
		// If "ip:port" doesn't exist in the record, add the slave in the list of slave.
		// and response; on the contrary, reform the node that it has been already established 
		// in the master.
		
		System.out.println(request.getIpPort());
		
		/*fault tolerance is undone, please check whehter "ip:port" is validate.*/
	    /*!!!!!!!!!!!!please add checked code.*/
		
		response = SlaveRegisterResponse.newBuilder()
		.setIsSuccess(addSlaveEntry(request)).build();
		done.run(response);
	}
	/**
	 * 
	 * @param request : parse the infomation from request, 
	 * construct a new SlaveRegisterRequest object,
	 * add to the slaveEntrys.
	 * @return If slaveEntry doesn't exist in the record, add the slave in the list of slaveEntrys.
	 *  and return true; on the contrary, return false.
	 */
	public static boolean addSlaveEntry(SlaveRegisterRequest request) {
		SlaveEntry entry = parse(request);
		if(slaveEntrys.contains(entry)) {
			return false;
		}
		else {
			slaveEntrys.add(entry);
			return true;
		}
		
	
	}
	/**
	 * 
	 * @param request : parse the related request infomation into
	 * SlaveEntry object. 
	 * @return
	 */
	public static SlaveEntry parse(SlaveRegisterRequest request) {
		String [] ip_port = request.getIpPort().split(":");
		int state = request.getState();
		SlaveEntry res = new SlaveEntry();
		res.setIp(ip_port[0]);
		res.setPort(ip_port[1]);
		res.setState(request.getState());
		res.setInfo("");
		return res;
	}
}
