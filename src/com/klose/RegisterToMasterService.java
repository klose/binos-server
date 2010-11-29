package com.klose;

import java.util.HashMap;
import java.util.HashSet;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.klose.MsConnProto.RegisterSlaveService;
import com.klose.MsConnProto.SlaveRegisterInfo;
import com.klose.MsConnProto.SlaveRegisterResponse;
/*
 * RegisterToMasterService  is code of master.
 * */

public class RegisterToMasterService extends RegisterSlaveService{

	private static HashMap<String,SlaveEntry> slaveEntrys = new HashMap<String,SlaveEntry> ();
	private static SlaveRegisterResponse response ;
	@Override
	public void slaveRegister(RpcController controller,
			SlaveRegisterInfo request,
			RpcCallback<SlaveRegisterResponse> done) {
		// TODO Auto-generated method stub
		// If "ip:port" doesn't exist in the record, add the slave in the list of slave.
		// and response; on the contrary, reform the node that it has been already established 
		// in the master.
		
		System.out.println(request.getIpPort() + " register.");
		
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
	public static boolean addSlaveEntry(SlaveRegisterInfo request) {
		SlaveEntry entry = parse(request);
		if(slaveEntrys.containsKey(request.getIpPort())) {
			return false;
		}
		else {
			slaveEntrys.put(request.getIpPort(),entry);
			return true;
		}
		
	
	}
	/**
	 * 
	 * @param request : parse the related request infomation into
	 * SlaveEntry object. 
	 * @return
	 */
	public static SlaveEntry parse(SlaveRegisterInfo request) {
		String [] ip_port = request.getIpPort().split(":");
		int state = request.getState();
		SlaveEntry res = new SlaveEntry();
		res.setIp(ip_port[0]);
		res.setPort(ip_port[1]);
		res.setState(request.getState());
		res.setLoginTime();
		res.setInfo("");
		return res;
	}
	public static HashMap<String,SlaveEntry> getSlaveEntrys(){
		return slaveEntrys;
	}
	public static  boolean findSlaveEntry(String ip_port) {
		if(slaveEntrys.containsKey(ip_port)){
			return true;
		}
		else {
			return false;
		}
	}
	public static SlaveEntry getSlaveEntry(String ip_port) {
		return slaveEntrys.get(ip_port);
	}
	public static boolean deleteSlaveEntry(String ip_port) {
		if(findSlaveEntry(ip_port)) {
			slaveEntrys.remove(ip_port);
			return true;
		}
		return false;
	}
}
