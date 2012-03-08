package com.klose.Master;

import java.util.HashMap;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.klose.MsConnProto.RegisterSlaveService;
import com.klose.MsConnProto.SlaveRegisterInfo;
import com.klose.MsConnProto.SlaveRegisterResponse;
/*
 * RegisterToMasterService  is code of master.
 * */
import com.klose.Slave.Slave;


public class RegisterToMasterService extends RegisterSlaveService{

	private static final Logger LOG = Logger.getLogger(RegisterToMasterService.class.getName()); 
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
		
		String ipPort = request.getIpPort();
		if(slaveEntrys.containsKey(ipPort)) {
			response = SlaveRegisterResponse.newBuilder()
			.setIsSuccess(false).build();
			LOG.log(Level.INFO, ipPort+" has already registered with the master.");
		}
		else {
		/*fault tolerance is undone, please check whehter "ip:port" is validate.*/
	    /*!!!!!!!!!!!!please add checked code.*/
			boolean oper_tmp = addSlaveEntry(request);
			response = SlaveRegisterResponse.newBuilder()
			.setIsSuccess(oper_tmp).build();
			
			if(oper_tmp) {
				LOG.log(Level.INFO, ipPort+" registers with the master.");
			}
			else {
				LOG.log(Level.INFO, ipPort+ " cannot registers with the master.");
			}
		}
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
		String ipPort = request.getIpPort();
		if(slaveEntrys.containsKey(ipPort)) {
			return false;
		}
		else {
			slaveEntrys.put(request.getIpPort(),entry);
			SlaveRPCConnPool.addSlave(ipPort);
			TaskScheduler.registerSlave(ipPort);
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
	public static Set<String> getSlavekeys() {
		return slaveEntrys.keySet();
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
			SlaveRPCConnPool.removeSlave(ip_port);
			TaskScheduler.removeSlave(ip_port);
			return true;
		}
		return false;
	}
}
