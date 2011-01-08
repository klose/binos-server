package com.klose.Master;

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.googlecode.protobuf.socketrpc.SocketRpcChannel;
import com.googlecode.protobuf.socketrpc.SocketRpcController;

/**
 * SlaveRPCConnPool maintains a pool of SocketRPCChannel which
 * is assigned to the Slave RPC server.
 * Create a new SocketRPCChannel  when each Slave register has 
 * been done.
 * @author Bing Jiang
 *
 */
public class SlaveRPCConnPool {
	private static final Logger LOG = Logger.getLogger(SlaveRPCConnPool.class.getName());
	private static final HashMap<String, SocketRpcChannel> slaveChannels = 
				new HashMap<String, SocketRpcChannel>();
//	private static final HashMap<String, SocketRpcController> slaveControllers =
//				new HashMap<String, SocketRpcController>();
	private SlaveRPCConnPool() {
		
	}
	/*create slave's channel as a slave comes in, and add the channel in the slaveChannels.*/
	public static void addSlave(String slaveIpPort) {
		String[] tmp = slaveIpPort.split(":");
		if(tmp.length != 2) {
			LOG.log(Level.WARNING, "Can't create SocketRpcChannel using " + slaveIpPort);
			return;
		}
		if(!slaveChannels.containsKey(slaveIpPort)) {
			SocketRpcChannel channel = new SocketRpcChannel(tmp[0], Integer.parseInt(tmp[1]));
			slaveChannels.put(slaveIpPort, channel);
		}
	}
	
	public static SocketRpcChannel getSocketRPCChannel(String slaveIpPort) {
		return slaveChannels.get(slaveIpPort);
	}
	public static void removeSlave(String slaveIpPort) {
		// TODO Auto-generated method stub
		slaveChannels.remove(slaveIpPort);
	}
	
	
}
