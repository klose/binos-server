package com.klose.Master;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.googlecode.protobuf.socketrpc.SocketRpcChannel;
import com.googlecode.protobuf.socketrpc.SocketRpcController;
import com.klose.MsConnProto.ExecOrder;
import com.klose.MsConnProto.ExecOrderResp;
import com.klose.MsConnProto.SlaveOrderExecutorService;
import com.klose.Slave.SlaveState;
/**
 * SimpleOrderExecService: the service of master to provide the simple order
 * executor engine. It will retrieve the slave nodes which enable simple order Executing,
 * and invoke the service individually. 
 * 
 * @author Bing Jiang
 *
 */
public class SimpleOrderExecService extends SlaveOrderExecutorService{
	
	private String order = ""; 
	private String slave = "";
	private HashMap<String,ExecResult> orderExecRet = new HashMap<String, ExecResult>();
	private ExecOrderResp respAll ;
	SimpleOrderExecService() {
		
	}
	@Override
	public void executeOrder(RpcController controller, ExecOrder request,
			RpcCallback<ExecOrderResp> done) {
		// TODO Auto-generated method stub
		order = request.getOrder();	
		if(order.equals("")) {
			ExecOrderResp error = ExecOrderResp.newBuilder().setIsExecuted(false)
							.setResultMessage("Error: the order that needs to be executed " +
									"does not exist.").build();
			done.run(error);
		}
		
		else {
			distributeExecOrder();
			assembleAllRet();
			done.run(respAll);
		}
	}
	
	/**
	 * assembleAllRet simple combine all the result. If needs 
	 * more operation, please extends the class and override the function. 
	 * The resultSet can be retrieved by the function by getOrderExecRet().
	 */
	public void assembleAllRet() {
		boolean  allSucceed = true;
		String allResult = "";
		Set<String> keySet = orderExecRet.keySet();
		Iterator<String> setIter = keySet.iterator();
		while(setIter.hasNext()) {
			String slaveAddr = setIter.next();
			ExecResult ret = orderExecRet.get(slaveAddr);
			allResult += slaveAddr + "\n";
			allSucceed = allSucceed && ret.isExecSucceed();
			allResult += ret.getResponse()+ "\n\n";
		}
		this.respAll = ExecOrderResp.newBuilder()
				.setIsExecuted(allSucceed).setResultMessage(allResult).build();
							
	}
	
	public void distributeExecOrder() {
		Set<String> choosedSlaves = chooseSlaveExecNode();
		int len = choosedSlaves.size();
		Iterator<String> iter = choosedSlaves.iterator();
		while( iter.hasNext() ) {
			slave = iter.next().trim();
			String slaveIpPort[] = slave.split(":");
			SocketRpcChannel socketRpcChannel = new SocketRpcChannel(slaveIpPort[0], Integer.parseInt(slaveIpPort[1]));
			SocketRpcController rpcController = socketRpcChannel.newRpcController();
			SlaveOrderExecutorService slaveOrderServ = SlaveOrderExecutorService.newStub(socketRpcChannel);
			ExecOrder order = ExecOrder.newBuilder().setOrder(this.order).build();
			slaveOrderServ.executeOrder(rpcController, order, new RpcCallback<com.klose.MsConnProto.ExecOrderResp>(){
				@Override
				public void run(ExecOrderResp resp) {
					// TODO Auto-generated method stub
					orderExecRet.put(slave, new ExecResult(resp.getIsExecuted(),
							resp.getResultMessage()));
				}
				
			});
		}
	}
	/*
	 * chooseSlaveExecNode: choose the appropriate node that fits the requirements.
	 * currently, it chooses all the nodes that satisfy the needs , the analysis of 
	 * the job or order is doing nothing. 
	 * 
	 * return Set<String> : the ip:port of nodes that satisfy the needs. 
	 *  
	 */
	public Set<String> chooseSlaveExecNode() {
		HashMap<String,SlaveEntry> slaveNodes = RegisterToMasterService.getSlaveEntrys();
		Set<String> slaveSet = new HashSet<String>();
		int needNodes = analysisOrder(this.order);
		if( needNodes > 0) {
			int choosedNum = 0;
			Iterator<String> slaveIpPort = slaveNodes.keySet().iterator();
			while(slaveIpPort.hasNext()) {
				String ipPort = slaveIpPort.next();
				if (SlaveState.SIMPLE_ORDER_EXEC == slaveNodes.get(ipPort).getState()) {
					choosedNum ++;
					slaveSet.add(ipPort);
					if(choosedNum == needNodes) {
						break;
					}
				}
			}
		}
		else {
			// choose all the approriate nodes
			Iterator<String> slaveIpPort = slaveNodes.keySet().iterator();
			while(slaveIpPort.hasNext()) {
				String ipPort = slaveIpPort.next();
				if (SlaveState.SIMPLE_ORDER_EXEC == slaveNodes.get(ipPort).getState()) {
					slaveSet.add(ipPort);
				}
			}
		}
		return slaveSet;
	}
	/**
	 * decide how many machines the order needs.
	 * @param order
	 * @return the number of the machines. if the value of return is -1, it means
	 * every node should be used.
	 */
	public int analysisOrder(String order) {
		return -1;
	}
	
}
