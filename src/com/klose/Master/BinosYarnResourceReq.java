package com.klose.Master;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.protobuf.RpcCallback;
import com.googlecode.protobuf.socketrpc.SocketRpcChannel;
import com.googlecode.protobuf.socketrpc.SocketRpcController;
import com.klose.MsConnProto.NullResponse;
import com.klose.MsConnProto.TaskAmountInfo;
import com.klose.MsConnProto.TaskAmountUpdateService;

public class BinosYarnResourceReq {
	private static final Log LOG = LogFactory.getLog(BinosYarnResourceReq.class);
	@SuppressWarnings("deprecation")
	private SocketRpcChannel channel; 
	private SocketRpcController rpcController;
	private TaskAmountUpdateService updateService;
	BinosYarnResourceReq() {
		try {
			channel = new SocketRpcChannel(InetAddress.getLocalHost().getHostAddress(), MasterArgsParser.getYarnPort());
			rpcController = channel.newRpcController();
			updateService = TaskAmountUpdateService.newStub(channel);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void sendRequest() {
		//Calculate the request args: 
		//required int32 wationgJobNum = 1;
		//required int32 watingTaskNum = 2;
		//required int32 averageTasksNumOneJob = 3;
		LOG.info("send Request to Binos-Yarn");
		TaskAmountInfo info = TaskAmountInfo.newBuilder().setWaitingJobNum(JobScheduler.getWaitingJobNum())
				.setWaitingTaskNum(JobScheduler.getWaitingTaskNum())
				.setAverageTasksNumOneJob(JobScheduler.getAverageTaskOneJob()).build();
		updateService.updateTaskAmount(rpcController, info, new RpcCallback<NullResponse>() {
			@Override
			public void run(NullResponse resp) {
				// TODO Auto-generated method stub
				LOG.info("updateTaskAmount to Binos-Yarn");
			}
			
		});
		LOG.info("Binos-Server status: " + "average tasks number in one job=" + info.getAverageTasksNumOneJob()
				+  " waiting tasks in RunningQueue=" + info.getWaitingTaskNum() 
				+ " waiting jobs in WaitingQueue=" + info.getWaitingJobNum());
	}
}
