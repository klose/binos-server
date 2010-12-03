package com.klose.Slave;

import java.io.IOException;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.klose.MsConnProto.ExecOrder;
import com.klose.MsConnProto.ExecOrderResp;
import com.klose.MsConnProto.SlaveOrderExecutorService;

public class SlaveOrderService extends SlaveOrderExecutorService{

	private ExecOrderResp resp ;
	@Override
	public void executeOrder(RpcController controller, ExecOrder request,
			RpcCallback<ExecOrderResp> done)  {
		// TODO Auto-generated method stub
		
		String order = request.getOrder();
		if("".equals(request.getOrder()) ) {
			resp = ExecOrderResp.newBuilder().setIsExecuted(false)
			.setResultMessage("Error: receive a null order.").build();
		}
		else {
			try {
				SlaveOrderExecutor executor = new SlaveOrderExecutor(order);
				executor.execute();
				while(executor.getExitValue() == Integer.MAX_VALUE) {
					continue;
				}
				if(executor.getExitValue() == 0) {
					resp = ExecOrderResp.newBuilder().setIsExecuted(true)
							.setResultMessage(executor.getStdout()).build();
				} 
				else {
					resp = ExecOrderResp.newBuilder().setIsExecuted(false)
							.setResultMessage(executor.getStderr() + "\n"
									+ executor.getStdout()).build();
				}
			
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		done.run(resp);
	}

}
