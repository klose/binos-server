package com.klose.Master;

import java.util.HashMap;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.klose.MsConnProto.ConfirmMessage;
import com.klose.MsConnProto.TransformXMLPath;
import com.klose.MsConnProto.XMLPathTransService;

public class TaskXMLCollector extends XMLPathTransService{
	
	/*taskXMLs  --->  <taskid, path>*/
	private HashMap<String, String> taskXMLs = new HashMap<String, String>();
	
	/*taskStatus ---> <taskid, task status>
	task status : if the value is negative, it represents the task need to run
	a task; if the value is positive, it represents the condition that the task
	depends on has been satisfied. 
	The absolute of task status's value represents the weight of task, and
	the greater its value, the more important the task.
	*/
	private HashMap<String, Integer> taskStatus = new HashMap<String, Integer>();
	@Override
	public void xmlTrans(RpcController controller, TransformXMLPath request,
			RpcCallback<ConfirmMessage> done) {
		// TODO Auto-generated method stub
		ConfirmMessage message = ConfirmMessage.newBuilder().build();
		String path = request.getPath();
		if(!path.substring(0, 4).equals("task")) {
			message = ConfirmMessage.newBuilder().setIsSuccess(false).build();
		}
		else if( !taskXMLs.containsKey((path.substring(5).split("."))[0]) ) {
			//if the taksXMLs doesn't contain the key, it add the key-value to taskXMLs.
			String key = (path.substring(5).split("."))[0];
			taskXMLs.put(key, path);
			taskStatus.put(key, 1);
			message = ConfirmMessage.newBuilder().setIsSuccess(true).build();
		}
		else {
			message = ConfirmMessage.newBuilder().setIsSuccess(false).build();
		}
		done.run(message);
	}
}
