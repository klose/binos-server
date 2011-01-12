package com.klose.Slave;

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.klose.MsConnProto.ConfirmMessage;
import com.klose.MsConnProto.TransformXMLPath;
import com.klose.MsConnProto.XMLPathTransService;

public class TaskXMLCollector extends XMLPathTransService{
	
	private static final Logger LOG = Logger.getLogger(TaskXMLCollector.class.getName()); 
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
		ConfirmMessage message ;
		String path = request.getPath();
		String[] tmp = path.split("/");
		String filename = tmp[tmp.length -1];
		LOG.log(Level.INFO, "path: " + path);
		LOG.log(Level.INFO, "filename: " + filename);
		if(filename.startsWith("job")) {
			
		}
		if(!filename.substring(0, 4).equals("task")) {
			message = ConfirmMessage.newBuilder().setIsSuccess(false).build();
		}
		else if( !taskXMLs.containsKey((filename.substring(5).split("\\."))[0]) ) {
			//if the taksXMLs doesn't contain the key, it add the key-value to taskXMLs.
			String key = (filename.substring(5).split("\\."))[0];
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
