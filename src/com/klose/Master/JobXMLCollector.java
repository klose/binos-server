package com.klose.Master;

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.klose.MsConnProto.ConfirmMessage;
import com.klose.MsConnProto.TransformXMLPath;
import com.klose.MsConnProto.XMLPathTransService;

public class JobXMLCollector extends XMLPathTransService{
	
	private static final Log LOG = LogFactory.getLog(JobXMLCollector.class); 
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
		LOG.debug("path: " + path);
		String[] tmp = path.split("/");
		String filename = tmp[tmp.length -1];
		LOG.debug("filename: " + filename);
		String jobId = filename.substring(0, filename.lastIndexOf(".xml"));
		LOG.debug("jobId:" + jobId);
		if(!filename.startsWith("job")) {
			message = ConfirmMessage.newBuilder().setIsSuccess(false).build();
		}
		else {
			JobScheduler.submitJob(jobId);
			message = ConfirmMessage.newBuilder().setIsSuccess(true).build();
		}
		done.run(message);
	}
	
}
