package com.klose.Slave;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

import cn.ict.binos.transmit.BinosURL;
import cn.ict.binos.transmit.ServiceType;

import com.klose.common.RunJar;
import com.klose.common.TaskDescriptor;
import com.klose.common.TaskState;
import com.klose.common.TransformerIO.FileUtility;
import com.klose.common.TransformerIO.FileUtility.FStype;
import com.transformer.compiler.JobProperties;

public class SlaveExecutor extends Thread{
	private final TaskDescriptor taskDes;
	private final JobProperties properties;
	private volatile TaskState.STATES state = TaskState.STATES.RUNNING;
	private static final Logger LOG = Logger.getLogger(SlaveExecutor.class.getName());
	public SlaveExecutor(TaskDescriptor taskDes, JobProperties pros) {
		this.taskDes = taskDes;
		this.properties = pros;
	}
	public TaskState.STATES getTaskState() {
		return state;
	}
	public synchronized void setTaskState(TaskState.STATES state) {
		this.state = state;
	}
	public void run() {
		String localJarPath = null;
		FStype type = FileUtility.getFileType(this.taskDes.getJarPath());
		String taskId = taskDes.getTaskId();
		if( type == FStype.HDFS) {
			String localDirPath = SlaveArgsParser.getWorkDir()+"/"+taskId;
			LOG.log(Level.INFO, "localDirPath:" + localDirPath);
			if(FileUtility.mkdirLocalDir(localDirPath)) {
				localJarPath = FileUtility.TransHDFSToLocalFile
				(this.taskDes.getJarPath(), localDirPath);
			}	
		}
		else if( type == FStype.REMOTE ) {
			String localDirPath = SlaveArgsParser.getWorkDir()+"/"+taskId;
			if(FileUtility.mkdirLocalDir(localDirPath)) {
				localJarPath = FileUtility.TransRemoteFileToLocal
				(this.taskDes.getJarPath(), localDirPath);
			}	
		}
		else if ( type == FStype.LOCAL){
			localJarPath = this.taskDes.getJarPath();
		}
		else {
			//default condition : using HDFS to resolve the file
			String localDirPath = SlaveArgsParser.getWorkDir()+"/"+taskId;
			LOG.log(Level.INFO, "localDirPath:" + localDirPath);
			if(FileUtility.mkdirLocalDir(localDirPath)) {
				localJarPath = FileUtility.TransHDFSToLocalFile
				(this.taskDes.getJarPath(), localDirPath);
			}
			
//			LOG.log(Level.WARNING, "The jar file---"+ this.taskDes.getJarPath() +" can't been recognized.");
//			this.setTaskState(TaskState.STATES.WARNING);
//			return;
		}
		
		if( null == localJarPath ){
			LOG.log(Level.SEVERE, "Cannot get the jar.");
			this.setTaskState(TaskState.STATES.WARNING);
			return;
		}
		else {
			LinkedList<String> argsAll  = new LinkedList<String>();
			argsAll.add(localJarPath);
			argsAll.add(taskDes.getClassName());
			int inputNum = taskDes.getInputPathNum();
			int outputNum = taskDes.getOutputPathNum();
			for(String tmp:this.properties.getAllProperties().keySet()) {
				System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"+ tmp+ "~" + this.properties.getProperty(tmp));
			}
			String inputPaths = "";
			for (int i = 0 ; i < inputNum; i++) {
				if (!taskDes.getInputValid(i)) {
					inputPaths += BinosURL.transformBinosURL(this.properties.getProperty(taskDes.getInputPath(i)),
							ServiceType.transmitTypeToServiceType(taskDes.getInputType(i), true).toString(),  "read") + " ";
				}
				else {
					if (!taskDes.getInputType(i).equals("CONFIG")) { 
						
						inputPaths += BinosURL.transformBinosURL(taskDes.getInputPath(i),
								ServiceType.transmitTypeToServiceType(taskDes.getInputType(i), true).toString(), "read") + " ";
					}
					else {
						inputPaths += taskDes.getInputPath(i)+ " ";
					}
				}
			}
			String outputPaths = "";
			for (int i = 0 ; i < outputNum; i++) {
				if (!taskDes.getOutputValid(i)) {
					outputPaths += BinosURL.transformBinosURL(this.properties.getProperty(taskDes.getOutputPath(i)),
							ServiceType.transmitTypeToServiceType(taskDes.getOutputType(i),false).toString(), "write") + " ";
				}
				else {
					if (!taskDes.getOutputType(i).equals("CONFIG")) {
						outputPaths += BinosURL.transformBinosURL(taskDes.getOutputPath(i),
								ServiceType.transmitTypeToServiceType(taskDes.getOutputType(i),false).toString(), "write") + " ";
					}
					else {
						outputPaths += taskDes.getOutputPath(i) + " ";
					}
				}
			}
			argsAll.add("-i " + inputNum);
			argsAll.add("-o " + outputNum);
			
			if(inputNum > 0) {
				argsAll.add(inputPaths.trim());
			}
			if(outputNum > 0) {
				argsAll.add(outputPaths.trim());
			}
			
			//String argsAll = localJarPath +   " " +
//			taskDes.getInputPaths() + " " + taskDes.getOutputPaths();
			System.out.println("argsAll:" + argsAll);
		
			try {
				RunJar.executeOperationJar(this.properties,argsAll.toArray(new String[0]));
			} catch (Throwable e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				this.setTaskState(TaskState.STATES.ERROR);
				return;
			}
			this.setTaskState(TaskState.STATES.FINISHED);
		}
	}
//	public static void main(String [] args) throws Throwable {
//		String path1 = "hdfs://10.5.0.55:26666/user/jiangbing/task/task-1_1_1.xml";
//		String path2 = "/tmp/task-1_1_1/task-1_1_1.xml";
//		if(FileUtil.checkFileValid(path2)) {
//			TaskDescriptor descriptor = new TaskDescriptor(path2);
//			//descriptor.parse();
//			SlaveExecutor se = new SlaveExecutor(descriptor);
//			se.start();
//		}
//		else {
//			LOG.log(Level.SEVERE, "Can't find the XML path:"+ path2);
//		}
//		
//	}
}
