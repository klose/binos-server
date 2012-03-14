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
			String localDirPath = this.properties.getProperty("tmpDir");
			LOG.log(Level.INFO, "localDirPath:" + localDirPath);
			if(FileUtility.mkdirLocalDir(localDirPath)) {
				localJarPath = FileUtility.TransHDFSToLocalFile
				(this.taskDes.getJarPath(), localDirPath);
			}	
		}
		else if( type == FStype.REMOTE ) {
			String localDirPath = this.properties.getProperty("tmpDir");
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
			String localDirPath = this.properties.getProperty("tmpDir");
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
//			for(String tmp:this.properties.getAllProperties().keySet()) {
//				System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"+ tmp+ "~" + this.properties.getProperty(tmp));
//			}
			StringBuilder inputPaths = new StringBuilder();
			for (int i = 0; i < inputNum; i++) {
				ServiceType.defaultType inputType = ServiceType
						.transmitTypeToServiceType(taskDes.getInputType(i),
								true);
				if (!taskDes.getInputValid(i)) {

					String depTaskId = taskDes.getInputPath(i).split("::")[0];
					String depTaskHost = this.properties.getProperty(depTaskId)
							.split(":")[0];
					StringBuilder path = new StringBuilder();
					LOG.info("depTaskHost:" + depTaskHost);

					if (inputType == ServiceType.defaultType.HDFS) {
						path.append(this.properties.getProperty(taskDes
								.getInputPath(i)));
					} 
					else if (inputType == ServiceType.defaultType.REMOTE) {
						// handle the REMOTE path
						if (!depTaskHost.equals(this.properties.getProperty(
								"self-loc").split(":")[0])) {
							path.append("http://");
							path.append(depTaskHost);
							path.append(":");
							path.append(SlaveArgsParser.getHttpServerPort());
							path.append("/output?file=");
						} else {
							// if the dependent task locates in the same
							// machine, please use the local path.
							if (inputType == ServiceType.defaultType.REMOTE) {
								inputType = ServiceType.defaultType.LOCAL;
							}
						}
						path.append(this.properties.getProperty(taskDes
								.getInputPath(i)));
					} 
					else if (inputType == ServiceType.defaultType.MESSAGE) {
						// handle the MESSAGE path
						path.append(this.properties.getProperty(taskDes
								.getInputPath(i)));
					}
					inputPaths.append(BinosURL.transformBinosURL(
							path.toString(), inputType.toString(), "read"));
					inputPaths.append(" ");

				} else {
					if (!taskDes.getInputType(i).equals("CONFIG")) {
						inputPaths.append(BinosURL.transformBinosURL(
								taskDes.getInputPath(i), inputType.toString(),
								"read"));
						inputPaths.append(" ");
					}else {
						inputPaths.append(taskDes.getInputPath(i));
						inputPaths.append(" ");
					}
				}
			}
			StringBuilder outputPaths = new StringBuilder();
			for (int i = 0 ; i < outputNum; i++) {
				ServiceType.defaultType outputType = ServiceType.transmitTypeToServiceType(taskDes.getOutputType(i),false);
				if (!taskDes.getOutputValid(i)) {
					outputPaths.append(BinosURL.transformBinosURL(this.properties.getProperty(taskDes.getOutputPath(i)),
							outputType.toString(), "write"));
					outputPaths.append(" ");
				}
				else {
					if (!taskDes.getOutputType(i).equals("CONFIG")) {
						outputPaths.append(BinosURL.transformBinosURL(taskDes.getOutputPath(i),
								outputType.toString(), "write"));
						outputPaths.append(" ");
					}
					else {
						outputPaths.append(taskDes.getOutputPath(i));
						outputPaths.append(" ");
					}
				}
			}
			argsAll.add("-i " + inputNum);
			argsAll.add("-o " + outputNum);
			
			if(inputNum > 0) {
				argsAll.add(inputPaths.toString().trim());
			}
			if(outputNum > 0) {
				argsAll.add(outputPaths.toString().trim());
			}
			
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
