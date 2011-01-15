package com.klose.Slave;

import java.util.logging.Level;
import java.util.logging.Logger;
import com.klose.common.RunJar;
import com.klose.common.TaskDescriptor;
import com.klose.common.TaskState;
import com.klose.common.TransformerIO.FileUtil;
import com.klose.common.TransformerIO.FileUtil.FStype;

public class SlaveExecutor extends Thread{
	private TaskDescriptor taskDes;
	private volatile TaskState.STATES state = TaskState.STATES.RUNNING;
	private static final Logger LOG = Logger.getLogger(SlaveExecutor.class.getName());
	public SlaveExecutor(TaskDescriptor taskDes) {
		this.taskDes = taskDes;
	}
	public TaskState.STATES getTaskState() {
		return state;
	}
	public synchronized void setTaskState(TaskState.STATES state) {
		this.state = state;
	}
	public void run()  {
		String localJarPath = null;
		FStype type = FileUtil.getFileType(this.taskDes.getJarPath());
		if( type == FStype.HDFS) {
			String localDirPath = SlaveArgsParser.getWorkDir()+"/"+taskDes.getTaskId();
			LOG.log(Level.INFO, "localDirPath:" + localDirPath);
			if(FileUtil.mkdirLocalDir(localDirPath)) {
				localJarPath = FileUtil.TransHDFSToLocalFile
				(this.taskDes.getJarPath(), localDirPath);
			}	
		}
		else if( type == FStype.REMOTE ) {
			String localDirPath = SlaveArgsParser.getWorkDir()+"/"+taskDes.getTaskId();
			if(FileUtil.mkdirLocalDir(localDirPath)) {
				localJarPath = FileUtil.TransRemoteFileToLocal
				(this.taskDes.getJarPath(), localDirPath);
			}	
		}
		else if ( type == FStype.LOCAL){
			localJarPath = this.taskDes.getJarPath();
		}
		else {
			LOG.log(Level.WARNING, "The jar file---"+ this.taskDes.getJarPath() +" can't been recognized.");
			this.setTaskState(TaskState.STATES.WARNING);
		}
		
		if( null == localJarPath ){
			LOG.log(Level.SEVERE, "Cannot get the jar.");
			this.setTaskState(TaskState.STATES.WARNING);	
		}
		else {
			String argsAll = localJarPath + " "+ 
			taskDes.getInputPaths() + " " + taskDes.getOutputPaths();
			System.out.println("argsAll:" + argsAll);
			try {
				RunJar.run(argsAll.split(" "));
			} catch (Throwable e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				this.setTaskState(TaskState.STATES.ERROR);
			}
			System.out.println("Finished");
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
