package com.klose.Slave;

import java.util.logging.Level;
import java.util.logging.Logger;
import com.klose.Master.TaskDescriptor;
import com.klose.common.RunJar;
import com.klose.common.TransformerIO.FileUtil;
import com.klose.common.TransformerIO.FileUtil.FStype;

public class SlaveExecutor {
	private TaskDescriptor taskDes;
	private static final Logger LOG = Logger.getLogger(SlaveExecutor.class.getName());
	public SlaveExecutor(TaskDescriptor taskDes) {
		this.taskDes = taskDes;
	}
	public void startExecute() throws Throwable {
		
		String localJarPath = null;
		FStype type = FileUtil.getFileType(this.taskDes.getJarPath());
		if( type == FStype.HDFS) {
			localJarPath = FileUtil.TransHDFSToLocalFile
			(this.taskDes.getJarPath(), SlaveArgsParser.getWorkDir()+"/"+taskDes.getTaskId());
		}
		else if ( type == FStype.LOCAL){
			localJarPath = this.taskDes.getJarPath();
		}
		else {
			LOG.log(Level.WARNING, "The jar file---"+ this.taskDes.getJarPath() +" can't been recognized.");
		}
		
		if( null == localJarPath ){
				LOG.log(Level.SEVERE, "Cannot get the jar.");
		}
		else {
			String argsAll = localJarPath + " "+ 
			taskDes.getInputPaths() + " " +taskDes.getOutputPaths();
			System.out.println("argsAll:" + argsAll);
			RunJar.run(argsAll.split(" "));
		}
	}
	public static void main(String [] args) throws Throwable {
		String path1 = "hdfs://10.5.0.55:26666/user/jiangbing/task/task-1_1_1.xml";
		String path2 = "/tmp/task-1_1_1/task-1_1_1.xml";
		TaskDescriptor descriptor = new TaskDescriptor(path2);
		descriptor.parse();
		SlaveExecutor se = new SlaveExecutor(descriptor);
		se.startExecute();
	}
}
