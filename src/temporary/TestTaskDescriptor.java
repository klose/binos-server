package temporary;

import java.io.IOException;

import com.klose.Master.TaskDescriptor;

public class TestTaskDescriptor {
	public static void generateXMLFile() {
		//yuanzheng to do...
	}
	public static void main(String[] args) throws IOException {
		String path1 = "hdfs://10.5.0.55:26666/user/jiangbing/task/task-1_1_1.xml";
		String path2 = "/tmp/task-1_1_1.xml";
		TaskDescriptor descriptor = new TaskDescriptor(path2);
		long begin = System.currentTimeMillis();
		descriptor.parse();
		System.out.println(descriptor.getInputPaths());
		System.out.println(descriptor.getOutputPaths());
		System.out.println(descriptor.getJarPath());
		System.out.println(descriptor.getTaskId());
		System.out.println(System.currentTimeMillis() - begin);
	}
}
