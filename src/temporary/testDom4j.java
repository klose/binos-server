package temporary;

import java.io.File;
import java.util.Iterator;
import java.nio.channels.FileChannel; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class testDom4j {
	public static void main(String arge[]) {
		long lasting = System.currentTimeMillis();
		try {
			Configuration conf  = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			Path p = new Path("hdfs://10.5.0.55:26666/user/jiangbing/task/task-1_1_1.xml");
//			File f = new File("/tmp/task-1_1_1.xml");
			SAXReader reader = new SAXReader();
			Document doc = reader.read(fs.open(p));
			Element root = doc.getRootElement();
			Element foo ;
			
			/*retrieve the path of jar from xml*/
			String jarPath = root.elementText("JarPath");
			System.out.println("jarpath:"+jarPath);
			
			/*retrieve the input path from xml*/
			Element inputPath = root.element("InputPath");
			System.out.print(inputPath.attributeValue("num"));
			for (Iterator i = inputPath.elementIterator("path"); i.hasNext();) {
				foo = (Element) i.next();
				System.out.println(foo.getText());
			}
			
			/*retrieve the outpt path from the xml*/
			Element outputPath = root.element("OutputPath");
			System.out.println(outputPath.attributeValue("num"));
			for (Iterator i = outputPath.elementIterator("path"); i.hasNext();) {
				foo = (Element) i.next();
				System.out.println(foo.getText());
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("运行时间：" + (System.currentTimeMillis() - lasting) + " 毫秒");
	}
}
