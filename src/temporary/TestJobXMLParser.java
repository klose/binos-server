package temporary;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;

import org.dom4j.Element;

import com.klose.Master.JobXMLParser;

public class TestJobXMLParser {
	public static void main(String[] args) throws IOException {
		String content = "" +
		"<?xml version=\"1.0\" encoding=\"GB2312\" standalone=\"no\"?>\n"
				+ "<Job>\n" + "<Id>1_1_201101041945</Id>\n"
				+ "<total>6</total>\n"
				+ "<task id=\"1_1_1\" dep=\"0\"/>\n"
				+ "<task id=\"1_1_2\" dep=\"0\"/>\n"
				+ "<task id=\"1_1_3\" dep=\"0\"/>\n"
				+ "<task id=\"1_1_4\" dep=\"-2\">\n"
				+ "<taskid>1_1_1</taskid>\n" + "<taskid>1_1_2</taskid>\n"
				+ "</task>\n" + "<task id=\"1_1_5\" dep=\"-2\">\n"
				+ "   <taskid>1_1_2</taskid>\n" + "   <taskid>1_1_3</taskid>\n"
				+ "</task>\n" + "<task id=\"1_1_6\" dep=\"2\">\n"
				+ "<taskid>1_1_4</taskid>\n" + "<taskid>1_1_5</taskid>\n"
				+ "</task>\n" + "</Job>\n";
		File file = new File("/tmp/job.xml");
		if (!file.exists()) {
			file.createNewFile();
		}
		FileWriter writer = new FileWriter(file);
		writer.write(content);
		writer.close();
		JobXMLParser parser = new JobXMLParser("/tmp/job.xml");
		System.out.println(parser.getID());
		System.out.println(parser.getTaskTotal());
		Iterator<Element> iter = parser.getTasks();
		while(iter.hasNext()) {
			System.out.println("***************");
			Element tmp = iter.next();
			System.out.println(tmp.asXML());
			System.out.println(parser.getTaskID(tmp));
			System.out.println(parser.getTaskDep(tmp));
			System.out.println(parser.getDepTaskEle(tmp));
		}
	}
}
