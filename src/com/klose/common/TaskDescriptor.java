package com.klose.common;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.dom4j.Element;


public class TaskDescriptor {
	private HashSet<String> inputPathSet = new HashSet<String>();
	private HashSet<String> outputPathSet = new HashSet<String>();
	private String className; // specify the class that contains user's operation.
	private String jarPath ;
	private String taskId;
	private String xmlPath;
	public TaskDescriptor(String path) throws IOException  {
		this.xmlPath = path;	
		parse();
	}
	public void parse() throws IOException {
		TaskXMLParser xmlParser = new TaskXMLParser(this.xmlPath);
		setTaskId(xmlParser.getTaskId());
		setJarPath(xmlParser.getJarPath());
		setInputPaths(xmlParser.getInputPaths());
		setOutputPaths(xmlParser.getOutputPaths());
		setClassName(xmlParser.getClassName());
	}
	public void setClassName(String className) {
		this.className = className;
	}

	public String getClassName() {
		return this.className;
	}
	public String getJarPath() {
		return jarPath;
	}
	public void setJarPath(String jarPath) {
		this.jarPath = jarPath;
	}
	public String getTaskId() {
		return taskId;
	}
	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}
	/**
	 * retrieve the input path from xml, 
	 * and put the corresponding path into the inputPathSet.
	 */
	public void setInputPaths(Element pathEle) {
		Element tmp;
		for (Iterator i = pathEle.elementIterator("path"); i.hasNext();) {
			tmp = (Element) i.next();
			this.inputPathSet.add(tmp.getText());
		}
	}
	/**
	 * retrieve the output path from xml, 
	 * and put the corresponding path into the outputPathSet.
	 */
	public void setOutputPaths(Element pathEle) {
		Element tmp;
		for (Iterator i = pathEle.elementIterator("path"); i.hasNext();) {
			tmp = (Element) i.next();
			this.outputPathSet.add(tmp.getText());
		}
	}
	/**return the inputPaths using blank as separator.
	 */
	public String getInputPaths() {
		String res = "";
		for(Iterator it = this.inputPathSet.iterator(); it.hasNext(); ) {
			res += (String)it.next();
			res += " ";
		}
		return res.trim();
	}
	/**return the outputPaths using blank as separator.
	 */
	public String getOutputPaths() {
		String res = "";
		for(Iterator it = this.outputPathSet.iterator(); it.hasNext(); ) {
			res += (String)it.next();
			res += " ";
		}
		return res.trim();
	}
	/**
	 * retrieve the number of the input path
	 * @return
	 */
	public int getInputPathNum() {
		return  this.inputPathSet.size();
	}
	
	/**
	 * retrieve the number of the output path
	 * @return 
	 */
	public int getOutputPathNum() {
		return this.outputPathSet.size();
	}
}
