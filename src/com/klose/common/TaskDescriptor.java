package com.klose.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.dom4j.Element;
public class TaskDescriptor {
	private LinkedList inputPathSet = new LinkedList();
	private LinkedList outputPathSet = new LinkedList();
	private String[] inputPath;
	private String[] outputPath;
	private boolean [] inputValid; 
	private boolean [] outputValid;
	private String [] inputType;
	private String [] outputType;
	private int inputPathNum;
	private int outputPathNum;
	
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
		setInputPathNum(Integer.parseInt(xmlParser.getInputPathAttriNum()));
		setOutputPathNum(Integer.parseInt(xmlParser.getOutputPathAttriNum()));
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
	public boolean getInputValid(int id) {
		if (id >= this.inputPathNum) {
			try {
				throw new Exception ("getInputValid-Index out of Bound InputPath: length=" + this.inputPathNum + " index=" + id);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return this.inputValid[id];
	}
	public boolean getOutputValid(int id)  {
		if (id >= this.outputPathNum) {
			try {
				throw new Exception ("getOutputValid-Index out of Bound OutputPath: length=" + this.outputPathNum + " index=" + id);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return this.outputValid[id];
	}
	public String getInputType(int id) {
		if (id >= this.inputPathNum) {
			try {
				throw new Exception ("getInputType-Index out of Bound InputPath: length=" + this.inputPathNum + " index=" + id);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return this.inputType[id];
	}
	public String getOutputType(int id)  {
		if (id >= this.outputPathNum) {
			try {
				throw new Exception ("getOutputType-Index out of Bound OutputPath: length=" + this.outputPathNum + " index=" + id);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return this.outputType[id];
	}
	public String getInputPath(int id)  {
		if (id >= this.inputPathNum) {
			try {
				throw new Exception ("getInputPath-Index out of Bound InputPath: length=" + this.inputPathNum + " index=" + id);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return this.inputPath[id];
	}
	public String getOutputPath(int id)  {
		if (id >= this.outputPathNum) {
			try {
				throw new Exception ("getOutputPath-Index out of Bound OutputPath: length=" + this.outputPathNum + " index=" + id);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return this.outputPath[id];
	}
	/**
	 * retrieve the input path from xml, 
	 * and put the corresponding path into the inputPathSet.
	 */
	public void setInputPaths(Element pathEle) {
		inputPath = new String[this.inputPathNum];
		inputType = new String[this.inputPathNum];
		inputValid = new boolean[this.inputPathNum];
		Iterator iter = pathEle.elementIterator("path");
		Element tmp;
		int id;
		for(int i = 0; i < this.inputPathNum; i++) {
			tmp = (Element) iter.next();
			id = Integer.parseInt(tmp.attributeValue("id"));
			inputPath[id] = new String(tmp.getText());
			inputType[id] = new String(tmp.attributeValue("type"));
			inputValid[id] = Boolean.parseBoolean(tmp.attributeValue("valid"));
		}
	}
	/**
	 * retrieve the output path from xml, 
	 * and put the corresponding path into the outputPathSet.
	 */
	public void setOutputPaths(Element pathEle) {	
		outputPath = new String[this.outputPathNum];
		outputType = new String[this.outputPathNum];
		outputValid = new boolean[this.outputPathNum];
		Iterator iter = pathEle.elementIterator("path");
		Element tmp;
		int id;
		for(int i = 0; i < this.outputPathNum; i++) {
			tmp = (Element) iter.next();
			id = Integer.parseInt(tmp.attributeValue("id"));
			outputPath[id] = new String(tmp.getText());
			outputType[id] = new String(tmp.attributeValue("type"));
			outputValid[id] = Boolean.parseBoolean(tmp.attributeValue("valid"));
		}
	}
	/**return the inputPaths using blank as separator.
	 */
	public String getInputPaths() {
		String res = "";
		for(String tmp: this.inputPath) {
			res += tmp;
			res += " ";
		}
		return res.trim();
	}
	/**return the outputPaths using blank as separator.
	 */
	public String getOutputPaths() {
		String res = "";
		for(String tmp: this.outputPath) {
			res += tmp;
			res += " ";
		}
		return res.trim();
	}
	
	/**
	 * set the number of input path
	 * @param inputPathNum
	 */
	public void setInputPathNum(int inputPathNum) {
		this.inputPathNum = inputPathNum;
	}
	/**
	 * set the number of output path
	 * @param outputPathNum
	 */
	public void setOutputPathNum(int outputPathNum) {
		this.outputPathNum = outputPathNum;
	}
	
	/**
	 * retrieve the number of the input path
	 * @return
	 */
	public int getInputPathNum() {
		return  this.inputPathNum;
	}
	
	/**
	 * retrieve the number of the output path
	 * @return 
	 */
	public int getOutputPathNum() {
		return this.outputPathNum;
	}
}
