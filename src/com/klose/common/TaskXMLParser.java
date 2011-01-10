package com.klose.common;

import java.io.IOException;
import java.util.HashSet;

import org.dom4j.Element;


public class TaskXMLParser extends XMLParser{
	private  final String inputpathDes = "InputPath";
	private  final String outputpathDes = "OutputPath";
	private  final String inputpathAttriNum = "num";
	private  final String outputpathAttriNum = "num";
	private  final String pathDes = "path";
	private  final String taskidDes = "TaskId";
	private  final String jarPathDes = "JarPath";

	public TaskXMLParser(String path) throws IOException {
		super(path);
		
	}
	public Element getInputPaths() {
		//return getDocument().getRootElement().element(this.inputpathDes);
		return getRootElement().element(this.inputpathDes);
	}
	public Element getOutputPaths() {
		//return getDocument().getRootElement().element(this.outputpathDes);
		return getRootElement().element(this.outputpathDes);
	}
	public String getJarPath() {
		return getRootElement().elementText(this.jarPathDes);
	}
	public String getInputPathAttriNum() {
		return getRootElement().element(this.inputpathDes)
				.attributeValue(this.inputpathAttriNum);
	}
	public String getOutputPathAttriNum() {
		return getRootElement().element(this.outputpathDes)
			.attributeValue(this.outputpathAttriNum);
	}
	public String getTaskId() {
		return getRootElement().elementText(this.taskidDes);
	}
}
