package com.klose.common;

import java.io.IOException;
import java.util.HashSet;

import org.dom4j.Element;


public class TaskXMLParser extends XMLParser{
	private  final String inputpathDes = "inputPath";
	private  final String outputpathDes = "outputPath";
	private  final String inputpathAttriNum = "inputPathNum";
	private  final String outputpathAttriNum = "outputPathNum";
	private  final String pathDes = "path";
	private  final String taskidDes = "taskId";
	private  final String jarPathDes = "jarPath";
	private final String className = "operationClass";

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
	public String getClassName() {
		return getRootElement().elementText(this.className);
	}
	public String getTaskId() {
		return getRootElement().elementText(this.taskidDes);
	}
}
