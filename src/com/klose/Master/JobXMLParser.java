package com.klose.Master;

import java.util.Iterator;

import org.dom4j.Element;

import com.klose.common.XMLParser;

/**
 * JobXMLParser provide a set of methods to parse the job.xml
 * @author Bing Jiang
 *The format of Job.xml
 *
 <?xml version="1.0" encoding="GB2312" standalone="no"?>
<job>
	<id>1_1_201101041945</id>
	<total>6</total>
	<task id="1_1_1" dep="0"/>
	<task id="1_1_2" dep="0"/>
	<task id="1_1_3" dep="0"/>
	<task id="1_1_4" dep="-2">
		<taskid>1_1_1</taskid>
		<taskid>1_1_2</taskid>
	</task>
	<task id="1_1_5" dep="-2">
		<taskid>1_1_2</taskid>
		<taskid>1_1_3</taskid>
	</task>
	<task id="1_1_6" dep="2">
		<taskid>1_1_4</taskid>
		<taskid>1_1_5</taskid>
	</task>
</job>
 *
 */
public class JobXMLParser extends XMLParser {
	private final String ID = "id";
	private final String Task = "task";
	private final String TaskAttriId = "id";
	private final String TaskAttriDep = "dep";
	private final String TaskID = "taskid";
	private final String TaskTotal = "total";
	
	public JobXMLParser(String path) {
		super(path);
	}
	public String getID() {
		return getRootElement().element(ID).getText();
	}
	public int getTaskTotal() {
		return Integer.parseInt(getRootElement().elementText(TaskTotal));
	}
	public Iterator<Element> getTasks() {
		return getRootElement().elementIterator(Task);
	}
	public int getTaskDep(Element taskEle) {
		return Integer.parseInt(taskEle.attributeValue(TaskAttriDep));
	}
	public String getTaskID(Element taskEle) {
		return taskEle.attributeValue(TaskAttriId);
	}
	/**
	 * return the dependence task id, if it depends more than one task, 
	 * it will return the taskid:taskid:...:taskid
	 */
	public String getDepTaskEle(Element taskEle) {
		Iterator<Element> iter = taskEle.elementIterator(TaskID);
		String res = "";
		if(iter.hasNext()) {
			res = iter.next().getText();
			while(iter.hasNext()) {
				res += (":" + iter.next().getText()); 
			}
		}
		return res;
	}
}
