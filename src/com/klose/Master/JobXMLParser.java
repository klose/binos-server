package com.klose.Master;

import org.dom4j.Element;

import com.klose.common.XMLParser;

/**
 * JobXMLParser provide a set of methods to parse the job.xml
 * @author Bing Jiang
 *The format of Job.xml
 *
 <?xml version="1.0" encoding="GB2312" standalone="no"?>
<Job>
    <id>1_1_201101041945</id>
    <total>6</total>
    <task id=task_1_1_1 dep=0/>
    <task id=task_1_1_2 dep=0/>
    <task id=task_1_1_3 dep=0/>
    <task id=task_1_1_4 dep=-2>
        <taskid>1_1_1</taskid>
        <taskid>1_1_2</taskid>
    </task>
    <task id=1_1_5 dep=-2>
        <taskid>1_1_2</taskid>
        <taskid>1_1_3</taskid>
    </task>
    <task id=1_1_6 dep=2>
        <taskid>1_1_4</taskid>
        <taskid>1_1_5</taskid>
    </task>
</Job>
 *
 */
public class JobXMLParser extends XMLParser {
	private final String ID = "id";
	private final String Task = "task";
	private final String TaskDep = "dep";
	private final String TaskID = "taskid";
	private final String TaskTotal = "total";
	
	public JobXMLParser(String path) {
		super(path);
	}
	public Element getID() {
		return getRootElement().element(ID);
	}
	public int getTaskTotal() {
		return Integer.parseInt(getRootElement().elementText(TaskTotal));
	}
	public Element getTasks() {
		return getRootElement().element(Task);
	}
	public int getTaskDep(Element taskEle) {
		return Integer.parseInt(taskEle.attributeValue(TaskDep));
	}
	public String getTaskID(Element taskEle) {
		return taskEle.attributeValue(ID);
	}
	public Element getDepTaskEle(Element taskEle) {
		return taskEle.element(TaskID);
	}
}
