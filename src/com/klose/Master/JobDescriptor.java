package com.klose.Master;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.dom4j.Element;
import org.dom4j.Node;

/**
 * JobDescriptor is used to describe the dependence and states of tasks in a job.
 * The Class is a graph structure.
 * @author Bing Jiang
 *
 */
public class JobDescriptor {
	private String xmlPath;
	
	/*tasksView is constructed by different tasks and their links.*/ 
	private TaskStates [] tasksView; 
	private static int taskStatesIndex = 0;// the index of array JobView
	
	//HashMap<taskid, taskStatesIndex>
	private HashMap<String, Integer> JobStatus = new HashMap<String ,Integer>();
	
	private JobXMLParser parser;
	
	public JobDescriptor(String path) {
		xmlPath = path;
		parser = new JobXMLParser(this.xmlPath);
		tasksView = new TaskStates[parser.getTaskTotal()];
		loadJobView();
	}
	private void loadJobView() {
		Iterator<Element> taskIter = parser.getTasks().elementIterator();
		while(taskIter.hasNext()) {
			Element taskEle = taskIter.next(); 
			String taskId = parser.getTaskID(taskEle);
			int dep = parser.getTaskDep(taskEle);
			tasksView[taskStatesIndex] = new TaskStates(taskId);
			JobStatus.put(taskId, taskStatesIndex);
			if(dep == 0) {
				tasksView[taskStatesIndex].setStates(TaskState.PREPARED);
			}
			else {
				tasksView[taskStatesIndex].setStates(TaskState.UNPREPARED);
				Iterator<Element> eleIter = parser.getDepTaskEle(taskEle).elementIterator();
				while(eleIter.hasNext()) {
					eleIter.next().
				}
				
			}
			taskStatesIndex ++;
			
			
		}
	}
	public void parse() {
		JobXMLParser parser = new JobXMLParser(this.xmlPath);
		
	}
	public class TaskStates {
		private String taskid;
		private ArrayList <String> prefixTaskIds  = new ArrayList<String>();
		private ArrayList <String> suffixTaskIds  = new ArrayList<String>();
		private int states = TaskState.UNPREPARED;
		public TaskStates(String taskId) {
			taskid = taskId;
		}
		public void setStates(int state) {
			this.states = state;
		}
	}
}
