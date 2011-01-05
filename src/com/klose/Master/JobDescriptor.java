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
	private HashMap<String, Integer> jobStatus = new HashMap<String ,Integer>();
	
	private JobXMLParser parser;
	
	public JobDescriptor(String path) {
		xmlPath = path;
		parser = new JobXMLParser(this.xmlPath);
		tasksView = new TaskStates[parser.getTaskTotal()];
		loadJobView();
	}
	private void loadJobView() {
		Iterator<Element> taskIter = parser.getTasks();
		
		/*In order to generate the suffix dependence between tasks, 
		store the prefix dependence.For example, task3 depends both 
		task1 and task2 finished. DAG: 1,2 ->3, store 1:2:3, when 
		the first structure of DAG has been constructed, it only records
		3 depends both 1 and 2, it misses the suffix
		dependence, and later will use the tasksDep accordingly to generate
		it.In the example above,task 1 and 2 will point to task3.  
		*/
		String [] tasksDep = new String[tasksView.length];
		
		while(taskIter.hasNext()) {
			Element taskEle = taskIter.next(); 
			String taskId = parser.getTaskID(taskEle);
			int dep = parser.getTaskDep(taskEle);
			tasksView[taskStatesIndex] = new TaskStates(taskId);
			jobStatus.put(taskId, taskStatesIndex);
			if(dep == 0) {
				tasksView[taskStatesIndex].setStates(TaskState.PREPARED);
			}
			else {
				tasksView[taskStatesIndex].setStates(TaskState.UNPREPARED);
			}
			String prefixDep = parser.getDepTaskEle(taskEle); 
			if(!prefixDep.equals(""))
				tasksDep[taskStatesIndex] = new String(prefixDep+":"+taskId);
			else
				tasksDep[taskStatesIndex] = new String("");
			tasksView[taskStatesIndex]
			          .addPrefixTaskIds(prefixDep);
			taskStatesIndex ++;
		}
		for(int i = 0; i < taskStatesIndex; i++) {
			if(!tasksDep[i].equals("")) {
				String [] id = tasksDep[i].split(":");
				String endId = id[id.length -1];
				for(int j = 0; j < id.length -1; j++) {
					tasksView[jobStatus.get(id[j])].addSuffixTaskIds(endId); 
				}
			}
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
		public void addPrefixTaskIds(String taskIds) {
			if(!taskIds.trim().equals("")) {
				String [] id = taskIds.split(":");
				for(String tmp: id) {
					prefixTaskIds.add(tmp);
				}
			}
		}
		public void addSuffixTaskIds(String taskIds) {
			if(!taskIds.trim().equals("")) {
				String [] id = taskIds.split(":");
				for(String tmp: id) {
					suffixTaskIds.add(tmp);
				}
			}
		}
	}
}
