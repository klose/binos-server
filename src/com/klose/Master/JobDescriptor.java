package com.klose.Master;


import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import org.dom4j.Element;



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
	
	private ArrayList<Integer> finishedTask = new ArrayList<Integer>();
	
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
				tasksView[taskStatesIndex].setStates(TaskState.STATES.PREPARED);
			}
			else {
				tasksView[taskStatesIndex].setStates(TaskState.STATES.UNPREPARED);
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
	/**retrieve the tasks whose condition is prepared, 
	 * and hasn't submitted to TaskScheduler. 
	 */
	public String[] getPreparedTask() {
		int length = tasksView.length;
		String res = "";
		for(int i = 0; i < length ; i ++) {
			if(tasksView[i].getState() == TaskState.STATES.PREPARED) {
				res += (tasksView[i].getTaskid() + " ");
			}
		}
		return res.trim().split(" ");
	}
	
	/**
	 * find the task in the job.
	 * @param taskId
	 * @return the index of the task in array of tasksView
	 * if it can't find it , it will return -1
	 */
	public int searchTask(String taskId) {
		Integer res = jobStatus.get(taskId);
		if(res != null) {
			return res;
		}
		else {
			return -1;
		}		
	}
	public synchronized TaskStates getTaskStates(int index) {
		return this.tasksView[index];
	}
	
	/*add the index of task into the finished task list.*/
	public void addFinishedTaskIndex(int index) {
		this.finishedTask.add(index);
	}
	
	/*return the total number of the task in the job.*/
	public int getTaskTotal() {
		return this.tasksView.length;
	}
	
	/*check whether the job has finished successfully*/
	public boolean isSuccessful() {
		if(finishedTask.size() == getTaskTotal()) {
			return true;
		}
		else {
			return false;
		}
	}
	
	public static void main(String [] args) {
		JobDescriptor des = new JobDescriptor("/tmp/Job.xml");
		System.out.println(Arrays.toString(des.getPreparedTask()));
		System.out.println();
	}
}
class TaskStates {
	private String taskid;
	private ArrayList <String> prefixTaskIds  = new ArrayList<String>();
	private ArrayList <String> suffixTaskIds  = new ArrayList<String>();
	private TaskState.STATES state = TaskState.STATES.UNPREPARED;
	public TaskStates(String taskId) {
		taskid = taskId;
	}
	
	public String getTaskid() {
		return taskid;
	}

	public void setTaskid(String taskid) {
		this.taskid = taskid;
	}

	public void setStates(TaskState.STATES state) {
		this.state = state;
	}
	public TaskState.STATES getState() {
		return this.state;
	}
	
	public ArrayList<String> getPrefixTaskIds() {
		return prefixTaskIds;
	}

	public void setPrefixTaskIds(ArrayList<String> prefixTaskIds) {
		this.prefixTaskIds = prefixTaskIds;
	}

	public ArrayList<String> getSuffixTaskIds() {
		return suffixTaskIds;
	}

	public void setSuffixTaskIds(ArrayList<String> suffixTaskIds) {
		this.suffixTaskIds = suffixTaskIds;
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
