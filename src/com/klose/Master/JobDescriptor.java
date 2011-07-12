package com.klose.Master;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.dom4j.Element;

import com.klose.common.TaskState;
import com.klose.common.TransformerIO.FileUtility;
import com.transformer.compiler.JobProperties;



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
	private static final Logger LOG = Logger.getLogger(JobDescriptor.class.getName());
	
	private final JobProperties jobProperties;
	//HashMap<taskid, taskStatesIndex>
	private HashMap<String, Integer> jobStatus = new HashMap<String ,Integer>();
	
	private ArrayList<Integer> finishedTask = new ArrayList<Integer>();
	
	private JobXMLParser parser;
	
	public JobDescriptor(String jobId) {
		//String dirName = path.substring(0, path.lastIndexOf("-"));
		xmlPath = FileUtility.getHDFSAbsolutePath(jobId + "/" + jobId + ".xml");
		System.out.println("------------------------------"+ xmlPath);
		jobProperties = new JobProperties(jobId);
		parser = new JobXMLParser(this.xmlPath);
		tasksView = new TaskStates[parser.getTaskTotal()];
		loadJobView();
	}
	/** load the content of jobId.xml into tasksView and jobProperties*/
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
		taskStatesIndex = 0;
		while(taskIter.hasNext()) {
			Element taskEle = taskIter.next(); 
			String taskId = parser.getTaskID(taskEle);
			int dep = parser.getTaskDep(taskEle);	
			tasksView[taskStatesIndex] = new TaskStates(taskId);
			tasksView[taskStatesIndex].setDependence(dep);
			jobStatus.put(taskId, taskStatesIndex);
			if(dep == 0) {
				tasksView[taskStatesIndex].setStates(TaskState.STATES.PREPARED);
			}
			else {
				tasksView[taskStatesIndex].setStates(TaskState.STATES.UNPREPARED);
			}
			String prefixDep = parser.getDepTaskEle(taskEle); 
			System.out.println("prefixDep:" + prefixDep);
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
		
		/*parser the property tag into Element.*/
		Iterator<Element> propertiesIter = parser.getProperties();
		while (propertiesIter.hasNext()) {
			Element propertyEle = propertiesIter.next();
			jobProperties.addProperty(propertyEle.attributeValue("key"),
					propertyEle.attributeValue("value"));
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
				System.out.println("getPreparedTask():" +  res);
			}
		}
		if(!res.equals("")) {
			return res.trim().split(" ");
		}
		else {
			return null;
		}
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
	/**
	 * return the state of task according to the index.
	 * @param index : the index of the taskStates
	 * @return
	 */
	public synchronized TaskStates getTaskStatesByIndex(int index) {
		return this.tasksView[index];
	}
	
	public synchronized void addProperty(String key, String value) {
		jobProperties.addProperty(key, value);
	}
	public synchronized JobProperties getJobProperties() {
		return jobProperties;
	}
	/**
	 * return the state of task according to the taskid
	 * @param 
	 * @return
	 */
	public synchronized TaskStates getTaskStatesByTaskid(String taskid) {
		if(jobStatus.containsKey(taskid)) {
			return this.tasksView[jobStatus.get(taskid)];
		}
		else {
			LOG.log(Level.WARNING, "taskid:" + taskid + " doesn't exist in the job!");
			return null;
		}
	}
	public synchronized void setTaskStates(String taskId, String state) {
		int index = jobStatus.get(taskId);
		tasksView[index].setStates(TaskState.STATES.valueOf(state));
	}
	public void addFinishedTaskId(String taskid) {
		int index = searchTask(taskid);
		if(index != -1) 
			addFinishedTaskIndex(index);
		else 
			LOG.log(Level.SEVERE, "There is not " + taskid + " in the job.");
	}
	/*add the index of task into the finished task list.*/
	private void addFinishedTaskIndex(int index) {
		// when a task has completed, it will change the
		//state of related tasks.[UNPREPARED] -> [PREPARED]
		System.out.println("333333333333333" + index + "2222222222");
		if(!this.finishedTask.contains(index)) {
			// It must ensure that this is the first action of adding finished index.
			this.finishedTask.add(index);
			TaskScheduler.removeTaskIdOnSlave(tasksView[index].getTaskid());
			System.out.println("4444444444444444444" + tasksView[index].getTaskid() + "2222222222");
			System.out.println("ssssssssssssssssss " + tasksView[index].getSuffixTaskIds().toString() + "55555555555");
			System.out.println("5555555555555555555" + tasksView[index].getSuffixTaskIds().size() + "2222222222");
			
			synchronized(tasksView) {
				
				for(String relatedTask : tasksView[index].getSuffixTaskIds()) {
					System.out.println(relatedTask + "222222222222222222");
					int dep = tasksView[jobStatus.get(relatedTask)].getDependence();
					if(dep > 0) {
						dep --;
					}
					else {
						dep ++;
					}
					tasksView[jobStatus.get(relatedTask)].setDependence(dep);
					System.out.println("666666666666666666666:" + dep + ":2222222222");
					if(dep == 0) {
						LOG.log(Level.INFO, tasksView[jobStatus.get(relatedTask)].getTaskid() +" :UNPREPARED -> PREPARED");
						tasksView[jobStatus.get(relatedTask)].
							setStates(TaskState.STATES.PREPARED);
					}
				}
			}
		}
	}
	
	
	/*return the total number of the task in the job.*/
	public int getTaskTotal() {
		return this.tasksView.length;
	}
	
	/*check whether the job has finished successfully*/
	public boolean isSuccessful() {
		LOG.log(Level.INFO, "finishedTask:#" + finishedTask.size());
		if(finishedTask.size() == getTaskTotal()) {
			return true;
		}
		else {
			return false;
		}
	}
//  just for test	
//	public static void main(String [] args) {
//		JobDescriptor des = new JobDescriptor("/tmp/Job.xml");
//		System.out.println(Arrays.toString(des.getPreparedTask()));
//		System.out.println();
//	}
}
class TaskStates {
	private String taskid;
	private ArrayList <String> prefixTaskIds  = new ArrayList<String>();
	private ArrayList <String> suffixTaskIds  = new ArrayList<String>();
	private TaskState.STATES state = TaskState.STATES.UNPREPARED;
	private int dependence = 0;
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
	
	public int getDependence() {
		return dependence;
	}

	public void setDependence(int dependence) {
		this.dependence = dependence;
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
