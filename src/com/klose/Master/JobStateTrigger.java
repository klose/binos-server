package com.klose.Master;
/**
 * JobStateTrigger is a class used to detect the tasks which is dependent upon 
 * other Running tasks. If the dependent tasks has all finished in a time, it 
 * will change the state of the task from UNPREPARED to PREPARED, and submitted 
 * to the module of TaskScheduler.     
 * @author Bing Jiang
 *
 */
public class JobStateTrigger {
	private JobStateTrigger() {
		
	}
	public void triggerNode(String taskIdPos, String state) {
		TaskStates tss = JobScheduler.getTaskStates(taskIdPos);
		if(tss.getState().toString().equals(state)) {
			if(state.equals("Finished")) {
				
			}
		}
	}
	public static JobStateTrigger valueOf() {
		
	}
	
}
