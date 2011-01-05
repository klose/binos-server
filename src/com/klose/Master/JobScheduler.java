package com.klose.Master;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * JobScheduler is a core class  used to schedule job from waiting queue to running queue.
 *  
 * @author Bing Jiang
 *
 */
public class JobScheduler {
	//* waitingQueue is used to store the jobs waiting to be added to running queue.
	private ArrayList<String> waitingQueue = new ArrayList<String>();
	private HashMap<String, JobDescriptor> runningQueue = new HashMap<String, JobDescriptor>();
	
}
