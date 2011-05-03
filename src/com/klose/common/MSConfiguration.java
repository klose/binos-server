package com.klose.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class MSConfiguration {
	private static HashMap<String, String> configuration 
		= new HashMap<String, String>();
	private static int maxTasksOnEachSlave = 3;
	static {
		File MSproperties = new File("MS.properties");
		try {
			BufferedReader br = new BufferedReader(new FileReader(MSproperties));
			String tmp = "";
			while ((tmp = br.readLine()) != null) {
				if ((!tmp.trim().equals("")) && (tmp.indexOf("=") != -1)) {
					String a[] = tmp.trim().split("=");
					configuration.put(a[0], a[1]);
				}
			}
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static int getMaxTasksOnEachSlave() {
		return Integer.parseInt(configuration.get("maxTasksOnEachSlave"));
	}
	//just for testing
	public static void main(String[] args) {
		System.out.println(MSConfiguration.getMaxTasksOnEachSlave());
	}
}
