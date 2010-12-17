package com.klose.common;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

/*
 * produces and stores reader array which indicates different reader functions.  
 * */

public class ReaderFactory {
	private VertexReader  reader;
	/*instance the Reader in according to Map map's input path*/
	public ReaderFactory(String inpath) throws IOException {
		if(inpath.startsWith("/")){
				reader = new LocalReader(inpath);
			}
			else 
				if(inpath.startsWith("hdfs"))
				{
					reader = new HDFSReader(inpath);
				}
				else{
					System.out.println("wrong input");
					System.exit(2);
				}
					
	}
	
	public  VertexReader getReader() {
		
		//return the reader as the input path sequences. 
		return reader;
	}
	
	//test
	public static void main(String[] args) throws IOException {
		ReaderFactory rf = new ReaderFactory("/etc/hosts");
		String a = null;
		while (null != (a = rf.getReader().readline())) {
			System.out.println(a);
		}
	}
	
}