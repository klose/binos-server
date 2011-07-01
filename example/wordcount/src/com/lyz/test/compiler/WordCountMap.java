package com.lyz.test.compiler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.transformer.compiler.*;
public class WordCountMap implements Operation{
	String fileName;
	private final int reduceNum = 4;
	private final int sbCapacity = 1024 * 1024;
	String outputPath[] = new String[this.reduceNum];
	String tmpPath[]  = new String[this.reduceNum];

	@Override
	public void operate(String[] arg0, String[] arg1) {
		// TODO Auto-generated method stub
		/**
		 * in transformer0.2, all path must be hdfs'
		 * */
		if(arg0.length != 1 || !arg0[0].startsWith("hdfs")){
			
			System.err.println("Wrong input path");
			return;
		}
		if(arg1.length != this.reduceNum){
			System.err.println("wrong output path number");
			return; 
		}
		
		/**
		 * assign the task's output path.
		 * */
		for(int i = 0;i<this.reduceNum;i++){
			this.outputPath[i] = arg1[i];
		}
		
		/**
		 * assign the task's input path.
		 * */
		this.fileName = arg0[0];
		
		/**
		 * assign the temp path
		 * */
		for(int i=0;i<this.reduceNum;i++){
			this.tmpPath[i] = "/tmp/wordCountMaptmppath"+arg0.toString()+i;
		}
		/**
		 * configurate the hdfs.
		 * */
		Configuration conf = new Configuration();
		Path pathq;
		Path tmppath[] = new Path[this.reduceNum];
		Path outpath[] = new Path[this.reduceNum];
		FileSystem fs = null;
		FSDataInputStream fsr = null;	
		
		try {
			fs = FileSystem.get(conf);
			pathq = new Path(fileName);
			if(!fs.exists(pathq)){
				System.err.println("the path "+fileName+" does not exists!");
				System.exit(2);
			}		
			fsr = fs.open(pathq);
			for(int i = 0; i < this.reduceNum; i++) {
				tmppath[i] = new Path(this.tmpPath[i]);
				outpath[i] = new Path(this.outputPath[i]);
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String s;
		String line;
//		BufferedReader reader = new BufferedReader(new InputStreamReader(fsr));
		StringBuffer sb[] = new StringBuffer[this.reduceNum];
		FileOutputStream fwriter[] = new FileOutputStream[this.reduceNum];
		PrintStream ps[] = new PrintStream[this.reduceNum];
		try {
			for (int i = 0; i < sb.length; i++) {
				sb[i] = new StringBuffer(this.sbCapacity);
				fwriter[i] = new FileOutputStream(tmpPath[i]);
				ps[i] = new PrintStream(fwriter[i]);
			}	
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			s = fsr.readLine();
			while(s != null){
				s = s.trim();
				String[] split = s.split(" ");
				for(int i = 0;i< split.length;i++){
					
					int hashIndex = split[i].hashCode()% this.reduceNum;
					if(hashIndex < 0 ){
						hashIndex = -hashIndex;
					}
//					System.out.println(""+hashIndex);
					sb[hashIndex].append(split[i] + " " + String.valueOf(1) + "\n");
					if (sb[hashIndex].length() >= 1024 * 1024) {
						ps[hashIndex].print(sb[hashIndex].toString());
						ps[hashIndex].flush();
						sb[hashIndex].delete(0, sb[hashIndex].length());
					}	
				}
				s = fsr.readLine();	
			}
			
			for (int i = 0; i < this.reduceNum; i++) {
				if(sb[i].length() > 0) {
					ps[i].print(sb[i].toString());
					ps[i].flush();
					sb[i].delete(0, sb[i].length());
				}
				ps[i].close();
				fwriter[i].close();
			}
			fsr.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		
		
		try {
			for (int i = 0; i < this.reduceNum; i++) {
				fs.copyFromLocalFile(tmppath[i], outpath[i]);
				
				File f = new File(tmpPath[i]);
				if(f.exists()){
					f.delete();
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	//for the test
	public static void main(String[] args){
		
		WordCountMap wcm = new WordCountMap();
		String[] input = {"hdfs://10.5.0.175:20001/user/lyz/input"};
		String[] output = {"hdfs://10.5.0.175:20001/user/lyz/output1","hdfs://10.5.0.175:20001/user/lyz/output2"};
		wcm.operate(input, output);
		
	}

}
