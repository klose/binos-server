package com.klose.common.TransformerIO;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.FileInputStream;

public class ReadXml {
		public void readxml(String path){
			int pathType;
			if(path.startsWith("/")){
					pathType = 1;
					
					
				}
				else 
					if(path.startsWith("hdfs"))
					{
						pathType = 2;
						try {
//							HDFSReader hrd = new HDFSReader(path);
							FileSystem fs;
							FSDataInputStream fsr;
							Configuration conf = new Configuration();

							Path pathq = new Path(path);
							fs = FileSystem.get(conf);
							fsr = fs.open(pathq);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					else
					{
						System.out.println("the wrong input");
					}
			
		}
}
