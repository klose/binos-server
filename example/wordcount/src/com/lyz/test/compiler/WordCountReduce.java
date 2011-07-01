package com.lyz.test.compiler;

import com.transformer.compiler.Operation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class WordCountReduce implements Operation{
	String filePath;
	String outputPath;
	String tmppath = "/tmp/tmpreducePath";
	@Override
	public void operate(String[] arg0, String[] arg1) {
		int reduceNum = 4;
		// TODO Auto-generated method stub
//		if(arg0[0].endsWith("0")){
//			this.tmppath = this.tmppath + "0";
//		}else{
//			this.tmppath = this.tmppath + "1";
//		}	
		for(int i=0; i<reduceNum;i++)
			{
				if(arg0[0].endsWith(String.valueOf(i))){
					this.tmppath = this.tmppath + i;
				}
			}
		
		// assign input or output path
		if(arg0.length !=1|| arg1.length != 1){
			System.err.println("there are only one input path or one output path permitted in the reduce stage");
			System.exit(2);
		}
		//check the validity of the input or output path
		if(!arg0[0].startsWith("hdfs") || !arg1[0].startsWith("hdfs")){
			System.err.println("input or output path must be start with hdfs");
			System.exit(2);
		}
		
		// assign the input and output path
		this.filePath = arg0[0];
		this.outputPath = arg1[0];
		
		Configuration conf = new Configuration();
		FileSystem fs = null;
		Path inpath;
		Path tmpPath = null;
		Path path = null;
		FSDataOutputStream ops = null;
		Map map = new HashMap();
		File f = new File(tmppath);
		if(f.exists()){
			boolean bl = f.delete();
			System.out.println(String.valueOf(bl));
		}
		try {
			
			fs = FileSystem.get(conf);			
			inpath = new Path(this.filePath);
			tmpPath = new Path(tmppath);
			path = new Path(this.outputPath);
			ops = fs.create(path);
			if(!fs.exists(inpath)){
				System.out.println("the input path does not exist");
				System.exit(2);
			}
			if(fs.isDirectory(inpath)) {
				FileStatus[] fstatus = fs.listStatus(inpath);
				for(FileStatus fstmp : fstatus) {
					
					fs.copyToLocalFile(fstmp.getPath(), tmpPath);
					try {
						FileReader fr = new FileReader(tmppath);
						BufferedReader br = new BufferedReader(fr);
						try {
//							System.out.println(this.tmppath);
							String line = br.readLine();
							while(line !=null){				
									 String key = line.trim().split(" ")[0];							 								
								
									if(!map.containsKey(key)){
										map.put(key, new Integer(1));
									}
									else{
										int vall = ((Integer)map.get(key)).intValue();
										vall = vall + 1;
										Integer valuee = new Integer(vall);
										map.put(key, valuee);
									}
									line = br.readLine();
								}
								
							
							
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
					} catch (FileNotFoundException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					
					File tmpPathFile = new File(this.tmppath);
					
//					System.out.println("delete:"+ String.valueOf(tmpPathFile.delete()));
				}
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}	
		
		Set set = map.entrySet();
		Iterator iterator = set.iterator();
		while(iterator.hasNext()){
			Map.Entry<String, Integer> mapentry = (Map.Entry<String, Integer>) iterator.next();
			String s = mapentry.getKey() + " " + mapentry.getValue();
			try {
				ops.writeBytes(s+"\n");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		try {
			ops.flush();
			ops.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block reduce
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args){
		String[] input = {"hdfs://10.10.102.21:26666/user/jiangbing/1_1_0outputPath0"};
		String[] output = {"hdfs://10.10.102.21:26666/user/jiangbing/output3"};
		WordCountReduce wcr = new WordCountReduce();
		wcr.operate(input, output);
	}

}
