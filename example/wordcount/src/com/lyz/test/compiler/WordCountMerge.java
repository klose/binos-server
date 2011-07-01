package com.lyz.test.compiler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.transformer.compiler.Operation;

public class WordCountMerge implements Operation{
	private String[] inputPath;
	private String outputPath;
	@Override
	public void operate(String[] arg0, String[] arg1) {
		// TODO Auto-generated method stub
		
		//assign input and output paths.
		if(arg0.length != 0)
		{
			this.inputPath = new String[arg0.length];
			for(int i=0;i<arg0.length;i++){
				if(!arg0[i].startsWith("hdfs")){
					System.err.println("Wrong input path in merge stage");
					return;
				}
				this.inputPath[i] = arg0[i];
			}
		}
		else{
			System.err.println("there is no input path");
			return;
		}
		if(arg1.length == 1){
			this.outputPath = arg1[0];
		}
		else{
			System.err.println("there is only one output path permitted in the merge stage");
			return;
		}
		
		Configuration conf = new Configuration();
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		Path dstPath = new Path(outputPath);
		try {
			OutputStream out = fs.create(dstPath);
			if (!fs.exists(dstPath)) {
				System.err.println("the outputpath " + outputPath
						+ " does not exist!");
				System.exit(1);
			}	
			for(int i = 0; i < inputPath.length;i++){			
				Path path = new Path(this.inputPath[i]);
				if(!fs.exists(path)){
					System.err.println("the inputpath "+inputPath[i]+" does not exist!");
					System.exit(1);
				}
				InputStream in = fs.open(path);
				IOUtils.copyBytes(in, out, conf,false);
				in.close();
			}
			out.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	public static void main(String[] args){
		String[] inputPath = {"hdfs://10.5.0.175:20001/user/lyz/output3","hdfs://10.5.0.175:20001/user/lyz/output"};
		String[] output = {"hdfs://10.5.0.175:20001/user/lyz/finaloutput"};
		WordCountMerge wcm = new WordCountMerge();
		wcm.operate(inputPath, output);
	}
}
