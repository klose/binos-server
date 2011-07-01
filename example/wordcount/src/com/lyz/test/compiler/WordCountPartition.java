package com.lyz.test.compiler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import com.transformer.compiler.Operation;

public class WordCountPartition implements Operation{
	private int mapNum = 80;
	private  final int reduceNum = 4;
	private String[] inputPath = new String[this.mapNum];
	private String[] outputPath = new String[this.reduceNum];
	
	@Override
	public void operate(String[] arg0, String[] arg1) {
		// TODO Auto-generated method stub
		/**
		 * check the input or output path numbers.
		 * */
		if(arg0.length !=this.mapNum || arg1.length != this.reduceNum){
			System.err.println("wrong input or output path number");
			return;
		}
		
		
		/**
		 * check the validity of input or output paths.
		 * the path must be start with hdfs.
		 * */
		
		for(int i=0;i<this.mapNum;i++){
			if(arg0[i].startsWith("hdfs")){
				this.inputPath[i] = arg0[i];
			}
			else{
				System.err.println("input path must be start with hdfs");
				return;
			}
		}
		for(int i=0;i<this.reduceNum;i++){
			if(arg1[i].startsWith("hdfs")){
				this.outputPath[i] = arg1[i];
			}
			else{
				System.err.println("output path must be start with hdfs");
				return;
			}
		}
				
		
		Configuration conf = new Configuration();
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

		Path[][] paths = new Path[this.reduceNum][this.inputPath.length/this.reduceNum];
		
		
		
		for(int i = 0; i < inputPath.length;i++){						
			Path path = new Path(this.inputPath[i]);
				try {
					if(!fs.exists(path)){
						System.err.println("the inputpath "+inputPath[i]+" does not exist!");
						return ;
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
		}
		

		
		int[] p =  new int[this.reduceNum];
		for(int i=0;i< this.reduceNum;i++){
			p[i] = 0;
		}
		System.out.println("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
		for(int j=0;j<this.inputPath.length;j++)
			for(int k=0;k<this.reduceNum;k++)
			{		
					if(this.inputPath[j].endsWith(String.valueOf(k))&&p[k] < this.inputPath.length/this.reduceNum)
					{						
						paths[k][p[k]] = new Path(inputPath[j]);
						p[k]++;
					}
			}
		try {
			System.out.println("wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww");
			for(int i=0;i< this.reduceNum;i++){
				fs.mkdirs(new Path(outputPath[i]));
			}
			for(int i=0;i<this.reduceNum;i++){
				FileUtil.copy(fs, paths[i], fs, new Path(outputPath[i]), true, true, conf);
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public  static void  main(String[] args){
		String[] input = new String[80];
		String a = "hdfs://10.10.102.21:26666/user/jiangbing/0_0_8outputPath3 hdfs://10.10.102.21:26666/user/jiangbing/0_0_19outputPath1 hdfs://10.10.102.21:26666/user/jiangbing/0_0_9outputPath3 hdfs://10.10.102.21:26666/user/jiangbing/0_0_2outputPath1 hdfs://10.10.102.21:26666/user/jiangbing/0_0_13outputPath0 hdfs://10.10.102.21:26666/user/jiangbing/0_0_14outputPath0 hdfs://10.10.102.21:26666/user/jiangbing/0_0_6outputPath1 hdfs://10.10.102.21:26666/user/jiangbing/0_0_10outputPath1 hdfs://10.10.102.21:26666/user/jiangbing/0_0_15outputPath0 hdfs://10.10.102.21:26666/user/jiangbing/0_0_16outputPath0 hdfs://10.10.102.21:26666/user/jiangbing/0_0_17outputPath0 hdfs://10.10.102.21:26666/user/jiangbing/0_0_10outputPath3 hdfs://10.10.102.21:26666/user/jiangbing/0_0_18outputPath0 hdfs://10.10.102.21:26666/user/jiangbing/0_0_12outputPath2 hdfs://10.10.102.21:26666/user/jiangbing/0_0_19outputPath0 hdfs://10.10.102.21:26666/user/jiangbing/0_0_15outputPath1 hdfs://10.10.102.21:26666/user/jiangbing/0_0_16outputPath3 hdfs://10.10.102.21:26666/user/jiangbing/0_0_14outputPath3 hdfs://10.10.102.21:26666/user/jiangbing/0_0_9outputPath1 hdfs://10.10.102.21:26666/user/jiangbing/0_0_9outputPath2 hdfs://10.10.102.21:26666/user/jiangbing/0_0_9outputPath0 hdfs://10.10.102.21:26666/user/jiangbing/0_0_3outputPath1 hdfs://10.10.102.21:26666/user/jiangbing/0_0_8outputPath0 hdfs://10.10.102.21:26666/user/jiangbing/0_0_7outputPath0 hdfs://10.10.102.21:26666/user/jiangbing/0_0_2outputPath3 hdfs://10.10.102.21:26666/user/jiangbing/0_0_6outputPath0 hdfs://10.10.102.21:26666/user/jiangbing/0_0_19outputPath3 hdfs://10.10.102.21:26666/user/jiangbing/0_0_7outputPath1 hdfs://10.10.102.21:26666/user/jiangbing/0_0_5outputPath2 hdfs://10.10.102.21:26666/user/jiangbing/0_0_8outputPath2 hdfs://10.10.102.21:26666/user/jiangbing/0_0_1outputPath3 hdfs://10.10.102.21:26666/user/jiangbing/0_0_1outputPath0 hdfs://10.10.102.21:26666/user/jiangbing/0_0_0outputPath0 hdfs://10.10.102.21:26666/user/jiangbing/0_0_11outputPath1 hdfs://10.10.102.21:26666/user/jiangbing/0_0_3outputPath3 hdfs://10.10.102.21:26666/user/jiangbing/0_0_5outputPath0 hdfs://10.10.102.21:26666/user/jiangbing/0_0_4outputPath0 hdfs://10.10.102.21:26666/user/jiangbing/0_0_3outputPath0 hdfs://10.10.102.21:26666/user/jiangbing/0_0_18outputPath3 hdfs://10.10.102.21:26666/user/jiangbing/0_0_2outputPath0 hdfs://10.10.102.21:26666/user/jiangbing/0_0_16outputPath1 hdfs://10.10.102.21:26666/user/jiangbing/0_0_6outputPath2 hdfs://10.10.102.21:26666/user/jiangbing/0_0_4outputPath3 hdfs://10.10.102.21:26666/user/jiangbing/0_0_7outputPath2 hdfs://10.10.102.21:26666/user/jiangbing/0_0_8outputPath1 hdfs://10.10.102.21:26666/user/jiangbing/0_0_6outputPath3 hdfs://10.10.102.21:26666/user/jiangbing/0_0_11outputPath3 hdfs://10.10.102.21:26666/user/jiangbing/0_0_0outputPath1 hdfs://10.10.102.21:26666/user/jiangbing/0_0_17outputPath1 hdfs://10.10.102.21:26666/user/jiangbing/0_0_0outputPath2 hdfs://10.10.102.21:26666/user/jiangbing/0_0_7outputPath3 hdfs://10.10.102.21:26666/user/jiangbing/0_0_17outputPath2 hdfs://10.10.102.21:26666/user/jiangbing/0_0_16outputPath2 hdfs://10.10.102.21:26666/user/jiangbing/0_0_4outputPath1 hdfs://10.10.102.21:26666/user/jiangbing/0_0_12outputPath1 hdfs://10.10.102.21:26666/user/jiangbing/0_0_18outputPath2 hdfs://10.10.102.21:26666/user/jiangbing/0_0_17outputPath3 hdfs://10.10.102.21:26666/user/jiangbing/0_0_4outputPath2 hdfs://10.10.102.21:26666/user/jiangbing/0_0_15outputPath3 hdfs://10.10.102.21:26666/user/jiangbing/0_0_19outputPath2 hdfs://10.10.102.21:26666/user/jiangbing/0_0_1outputPath1 hdfs://10.10.102.21:26666/user/jiangbing/0_0_14outputPath2 hdfs://10.10.102.21:26666/user/jiangbing/0_0_3outputPath2 hdfs://10.10.102.21:26666/user/jiangbing/0_0_13outputPath1 hdfs://10.10.102.21:26666/user/jiangbing/0_0_0outputPath3 hdfs://10.10.102.21:26666/user/jiangbing/0_0_18outputPath1 hdfs://10.10.102.21:26666/user/jiangbing/0_0_5outputPath1 hdfs://10.10.102.21:26666/user/jiangbing/0_0_12outputPath0 hdfs://10.10.102.21:26666/user/jiangbing/0_0_11outputPath0 hdfs://10.10.102.21:26666/user/jiangbing/0_0_12outputPath3 hdfs://10.10.102.21:26666/user/jiangbing/0_0_5outputPath3 hdfs://10.10.102.21:26666/user/jiangbing/0_0_10outputPath2 hdfs://10.10.102.21:26666/user/jiangbing/0_0_10outputPath0 hdfs://10.10.102.21:26666/user/jiangbing/0_0_14outputPath1 hdfs://10.10.102.21:26666/user/jiangbing/0_0_15outputPath2 hdfs://10.10.102.21:26666/user/jiangbing/0_0_1outputPath2 hdfs://10.10.102.21:26666/user/jiangbing/0_0_13outputPath3 hdfs://10.10.102.21:26666/user/jiangbing/0_0_2outputPath2 hdfs://10.10.102.21:26666/user/jiangbing/0_0_13outputPath2 hdfs://10.10.102.21:26666/user/jiangbing/0_0_11outputPath2";
		String[] A = a.split(" ");
		System.out.println(A.length);
		for(int i = 0;i<80;i++){
			input[i] = A[i]; 
		}
		String b = "hdfs://10.10.102.21:26666/user/jiangbing/1_1_0outputPath0 hdfs://10.10.102.21:26666/user/jiangbing/1_1_0outputPath1 hdfs://10.10.102.21:26666/user/jiangbing/1_1_0outputPath2 hdfs://10.10.102.21:26666/user/jiangbing/1_1_0outputPath3";
		String[] B = b.split(" ");
		
		String[] output = new String[4];
		
		for(int i=0;i<B.length;i++){
			output[i] = B[i];
		}
		WordCountPartition wcp = new WordCountPartition();
		wcp.operate(input, output);
	}
}
