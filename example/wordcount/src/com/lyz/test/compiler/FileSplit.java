package com.lyz.test.compiler;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
/**
 * @author Yuanzheng Lu
 * @param file source file
 * @param count the number which file to be splited.
 * @throws Exception
 * */
public class FileSplit {
		public static String[] split(String file, int count) throws Exception{
			String[] subfiles = new String[count];
			RandomAccessFile raf = new RandomAccessFile(new File(file),"r");
			long length = raf.length();
			
			long theadMaxSize = length/count;
			raf.close();
			
			long offset = 0L;
			for(int i=0;i<count - 1;i++){
				long fbegin = offset;
				long fend = (i+1)*theadMaxSize;
				String subfile = file + "_"+i+".tmp";
				subfiles[i] = subfile;
				offset = write(file,subfile,i,fbegin,fend);
			}
			
			if(length - offset >= 0)
				{
					String subfile = file + "_"+ String.valueOf(count - 1) +".tmp";
					subfiles[count - 1] = subfile;
					write(file,subfile,count-1,offset,length);
				}
			return subfiles;
		}
		
		/**
		 * @author lyz
		 * write used for splited files.
		 * @param file source file
		 * @param index the mark of file order.
		 * @param begin position of file begin.
		 * @param end position of file end.
		 * @return
		 * @throws Exception
		 * */
		private static long write(String file, String subfile, int index, long begin, long end) throws IOException {
			RandomAccessFile in = new RandomAccessFile(new File(file),"r");
			RandomAccessFile out = new RandomAccessFile(new File(subfile),"rw");
			byte[] b = new byte[1024];
			int n=0;
			in.seek(begin);
			
			while(in.getFilePointer() <= end && (n = in.read(b)) != -1)
			{
				out.write(b, 0, n);
			}
			long endPointer = in.getFilePointer();
			in.close();
			out.close();
			return endPointer;
		}
		
		/**
		 * merge files
		 * @param file the file after merged.
		 * @param tempFiles the files to be merged.
		 * @param tempCount the number of files.
		 * @throws IOException 
		 * @throws Exception
		 * */
		public static void merge(String file, String tempFiles, int tempCount) throws IOException{
			RandomAccessFile ok = new RandomAccessFile(new File(file),"rw");
			for(int i=0;i<tempCount;i++){
				RandomAccessFile read = new RandomAccessFile(new File(tempFiles+"_"+i+".tmp"),"r");
				byte[] b = new byte[1024];
				int n=0;
				while((n=read.read(b))!=-1){
					ok.write(b, 0, n);
				}
				read.close();
			}
			ok.close();
		}

		public String[] testSplit(String file,int splitNumber) throws Exception {
			String[] subfiles = new String[splitNumber];
			subfiles = FileSplit.split(file, splitNumber);	
			return subfiles;
		}
		/*public void testMerge() throws IOException {
			FileSplit.merge("/home/lyz/mergeResult", "/home/lyz/a.c", 5);
		}*/
		
		public static void main(String[] args){
			String file = args[0];
			FileSplit fsp = new FileSplit();
			Configuration conf = new Configuration();
			FileSystem fs = null;
			
			try {
					fs = FileSystem.get(conf);
					System.out.println(fs.toString());
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			try {
				String[] subfiles = fsp.testSplit(file,20);
				String prefix = "hdfs://10.5.0.55:26666/user/jiangbing/";
				Path[] src = new Path[subfiles.length];
				Path[] dst = new Path[subfiles.length];
				for(int i=0;i< subfiles.length;i++){
					System.out.println(subfiles[i]);
					src[i] = new Path(subfiles[i]);
					dst[i] = new Path(prefix+"input"+i );
				}
				
				for(int i=0;i<subfiles.length;i++){
					fs.copyFromLocalFile(true,src[i], dst[i]);
				}
				
		//		fs.testMerge();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
}

