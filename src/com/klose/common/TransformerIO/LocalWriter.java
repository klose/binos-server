package com.klose.common.TransformerIO;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

public  class LocalWriter implements VertexWriter,Serializable{
		FileWriter fw ;
		FileOutputStream fs ;
		ObjectOutputStream ops;
		BufferedWriter bw;
		/*
		 * Creates a LocalWriter class using file path
		 * 
		 * @param path the path of file
		 * 
		 * @exception IOException if an I/O error occur
		 * */
		public LocalWriter(String path) throws IOException
		{
			this.fw = new FileWriter(path);
			this.bw = new BufferedWriter(fw);
		}
		/*
		 * Creates a LocalWriter class using a file path and a boolean indicating 
		 * whether or not to append the data written
		 * 
		 * @param path the path of file
		 * @param append  boolean if <code>true</code>, then data will be written
		 * to the end of the file rather than the beginning.
		 * 
		 * @exception IOException if an I/O error occur.
		 * */
		public LocalWriter(String path, boolean append) throws IOException
		{
			this.fw = new FileWriter(path, append);
		}
		/*
		 * Writes an array of characters.
		 * 
		 * @param cbuf An array
		 * 
		 * @exception IOException if an I/O error occur
		 * */
		public void write(char[] cbuf) throws IOException{
			 fw.write(cbuf);
		}
		/*
		 * Writes a single character. the character to be written is contained in 
		 * the lower 16-order bits of the given integer. the higher 16-bits are ignored.
		 * 
		 * @param c The integer.
		 * 
		 * @exception IOException if an I/O error occur
		 * */
		public void write(int c) throws IOException{
			fw.write(c);
		}
		/*
		 * Writes a string.
		 * 
		 * @param str The String.
		 * 
		 *  @exception IOException if an I/O error occur
		 * */
		public void write(String str) throws IOException{
			fw.write(str);
		}
		/*
		 * Writes a portion of an array. 
		 * 
		 * @param cbuf An array.
		 * @param off Offset from which to start writing Characters.
		 * @param len Number of Characters to write.
		 * */
		public void write(char[] cbuf, int off, int len) throws IOException{
			fw.write(cbuf, off, len);
		}
		/*
		 * Writes a portion of a String.
		 * 
		 * @param str A String
		 * @param off Offset from which to start writing Characters
		 * @param len Number of Characters to write
		 * 
		 * @exception IOException if an I/O error occur
		 * */
		public void write(String str, int off, int len) throws IOException{
			fw.write(str, off, len);
		}
		@Override
		public void write() throws IOException {
			// TODO Auto-generated method stub
			
		}
		@Override
		public void create(String f) throws IOException {
			// TODO Auto-generated method stub
			
		}
//		@Override
//		public void write1(String str) throws IOException {
//			// TODO Auto-generated method stub
//			
//		}
		
		/*
		 * copies or moves the source file to the destination file.
		 *
		 * @param delSrc indicates delete the source file or not.
		 * @param src string of source file path.
		 * @param dst string of destination file path.
		 * 
		 * @exception IOException if an I/O error occur.
		 * */
		
		@Override
		public void write(boolean delSrc, String src, String dst)
				throws IOException {
			
//			Path srcpath = new Path(src);
//			Path dstpath = new Path(dst);
			if(src.startsWith("/")){
				File srcFile = new File(src);
				File dstFile = new File(dst);
				if(!srcFile.exists()){
					System.out.println(srcFile.getAbsolutePath() + "is not exists!");
					System.exit(2);
				}
				if(!dstFile.exists()){
					dstFile.getParentFile().mkdir();
				}
				int readed = 0;
				int bytes = 0;
				byte[] buff = new byte[4 << 10];
				FileInputStream fis = new FileInputStream(srcFile);
				FileOutputStream fos = new FileOutputStream(dstFile);
				readed = fis.read(buff);
				/*loop the file until reach the end of the file*/
				while(readed != -1){
					fos.write(buff, 0, readed);
					bytes += readed;
					readed = fis.read(buff);
//					System.out.println(srcFile + " has copyed " + bytes + " to " + dstFile);
					
				}
				fis.close();
				fos.close();
				if(delSrc){
					srcFile.delete();
				}
			}
			else if(src.startsWith("hdfs")){
				Configuration conf = new Configuration();
				FileSystem hdfs = FileSystem.get(conf);
				LocalFileSystem localfs = FileSystem.getLocal(conf);
				Path srcpath = new Path(src);
				Path dstpath = new Path(dst);
				if(delSrc)
					hdfs.moveToLocalFile(srcpath, dstpath);
				else
					FileUtil.copy(hdfs, srcpath, localfs, dstpath, false, conf);
				hdfs.close();
				localfs.close();
				
				
			}	
			
			// TODO Auto-generated method stub
			
		}
		
		/*
		 * closes all the streams.
		 * */
		@Override
		public void close() throws IOException {
			fw.close();
			// TODO Auto-generated method stub
			
		}
		/*
		 * copies the source file to the destination file.
		 * 
		 * @param src string of source file path.
		 * @param dst string of destination file path.
		 * 
		 * @exception IOException if an I/O error occur.
		 * */
		@Override
		public void write(String src, String dst) throws IOException {
//			Path srcpath = new Path(src);
//			Path dstpath = new Path(dst);
			
			if(src.startsWith("/")){
				File srcFile = new File(src);
				File dstFile = new File(dst);
				if(!srcFile.exists()){
					System.out.println(srcFile.getAbsolutePath() + "is not exists!");
					System.exit(2);
				}
				if(!dstFile.exists()){
					dstFile.getParentFile().mkdir();
				}
				int readed = 0;
				int bytes = 0;
				byte[] buff = new byte[4 << 10];
				FileInputStream fis = new FileInputStream(srcFile);
				FileOutputStream fos = new FileOutputStream(dstFile);
				readed = fis.read(buff);
				/*loop the file until reach the end of the file*/
				while(readed != -1){
					fos.write(buff, 0, readed);
					bytes += readed;
					readed = fis.read(buff);
//					System.out.println(srcFile + " has copyed " + bytes + " to " + dstFile);
					
				}
				fis.close();
				fos.close();
			}
			else if(src.startsWith("hdfs")){
				Configuration conf = new Configuration();
				FileSystem hdfs = FileSystem.get(conf);
				LocalFileSystem localfs = FileSystem.getLocal(conf);
				Path srcpath = new Path(src);
				Path dstpath = new Path(dst);
				FileUtil.copy(hdfs, srcpath, localfs, dstpath, false, conf);
				hdfs.close();
				localfs.close();
			}
		}
/*
 * writes a object typed Map into the destination file.
 * 
 * @param map the Map.
 * @param dst string of destination file path.
 * 
 * @exception IOException if an I/O error occur.
 * */
		public void write(Map map, String dst) throws IOException{
			this.fs = new FileOutputStream(dst);
			this.ops = new ObjectOutputStream(fs);
			ops.writeObject(map);
			// TODO Auto-generated method stub
			
		}
}
