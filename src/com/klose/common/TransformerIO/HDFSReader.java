package com.klose.common.TransformerIO;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Reader;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;


public class HDFSReader implements VertexReader, Serializable{

	FileSystem fs;
	FSDataInputStream fsr;
	private Configuration conf = new Configuration();
	Reader reader;
	BufferedReader br;
	LineReader lr;
	boolean exist = true;
	
	public HDFSReader(String path) throws IOException{
		
		System.out.println("HDFSReader:" +path);
		Configuration conf = new Configuration();

		Path pathq = new Path(path);
		this.fs = FileSystem.get(conf);
		if(exist == true){
			if(!fs.exists(pathq)){
				System.err.println("the path "+path+" does not exists!");
				System.exit(2);
			}		

			fsr = fs.open(pathq);	

			this.lr = new LineReader(fsr);
		}
		
		
	}
	
	/*
	 * Creates a HDFSReader class using FSDataInputStream methods
	 * 
	 * @exception IOException if an I/O error occur
	 * */
	@Override
	public int read() throws IOException {
		// TODO Auto-generated method stub
		return fsr.read();
	}
	/*
	 * Read a portion of a file into an array. 
	 * 
	 * @param cbuf An array.
	 * @param off Offset from which to start reading Characters.
	 * @param len Number of Characters to read.
	 * @exception IOException if an I/O error occur
	 * */
	@Override
	public int read(char[] cbuf, int offset, int length) throws IOException {
		// TODO Auto-generated method stub
		byte[] b = new byte[length + offset];
	    fsr.read(b, offset, length);
	    String s = new String(b);
	    s.getChars(0, s.length(), cbuf, offset);
	    return 0;
	}
	/*
	 * Reads a portion of a file into an array. 
	 * 
	 * @param buf An array.
	 * @param off Offset from which to start reading Bytes.
	 * @param len Number of Bytes to write.
	 * @exception IOException if an I/O error occur
	 * */
	@Override
	public int read(byte[] buf, int offset, int length) throws IOException {
		// TODO Auto-generated method stub
		return fsr.read(buf, offset, length);
	}
	
	/*
	 * Reads a file fully into a buffer.
	 * 
	 * @param buf An buffer.
	 * @exception IOException if an I/O error occur
	 * 
	 */
	public void read(byte[] buf) throws IOException
	{
		 fsr.readFully(buf);
	}

	/*
	 * closes the streams.
	 * */
	@Override
	public void close() throws IOException {
		lr.close();
		fsr.close();
		fs.close();
		
		// TODO Auto-generated method stub	
	}
	/*
	 * Copys hdfs files into local files
	 * 
	 * @param src the string of input data path
	 * @param dst the string of output data path
	 * 
	 * @exception IOException if an I/O error occur
	 * */
	public void copyToLocalFile(String src, String dst) throws IOException{
		Path srcpath = new Path(src);
		Path dstpath = new Path(dst);
		fs.copyToLocalFile(srcpath, dstpath);
	}
	/*
	 * Moves hdfs files into local files
	 * 
	 * @param src the string of input data path
	 * @param dst the string of output data path
	 * 
	 * @exception IOException if an I/O error occur
	 * */
	public void movToLocalFile(String src, String dst) throws IOException{
		Path srcpath = new Path(src);
		Path dstpath = new Path(dst);
		fs.moveToLocalFile(srcpath, dstpath);
	}


	/*
	 * Copys a line of a file into a string.
	 * 
	 * when reach the end of the file , the function will return null.
	 * @exception IOException if an I/O error occur
	 * */
	@Override
	public String readline() throws IOException {
		// TODO Auto-generated method stub
		Text str = new Text();

		int t = lr.readLine(str);
		if(t == 0)
			return null;

		String s = str.toString();
		return s;
	}

	@Override
	public InputStream getInputStream() throws IOException {
		// TODO Auto-generated method stub
		
		return this.fsr;
	}

	@Override
	public BufferedReader getBufferedReaderStream() throws IOException {
		// TODO Auto-generated method stub
		return this.br;
	}
	
}
