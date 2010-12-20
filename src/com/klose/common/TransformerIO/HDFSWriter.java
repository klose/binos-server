package com.klose.common.TransformerIO;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSWriter implements VertexWriter, Serializable{
	private  Configuration conf = new Configuration();
	private  FileSystem fs = FileSystem.get(conf);
	FSDataOutputStream ops = null;
	public HDFSWriter(String path) throws IOException {
		this.ops = fs.create(new Path(path));
	}
	
	public void write(String str) throws IOException {
		ops.write(str.getBytes());
		
	// TODO Auto-generated method stub	
	}
	public void write(String src, String dst) throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path srcpath = new Path(src);
		Path dstpath = new Path(dst);
		if(src.startsWith("/"))
			fs.copyFromLocalFile(srcpath, dstpath);
		if(src.startsWith("hdfs"))
			FileUtil.copy(fs, srcpath, fs, dstpath, false, conf);
		fs.close();
	}
	public void write(boolean delSrc, String src, String dst) throws IOException{
		Path srcpath = new Path(src);
		Path dstpath = new Path(dst);
		fs.copyFromLocalFile(delSrc, srcpath, dstpath);
	}
	public void create(String f) throws IOException{
		Path p = new Path(f);
		fs.create(p);
	}
	public void create(String f, boolean overwrite) throws IOException{
		Path p = new Path(f);
		fs.create(p, overwrite);
	}
	@Override
	public void write() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		fs.close();
		ops.close();
		// TODO Auto-generated method stub		
	}

	/*
	 * a new interface for reading files with the type of Map.
	 * 
	 * @param map the Map to be written.
	 * @param dst the string of output data path
	 * 
	 * @exception IOException if an I/O error occur
	 * @exception ClassNotFoundException if an class can not be found.
	 * */
	@Override
	public void write(Map map, String dst) throws IOException {
		// TODO Auto-generated method stub
		FileOutputStream fs = new FileOutputStream(dst);
		ObjectOutputStream ops = new ObjectOutputStream(fs);
		ops.writeObject(map);
		fs.close();
		ops.close();
	}
}
