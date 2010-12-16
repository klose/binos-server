package com.klose.common;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Writer;

/*
 * produces and stores writer array which indicates different writer functions.  
 * */
public class WriterFactory {
	VertexWriter writer;
	/*
	 * instance the Writer in according to Map map's output path
	 * */
	public WriterFactory (String outpath) throws IOException {
		if(outpath.startsWith("/")){
			writer = new LocalWriter(outpath);
		}
		else 
			if(outpath.startsWith("hdfs"))
			{
				writer = new HDFSWriter(outpath);
			}
			else{
				System.out.println("wrong input");
				System.exit(2);
			}
	}
	public VertexWriter  getWriter() {
		//return the writer as the output path sequences. 
		return writer;	
	}
}
