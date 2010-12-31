package com.klose.common.TransformerIO;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.io.BufferedReader;
import java.util.Iterator;
import java.util.Map;


public class LocalReader implements VertexReader, Serializable{
	FileReader fr;
	FileInputStream fis;
	BufferedReader br;
	public LocalReader(String path) throws FileNotFoundException{
		fr = new FileReader(path);
		fis = new FileInputStream(path);
		br = new BufferedReader(fr);
		
	}
	@Override
	public int read() throws IOException {
		// TODO Auto-generated method stub
		return fr.read();
	}

	@Override
	public int read(char[] cbuf, int offset, int length) throws IOException{
		// TODO Auto-generated method stub
		return fr.read(cbuf, offset, length);
	}

	/*
	 * Reads a portion of a file into a buffer. 
	 * 
	 * @param buf A buffer.
	 * @param off Offset from which to start reading Bytes.
	 * @param len Number of Bytes to write.
	 * @exception IOException if an I/O error occur
	 * */
	@Override
	public int read( byte[] buf, int offset, int length) throws IOException {
		
		char[] cbuf = new char[offset + length];
		if(length/2 == 0)
			fr.read(cbuf, offset, length/2);
		else
			fr.read(cbuf, offset, length/2+1);
		int i = 0;
		int k = 0;
		int tmp = 0;
		while(i < length/2){			
			tmp = (int)cbuf[offset + i];
			buf[k + offset] = new Integer(tmp & 0xff).byteValue();
			tmp = tmp >> 8;
			buf[k + 1 + offset] = new Integer(tmp & 0xff).byteValue();
			k += 2;
			i++;
		}
		if(length/2 != 0){
			tmp = (int)cbuf[offset + length/2 + 1];
			buf[k + offset] = new Integer(tmp & 0xff).byteValue();

		}
		// TODO Auto-generated method stub
		return fis.read(buf, offset, length);
	}
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		fr.close();
		
	}
	
	/*
	 * Reads a line of text.  A line is considered to be terminated by any one
     * of a line feed ('\n'), a carriage return ('\r'), or a carriage return
     * followed immediately by a line feed.
	 *@return     A String containing the contents of the line, not including
     *             any line-termination characters, or null if the end of the
     *             stream has been reached.
	 * @exception IOException if an I/O error occur
	 * */
	public String readline() throws IOException{
		return br.readLine();
	}
	@Override
	public InputStream getInputStream() throws IOException {
		// TODO Auto-generated method stub
		return this.fis;
	}
	@Override
	public BufferedReader getBufferedReaderStream() throws IOException {
		// TODO Auto-generated method stub
		return this.br;
	}

}
