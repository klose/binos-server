package com.klose.common.TransformerIO;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;


public interface VertexReader {
		public int read() throws IOException;
		public BufferedReader getBufferedReaderStream() throws IOException;
		public InputStream getInputStream() throws IOException;
		public int read(char[] cbuf, int offset, int length) throws IOException;
		public int read(byte[] buf, int offset, int length) throws IOException;
		public String readline() throws IOException;
		public void close() throws IOException;

}
