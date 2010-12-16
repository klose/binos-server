package com.klose.common;

import java.io.IOException;
import java.util.Map;

public interface VertexReader {
		public int read() throws IOException;

		public int read(char[] cbuf, int offset, int length) throws IOException;
		public int read(byte[] buf, int offset, int length) throws IOException;
		public String readline() throws IOException;
		public void close() throws IOException;

}
