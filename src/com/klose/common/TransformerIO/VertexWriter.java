package com.klose.common.TransformerIO;

import java.io.IOException;
import java.util.Map;

public interface VertexWriter {
		public void write() throws IOException;
		public void write(String str) throws IOException;
		public void write(Map map, String dst) throws IOException;
		public void write(String src, String dst) throws IOException;
		public void write(boolean delSrc, String src, String dst) throws IOException;
		public void create(String f) throws IOException;
		public void close() throws IOException;
}
