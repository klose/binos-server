package temporary;


import java.io.BufferedReader;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import com.klose.Slave.Slave;



public class HDFSReader {
	private static final Logger LOG = Logger.getLogger(HDFSReader.class.getName());
//	private FSNamesystem fs;
//	public HDFSReader(FSNamesystem fsn) {
//		this.fs = fsn;
//		
//	}
//	
//	public HDFSReader() {
//		
//	}
	
	public static void main(String [] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream fsdi = null;
		Text txt = new Text();
		Path p = new Path("hdfs://10.5.0.55:26666/user/jiangbing/test/core-default.xml");
		if(! fs.exists(p) ) {
			LOG.log(Level.WARNING, "The path not exists.");
			System.exit(1);
		}
		else {
			LineReader lr = new LineReader(fs.open(p));
			while( lr.readLine(txt) > 0) {
				System.out.println(txt.toString());
			}
		}
	}
}
