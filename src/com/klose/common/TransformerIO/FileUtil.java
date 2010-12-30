package com.klose.common.TransformerIO;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.klose.Master.TaskXMLCollector;

/**
 * FileUtil provides the utility of file
 * @author Bing Jiang
 *
 */
public class FileUtil {
	private static final Logger LOG = Logger.getLogger(FileUtil.class.getName());
	private final static  String hdfsHeader =  "hdfs://";
	private final static  String localHeader = "/";
	private final static  String remoteHeaderRegex = 
		"[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}:[0-9]{4,5}"
			+ ":/[\\w\\W]*";
	public static enum FStype {
		LOCAL, HDFS, REMOTE, UNRECOGNIZED
	}; //support local file system and hdfs system currently.
	/*get the type of file.*/
	public static  FStype getFileType(String path) {
		if(path.charAt(0) == '/') {
			//local file system
			return FStype.LOCAL;
		}
		else if(path.substring(0, 7).equals(hdfsHeader)) {
			return FStype.HDFS;
		}
		else {
			return FStype.UNRECOGNIZED;
		}
	}
	/**
	 * If dir path exists, it will return true.
	 * If dir path doesnot exist, it will make the directory using dirPath as path, 
	 * and it will return true when creating successfully, return false as failure.   
	 * @param dirPath
	 * @return 
	 */
	public static boolean mkdirLocalDir(String dirPath) {
		File dir = new File(dirPath);
		if(dir.exists() && dir.isDirectory()) {
			return true;
		}
		else {
			if(dir.mkdirs()) {
				return true;
			}
			else {
				return false;
			}
		} 
	}
	/**
	 * Check the file path valid.
	 * @param path
	 * @return
	 */
	public static boolean checkFileValid(String path) {
		FStype type = getFileType(path);
		if( type == FStype.LOCAL) {
			return ensureLocalFile(path);
		}
		else if (type == FStype.HDFS) {
			return ensureHDFSFile(path);
		}
		else if (type == FStype.UNRECOGNIZED) {
			return false;
		}
		else {
			return false;
		}
	}
	
	/*Judge the file with "/" exists in the local file system, If the file exists,
	 * it will return true, else, it will return false. 
	 **/
	private static boolean ensureLocalFile(String path) {
		File testFile = new File(path);
		if(testFile.exists() && testFile.isFile()) {
			return true;
		}
		else {
			return false;
		}
	}
	
	/**
	 * Judge whether the path is directory in the local file system. 
	 * @param path
	 * @return
	 */
	private static boolean ensureLocalDirectory(String path) {
		File testFile = new File(path);
		if(testFile.exists() && testFile.isDirectory()) {
			return true;
		}
		else {
			return false;
		}
	}
	/*Judge the file with "hdfs://" exists in the hdfs, If the file exists,
	 * it will return true, else, it will return false. 
	 **/
	private static boolean ensureHDFSFile(String path){
		Configuration conf = new Configuration();
		Path p = new Path(path);
		try {
			FileSystem fs = FileSystem.get(conf);
			if(fs.isFile(p)){
				return true;
			}
			else {
				return false;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	/**
	 * Transmit file form remote slave node to local file system, using Http connection.		
	 * @param remotePath: Slave-IP:httpServerPort:/absolute/path, for example 10.5.0.55:8081:/tmp/input 
	 * @param localDir absolute file path in the local file system
	 * @return
	 */
	public static String TransRemoteFileToLocal(String remotePath, String localDir){
		
	}
	/**
	 * Put the local file into the directory of hdfs. 
	 * It will return the hdfs file path generated.
	 * @param localpath: use the absolute path of file
	 * @param hdfsDir: use the relative path in the hdfs.
	 */
	public static String TransLocalFileToHDFS(String localpath, String hdfsDir) {
		try {
			if(ensureLocalFile(localpath)) {
				Configuration conf = new Configuration();
				Path p = new Path(hdfsDir);
				FileSystem fs = FileSystem.get(conf);
				if(fs.isDirectory(p)) {
					fs.copyFromLocalFile(new Path(localpath), p);
					String[] tmp = localpath.split("/");
					String filename = tmp[tmp.length -1];
					if (p.isAbsolute()) {
						return p.toString() + "/" + filename;
					}
					else {
						return fs.getWorkingDirectory().toString() + "/" + p.toString()
						+ "/" + filename; 
					}
				}
				else {
					return null;
				}
			}
			else {
				return null;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.log(Level.SEVERE, e.toString());
			return null;
		}
	}
	/**
	 * Copy the file from hdfs to local file system.
	 * @param hdfsFile
	 * @param localDir : the absolute path of local directory 
	 * @return the local path.
	 */
	public static String TransHDFSToLocalFile(String hdfsFile, String localDir) {
		if(!ensureHDFSFile(hdfsFile) || !ensureLocalDirectory(localDir) ) {
			return null;
		}
		else {
			try {
				Configuration conf = new Configuration();
				FileSystem fs  = FileSystem.get(conf);
				Path hdfsPath = new Path(hdfsFile);
				Path localPath = new Path(localDir);
				fs.copyToLocalFile(hdfsPath, localPath);
				String[] tmp = hdfsPath.toString().split("/");
				return  localDir + "/" + tmp[tmp.length-1];
			} catch (IOException e) {
				// TODO Auto-generated catch block	
				LOG.log(Level.SEVERE, e.toString());
				return null;
			}
			
		}
	}
}
