package com.klose.common.TransformerIO;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import com.klose.common.HttpClient;

/**
 * FileUtil provides the utility of file
 * @author Bing Jiang
 *
 */
public class FileUtility {
	private static final Logger LOG = Logger.getLogger(FileUtility.class.getName());
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
		else if(path.length() >= 7 && path.substring(0, 7).equals(hdfsHeader)) {
			return FStype.HDFS;
		}
		else if(Pattern.matches(remoteHeaderRegex, path)) {
			return FStype.REMOTE;
		}
		else {
			return FStype.UNRECOGNIZED;
		}
	}
	
	public static String getHDFSAbsolutePath(String path) {
		String result = "";
		Path p = new Path(path);
		Configuration conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(conf);
			result +=  (fs.getWorkingDirectory().toString() + "/" + p.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
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
	 * check the directory valid
	 * @param path
	 * @return
	 */
	public static boolean checkDirectoryValid(String path, FStype type) {
		if(type == FStype.LOCAL) {
			return ensureLocalDirectory(path);
		}
		else if(type == FStype.HDFS) {
			return ensureHDFSDirectory(path);
		}
		else if (type == FStype.UNRECOGNIZED) {
			return false;
		}
		else {
			return false;
		}
	}
	/**
	 * get the prefix of path in the system of HDFS. 
	 * if the HDFS is open, it will return a String like below:
	 * HDFS://***.***.****.***:port/user/name/
	 * if the HDFS doesn't work, it will return null
	 * @return the prefix of path of HDFS
	 * 
	 */
	public static String getHDFSPrefix() {
		Configuration conf = new Configuration();
		String res = null;
		try {
			FileSystem fs = FileSystem.get(conf);
			res = fs.getHomeDirectory().toString();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			res = null;
		}
		return res;
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
	
	/**
	 * Judge the file with "/" exists in the local file system, If the file exists,
	 * it will return true, else, it will return false. 
	 **/
	private static boolean ensureLocalFile(String path) {
		File testFile = new File(path);
		//if(testFile.exists() && testFile.isFile()) {
		if(testFile.exists()) {	
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
	/**
	 * Judge whether the path is directory in the hdfs.
	 * @param path
	 * @return
	 */
	private static boolean ensureHDFSDirectory(String path) {
		Configuration conf = new Configuration();
		Path p = new Path(path);
		try {
			FileSystem fs = FileSystem.get(conf);
			return fs.isDirectory(p);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
		String[] tmp = remotePath.split(":");
		String server_ip = "";
		int server_port = 0;
		String filePath = "";
		if(tmp.length < 3) {
			LOG.log(Level.WARNING, " An error occurs when parsing the path from remote machine");
		}
		else  {
			server_ip = tmp[0];
			try {
				server_port = Integer.parseInt(tmp[1]);
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				LOG.log(Level.WARNING, "An error occurs when parse "+tmp[1]+ " to a number.");
//				e.printStackTrace();
			}
			for(int i = 2; i < tmp.length; i++) {
				filePath += tmp[i];
			}
			
		}
		HttpClient httpClient = new HttpClient(server_ip, server_port);
		String resPath = httpClient.transFileToDataDir(filePath, localDir);
		if( resPath != null) {
			return resPath;
		}
		else {
			return null;
		}
		
	}
	/**
	 * copy the file to another specified path. It will not delete the source file.
	 * @param originPath
	 * @param newPath
	 * @return
	 */
	public static String copyLocalFileName(String originPath, String newPath) {
		
		if(ensureLocalFile(originPath)) {
			if(!ensureLocalFile(newPath)) {
				Path srcPath = new Path(originPath);
				Path dstPath = new Path(newPath);
				Configuration conf = new Configuration();
				try {
					FileUtil.copy(FileSystem.getLocal(conf), srcPath, FileSystem.getLocal(conf), dstPath, false, conf);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return null;
				}
				return newPath;
			}
			else {
				LOG.log(Level.WARNING, " newPath:"+ newPath + " exists already!");
				return null;
			}
		}
		else {
			LOG.log(Level.WARNING, "originPath:" + originPath + "doesn't exist!");
			return null;
		}
	}
	/**
	 * @deprecated
	 * rename the file specified by originName to the file specified by newName
	 * NOTICE: The function cannot work when the originPath and the newPath is not in
	 * a single fileSystem.
	 * @param originPath: the former file path
	 * @param newPath : the new file path
	 * @return the new file path
	 */
	public static String renameLocalFileName(String originPath, String newPath) {
		
		if(ensureLocalFile(originPath)) {
			if(!ensureLocalFile(newPath)) {
				File src = new File(originPath);
				File dest = new File(newPath);
				if(src.renameTo(dest)) {
					return newPath;
				}
				else {
					LOG.log(Level.WARNING,"rename failure!");
					return null;
				}
			}
			else {
				LOG.log(Level.WARNING, " newPath:"+ newPath + " exists already!");
				return null;
			}
		}
		else {
			LOG.log(Level.WARNING, " originPath:"+ originPath + " doesnot exist!");
			return null;
		}
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
					LOG.log(Level.INFO, p.toString() + " doesn't exist.");
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
	public static void removeLocalFile(String path) {
		File file  = new File(path);
		if(file.exists()) {
			file.delete();
		}
	}
	/**
	 * copy local directory to HDFS.
	 */
	public static void copyLocalDirToHDFS(File localDir, String hdfsDir) {
		Path dstPath = new Path(hdfsDir);
		Path srcPath = new Path(localDir.getAbsolutePath());
		Configuration conf = new Configuration();
		try {
			FileSystem dstFs = FileSystem.get(conf);
			dstFs.copyFromLocalFile(srcPath, dstPath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.log(Level.INFO, dstPath.getName() + " has already get the "+srcPath.getName());
			//e.printStackTrace();
			
		}
	}
	  /**
	   * Create the given dir in HDFS
	   */
	  public static void mkdirInHDFS(String src) throws IOException {
	    Path f = new Path(src);
	    Configuration conf = new Configuration();
	    FileSystem srcFs = f.getFileSystem(conf);
	    FileStatus fstatus = null;
	    try {
	      fstatus = srcFs.getFileStatus(f);
	      if (fstatus.isDir()) {
	        throw new IOException("cannot create directory " 
	            + src + ": File exists");
	      }
	      else {
	        throw new IOException(src + " exists but " +
	            "is not a directory");
	      }
	    } catch(FileNotFoundException e) {
	        if (!srcFs.mkdirs(f)) {
	          throw new IOException("failed to create " + src);
	        }
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
			LOG.log(Level.INFO, "hdfsFile: " + hdfsFile + " localDir: " + localDir);
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
