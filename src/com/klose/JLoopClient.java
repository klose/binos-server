package com.klose;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;

import com.google.protobuf.RpcCallback;
import com.googlecode.protobuf.socketrpc.SocketRpcChannel;
import com.googlecode.protobuf.socketrpc.SocketRpcController;
import com.klose.MsConnProto.ConfirmMessage;
import com.klose.MsConnProto.ExecOrder;
import com.klose.MsConnProto.ExecOrderResp;
import com.klose.MsConnProto.SlaveOrderExecutorService;
import com.klose.MsConnProto.TransformXMLPath;
import com.klose.MsConnProto.XMLPathTransService;
import com.klose.common.RunJar;
import com.klose.common.TransformerIO.FileUtility;
import com.klose.common.TransformerIO.FileUtility;
import com.klose.common.TransformerIO.FileUtility.FStype;
import com.transformer.compiler.JarCreator;
import com.transformer.compiler.JarResolver;
import com.transformer.compiler.JobConfiguration;
import com.transformer.compiler.TaskStructClassGene;


/**
 * JLoopClient: provide the API for submitting job to Master. JLoopClient is not
 * necessarily the module of the Slave daemon.
 * 
 * @author Bing Jiang
 * 
 */
public class JLoopClient {
	private String[] args_;
	private static HashMap<String, String> argsMap = new HashMap<String, String>();
	private static final Logger LOG = Logger.getLogger(JLoopClient.class.getName());
	private static final String workingDirectory = "/tmp/JLoopClient"; 
	private static final String jarNewName = "job.jar";
	public JLoopClient(String[] args) {
		this.args_ = args;
	}

	private void printUsage() {
		System.out
				.print("Usage: JLoopClient"
						+ " --url=MASTER_URL --jobJAR=PATH [--exec=ORDER] [...] "
						+ "\n"
						+ "MASTER_URL may be one of:"
						+ "\n"
						+ "  JLoop://id@host:port"
						+ "\n"
						+ "  zoo://host1:port1,host2:port2,..."
						+ "\n"
						+ "  zoofile://file where file contains a host:port pair per line"
						+ "\n"
						+ "Support options:\n"
						+ "    --help                   display this help and exit.\n"
						+ "    --url=VAL                URL to represent Master URL\n"
						+ "    --exec=VAL               ORDER which need to run \n"
						+ "    --jobJAR=VAL             jobJAR specifies the path of executable java jar to be submitted\n");
		System.exit(1);
	}

	/*
	 * parserArgs: parse the args and load information into argsMap(HashMap). if
	 * args has invalidate value, it will return false.
	 */
	private void parseArgs()  {
		// premise: the master url is like JLoop://id@host:port
		if (args_.length <= 1) {
			printUsage();
		}
		int length = this.args_.length;
		String urlRegex = "--url\\s*=\\s*JLoop://[0-9]+@"
				+ "[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}:[0-9]{4,5}";
		String execRegex = "--exec=";
		String jobJARRegex = "--jobJAR=";
		for (int i = 0; i < length; i++) {
			if (this.args_[i].equals("--help")) {
				printUsage();
			} else if (Pattern.matches(urlRegex, args_[i])) {
				String url = (this.args_[i].trim().split("="))[1].trim();
				String url_1 = url.trim().split("@")[1];
				this.argsMap.put("master-ip", (url_1.split(":"))[0].trim());
				this.argsMap.put("master-port", (url_1.split(":"))[1].trim());
			} else if (this.args_[i].trim().length() >= 7) {
				if (Pattern.matches(execRegex,
						this.args_[i].trim().substring(0, 7))) {
					String order = "";
					if (this.args_[i].trim().length() > 7) {
						order = this.args_[i].trim().substring(7);
					}
					i++;
					// String order = this.args_[i].trim().substring(7);
					while (i < length) {
						if (this.args_[i].trim().length() < 2) {
							order += "" + this.args_[i];
						} else if (this.args_[i].trim().substring(0, 2)
								.equals("--")) {
							this.argsMap.put("exec-order", order);
							i--;
							break;
						} else {
							order = order + " " + this.args_[i];
						}
						i++;
					}
					if (!this.argsMap.containsKey("exec-order")) {
						this.argsMap.put("exec-order", order);
					}
				}
				else if (Pattern.matches(jobJARRegex,
						this.args_[i].substring(0, jobJARRegex.length()))) {
					String path = "";
					if (this.args_[i].trim().length() > jobJARRegex.length()) {
						path = this.args_[i].trim().substring(jobJARRegex.length()).trim();
						this.argsMap.put("exec-jobJAR", path);
					} else {
						printUsage();
					}
				}
			} else if (this.args_[i].trim().length() >= jobJARRegex.length()) {
				if (Pattern.matches(jobJARRegex,
						this.args_[i].trim().substring(0, jobJARRegex.length()))) {
					String path = "";
					if (this.args_[i].trim().length() > jobJARRegex.length()) {
						path = this.args_[i].trim().substring(jobJARRegex.length()).trim();
						this.argsMap.put("exec-jobJAR", path);
					} else {
						printUsage();
					}
				}
			} else if (this.args_[i].equals("--help")) {
				printUsage();
			} else {
				System.out.println("Error arg: " + this.args_[i] + " ");
				printUsage();
			}
		}
		try {
			constructWorkingEnv(this.argsMap.get("exec-jobJAR"));
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	/**
	 * Copy the directory of according tasks from local job path.
	 * Every directory of task is named as to taskid.
	 * The catalog of the path is like this:
	 * ../1_1_201101112010   --- jobId
	 * ../1_1_201101112010/job-1_1_201101112010.xml  
	 * ../1_1_201101112010/1_1_1/	---- containing the details of task 1_1_1 
	 * ../1_1_201101112010/1_1_2/	---- containing the details of task 1_1_2
	 * ../1_1_201101112010/1_1_3/	---- containing the details of task 1_1_3
	 * ...........
	 * The function is used to copy all details except for the job.xml,
	 * transmitting from local job path to according hdfs path. 
	 *  @param localJobPath: the path of job
	 */
	private static void copyTaskDirPath(String localJobPath, String jobId) {
		String jobDirPath = localJobPath.substring(0, localJobPath.lastIndexOf("/"));
		File localJobDir = new File(jobDirPath);
		if(!localJobDir.exists()) {
			LOG.log(Level.WARNING, localJobPath + " doesn't exist.");
			return;
		}
		else {
			if(!localJobDir.isDirectory()) {
				LOG.log(Level.WARNING, localJobPath + " is a directory.");
				return ;
			}
			else {
				FileFilter filter = new FileFilter(){
				@Override
					public boolean accept(File pathname) {
						// TODO Auto-generated method stub
						if(pathname.isDirectory() && pathname.exists()) {
							return true;
						}
						return false;
					}
					
				};
				for(File taskDir : localJobDir.listFiles(filter)) {
					System.out.println(taskDir.toString() + ":" +taskDir.getAbsolutePath() );
					FileUtility.copyLocalDirToHDFS(taskDir, jobId);
					
				}
			}
		}
	}
	
	/**
	 * Normalize the path which is not provided by hdfs into file in the hdfs.
	 *  In the function, there are sometimes operations copying file from one file system to
	 *  hdfs.
	 * @param path: It is a absolute path in file system supported.
	 * @return the normalized hdfs absolute file path
	 */
	public static String normalizingUniformJobPath(String path) {
		FStype type = FileUtility.getFileType(path);
		if(type == FStype.LOCAL) {
			String[] tmp = path.split("/");
			String jobXMLName = tmp[tmp.length -1];
			LOG.log(Level.INFO, "xml:"+jobXMLName);
			String jobId = jobXMLName.substring(0, jobXMLName.lastIndexOf(".xml"));
			LOG.log(Level.INFO, "id:"+jobId);
			//String jobDirName = jobId.substring(jobId.lastIndexOf("_"),);
			String jobDirName =  jobId;
			LOG.log(Level.INFO, "dirname:"+jobDirName);
			try {
				if(!FileUtility.checkDirectoryValid(jobDirName, FStype.HDFS))
					FileUtility.mkdirInHDFS(jobDirName);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				LOG.log(Level.WARNING, "Can't create the directory in HDFS:"+jobDirName);
				System.exit(-1);
			}
//			while(true) {
//				if(FileUtil.checkDirectoryValid(jobDirName, FStype.HDFS)) {
//					break;
//				}
//			}
			String generatedPath = FileUtility.TransLocalFileToHDFS(path, jobDirName);
			copyTaskDirPath(path, jobDirName);
			if (generatedPath != null){
				// hdfs://***.***.***.***:*****/user/***/task is chosen as directory path.
				return generatedPath;
			}
			else {
				LOG.log(Level.SEVERE, "copy "+ path.toString() + " happens error.");
				return null;
			}
		}
		else if(type == FStype.HDFS) {
			if( FileUtility.checkFileValid(path) ) {
				return path;
			}
			else {
				LOG.log(Level.INFO, path + " doesn't exist.");
				return null;
			}
		}
		else {
			return null;
		}
	}
	public void printArgMap() {
		System.out.println(this.argsMap.toString());
	}

	public String findKeyInMap(String key) {
		return this.argsMap.get(key);
	}
	public static String copyJobJarPath(String jarPath, String jobDir) {
		return FileUtility.TransLocalFileToHDFS(jarPath, jobDir);
	}
	
	/**
	 * Construct the environment that job relies on.
	 * Execute the specified jar that generate the working directory.
	 * @param jarPath: specify the path of jar.
	 * @throws Throwable 
	 */
	private static void constructWorkingEnv(String jarPath) throws Throwable {
		if(!FileUtility.checkFileValid(jarPath)) {
			LOG.log(Level.WARNING, "jarPath:"+jarPath + " doesnot exist!");
			System.exit(-1);
		}
		String[] args = new String[1];
		args[0] = new String(jarPath);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
		Date date  = new Date();
		String str = sdf.format(date);
		JobConfiguration.setWorkingDirectory(workingDirectory);
		JobConfiguration.setCreateTime(str);
		JobConfiguration.setPathHDFSPrefix(FileUtility.getHDFSPrefix() + "/job-" + str);
		String localTmpJobXMLPath = JobConfiguration.getWorkingDirectory()
					+ "/" + str + "/job-" + str + ".xml";  
		RunJar.run(args);
		if(!FileUtility.checkDirectoryValid(workingDirectory+"/"+str, com.klose.common.TransformerIO.FileUtility.FStype.LOCAL)) {
			LOG.log(Level.WARNING, "It occurs to an error when generating a job.");
			System.exit(-1);
		}
		else {
			String  newPath = normalizingUniformJobPath(localTmpJobXMLPath);
			if(newPath == null) {
				System.exit(-1);
			}
			else {
				argsMap.put("exec-jobXML", newPath);
			}
//			String jarNewPath = JobConfiguration.getWorkingDirectory()
//			+ "/" + str + "/job.jar";
//			constructJobJar(jarPath, jarNewPath);
			// rename the jar, put the jar into the corresponding job directory and upload the jar into HDFS.
			LOG.log(Level.INFO, "rename File Name:" + jarPath + " to " + workingDirectory + "/" + str + "/"+ jarNewName);
			String jarNewPath = FileUtility.copyLocalFileName(jarPath, workingDirectory + "/" + str + "/"+ jarNewName);
			LOG.log(Level.INFO, "jarNewPath" + jarNewPath);
			if( null != copyJobJarPath(jarNewPath,"job-" + str) )  {
				//FileUtil.removeLocalFile(jarNewPath);
				LOG.log(Level.INFO, jarNewPath + " upload the file into the HDFS" );
			}
		}
	}
	/**
	 * @deprecated
	 * Construct the job.jar.
	 * @throws IOException 
	 */
	private static void constructJobJar(String jarPath, String jobJarPath) throws IOException {
		String className =  "TaskStructClassGene.class";
		Class cls = TaskStructClassGene.class;
		File workingDir  = new File(workingDirectory);
		RunJar.ensureDirectory(workingDir);
		File jarFile = new File(jarPath);
		final File workDir = new File(workingDirectory, "Transformer-unjar");	
	    RunJar.ensureDirectory(workDir);
	    JarResolver resolver = new JarResolver(jarPath, workDir.getPath());
	    resolver.unJar();
	    System.out.println(workDir.getPath());
	    Runtime.getRuntime().addShutdownHook(new Thread() {
	        public void run() {
	        	FileUtil.fullyDelete(workDir);
	        }
	      });
	    String classNamePath = workDir.getPath() + "/" + cls.getName().replace('.', '/') +".class";
	    System.out.println(classNamePath);
	    InputStream in = cls.getResourceAsStream(className);
	    RunJar.ensureDirectory(new File(classNamePath).getParentFile());
	    OutputStream out = new FileOutputStream(classNamePath);
	    PrintStream ps = out instanceof PrintStream ? (PrintStream)out : null;
	    byte buf[] = new byte[1024];
	    int bytesRead = in.read(buf);
	    while (bytesRead >= 0) {
	      out.write(buf, 0, bytesRead);
	      if ((ps != null) && ps.checkError()) {
	        throw new IOException("Unable to write to output stream.");
	      }
	      bytesRead = in.read(buf);
	    }
	    out.close();
	    in.close();
	    File META_INF_tmpDir = new File(workDir.getPath()+"/META-INF/MANIFEST.MF");
	    if(META_INF_tmpDir.exists()) {
	    	System.out.println(META_INF_tmpDir.delete());
	    }	      
	    JarCreator creator = new JarCreator(workDir.getPath(), cls.getName(), jobJarPath);
	    creator.createJar();
	}
	/*
	 * This main function is used to test.
	 */
	public static void main(String [] args) {
		
		JLoopClient client = new JLoopClient(args);
		client.parseArgs();
		client.printArgMap();
		
		SocketRpcChannel socketRpcChannel = new SocketRpcChannel(client.findKeyInMap("master-ip"), 
				Integer.parseInt(client.findKeyInMap("master-port")) );
		SocketRpcController rpcController = socketRpcChannel.newRpcController();
		
		/*transmit the XML path to Master*/
		
		XMLPathTransService transService = XMLPathTransService.newStub(socketRpcChannel);
		TransformXMLPath transPath = TransformXMLPath.newBuilder()
							.setPath(client.findKeyInMap("exec-jobXML")).build();
		transService.xmlTrans(rpcController, transPath, new RpcCallback<com.klose.MsConnProto.ConfirmMessage>(){
			
			@Override
			public void run(ConfirmMessage message) {
				// TODO Auto-generated method stub
				if(message.getIsSuccess()) {
					LOG.log(Level.INFO, "The XML path has been submitted to Master.");
				}
				else {
					LOG.log(Level.INFO, "The XML path submits break down.");
				}
			}
			
		});
	}
}
