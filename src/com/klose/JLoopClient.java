package com.klose;

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import com.google.protobuf.RpcCallback;
import com.googlecode.protobuf.socketrpc.SocketRpcChannel;
import com.googlecode.protobuf.socketrpc.SocketRpcController;
import com.klose.MsConnProto.ConfirmMessage;
import com.klose.MsConnProto.ExecOrder;
import com.klose.MsConnProto.ExecOrderResp;
import com.klose.MsConnProto.SlaveOrderExecutorService;
import com.klose.MsConnProto.TransformXMLPath;
import com.klose.MsConnProto.XMLPathTransService;
import com.klose.common.TransformerIO.FileUtil;
import com.klose.common.TransformerIO.FileUtil.FStype;

/**
 * JLoopClient: provide the API for submitting job to Master. JLoopClient is not
 * necessarily the module of the Slave daemon.
 * 
 * @author Bing Jiang
 * 
 */
public class JLoopClient {
	private String[] args_;
	private HashMap<String, String> argsMap = new HashMap<String, String>();
	private static final Logger LOG = Logger.getLogger(JLoopClient.class.getName());
	public JLoopClient(String[] args) {
		this.args_ = args;
	}

	private void printUsage() {
		System.out
				.print("Usage: JLoopClient"
						+ " --url=MASTER_URL --xml=PATH [--exec=ORDER] [...] "
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
						+ "    --xml=VAL                XML specifies the detail of task to be submitted\n");
		System.exit(1);
	}

	/*
	 * parserArgs: parse the args and load information into argsMap(HashMap). if
	 * args has invalidate value, it will return false.
	 */
	private void parseArgs() {
		// premise: the master url is like JLoop://id@host:port
		if (args_.length <= 1) {
			printUsage();
		}
		int length = this.args_.length;
		String urlRegex = "--url\\s*=\\s*JLoop://[0-9]+@"
				+ "[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}:[0-9]{4,5}";
		String execRegex = "--exec=";
		String xmlRegex = "--xml=";
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
				else if (Pattern.matches(xmlRegex,
						this.args_[i].trim().substring(0, 6))) {
					String path = "";
					if (this.args_[i].trim().length() > 6) {
						path = this.args_[i].trim().substring(6).trim();
						this.argsMap.put("exec-xml", path);
					} else {
						printUsage();
					}
				}
			} else if (this.args_[i].trim().length() >= 6) {
				if (Pattern.matches(xmlRegex,
						this.args_[i].trim().substring(0, 6))) {
					String path = "";
					if (this.args_[i].trim().length() > 6) {
						path = this.args_[i].trim().substring(6).trim();
						this.argsMap.put("exec-xml", path);
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
		String newPath = normalizingUniformTaskPath(this.argsMap.get("exec-xml"));
		if(newPath == null) {
			System.exit(-1);
		}
		else {
			this.argsMap.put("exec-xml", newPath);
		}
	}
	
	/**
	 * Normalize the path which is not provied by hdfs into file in the hdfs.
	 *  In the function, there are sometimes operations copying file from one file system to
	 *  hdfs.
	 * @param path: It is a absolute path in file system supported.
	 * @return the normalized hdfs absolute file path
	 */
	public static String normalizingUniformTaskPath(String path) {
		FStype type = FileUtil.getFileType(path);
		if(type == FStype.LOCAL) {
			String[] tmp = path.split("/");
			String filename = tmp[tmp.length -1];
			String generatedPath = FileUtil.TransLocalFileToHDFS(path, "task");
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
			if( FileUtil.checkFileValid(path) ) {
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
							.setPath(client.findKeyInMap("exec-xml")).build();
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
//		SlaveOrderExecutorService executeService = SlaveOrderExecutorService.newStub(socketRpcChannel);
//		ExecOrder requestOrder = ExecOrder.newBuilder().setOrder(client.findKeyInMap("exec-order")).build();
//		executeService.executeOrder(rpcController, requestOrder, new RpcCallback<com.klose.MsConnProto.ExecOrderResp>(){
//
//			@Override
//			public void run(ExecOrderResp resp) {
//				// TODO Auto-generated method stub
//				if(resp.getIsExecuted()) {
//					System.out.println("Execute Successfully!");
//				}
//				else {
//					System.out.println("Execute failure!");
//				}
//				System.out.println(resp.getResultMessage());
//				
//			}	
//		});
	}
}
