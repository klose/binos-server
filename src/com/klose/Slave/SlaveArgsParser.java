package com.klose.Slave;

import java.io.File;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import com.klose.Master.RegisterToMasterService;

public class SlaveArgsParser {
	private String masterIp = ""; 
	private int masterPort ; 
	private static int port = 6061;
	private static int httpServerPort = 36661;
	private static String ip_port = ""; //the ip of slave node
	private static String workDir = "/tmp"; 
	private int state = 0; // 
	private boolean necessaryArgsExist = false; //identify whether all the arguments exist.
	private static final Logger LOG = Logger.getLogger(SlaveArgsParser.class.getName());
	private String [] args_;
	
	SlaveArgsParser(String [] args){
		args_ = args;
	}
	
	public void printUsage() {
		System.out.print(
				"Usage: Slave" + " --url=MASTER_URL [--enable-order] [--port=PORT] [--httpport=PORT] [--workdir=DIR] [...] "+"\n"
				 + "MASTER_URL may be one of:" + "\n"
			       + "  JLoop://id@host:port" + "\n" 
			       + "  zoo://host1:port1,host2:port2,..." + "\n"
			       + "  zoofile://file where file contains a host:port pair per line"
			       + "\n"
			       + "Support options:\n"
			       + "    --help                   display this help and exit.\n" 
			       + "    --url=VAL                URL to represent Master URL\n"
			       + "    --port=VAL               port to listen on (default: 6061)\n"
			       + "    --workdir=VAL            DIR to store necessary data (default: /tmp)\n "		
			       + "    --enable-order           enable execute simple order in the node.\n"
			       + "    --httpport=VAL           port to http server listen on(default: 8081)\n"
		);
		System.exit(1);
	}
	public static String getIp_port() {
		return ip_port;
	}

	public void setIp_port(String ip_port) {
		this.ip_port = ip_port;
	}

	public  String getMasterIp() {
		return masterIp;
	}

	public  void setMasterIp(String masterIp) {
		this.masterIp = masterIp;
	}

	public  int getMasterPort() {
		return masterPort;
	}

	public  void setMasterPort(int masterPort) {
		this.masterPort = masterPort;
	}
	
	public static int getHttpServerPort() {
		return httpServerPort;
	}

	public static void setHttpServerPort(int httpServerPort) {
		SlaveArgsParser.httpServerPort = httpServerPort;
	}

	public void setPort(int port) {
		this.port =  port;
	}
	
	public int getPort() {
		return this.port;
	}
	
	public int getState() {
		return state;
	}

	public void setState(int state) {
		this.state = state;
	}

	public static String getWorkDir() {
		return workDir;
	}
	
	public void setWorkDir(String workDir) {
		this.workDir = workDir;
	}
	public void loadValue() throws UnknownHostException{
		if(this.args_.length < 1) {
			printUsage();
			System.exit(1);
		}
		else {
			String portRegex = "--port\\s*=\\s*[0-9]{4,5}";
			String httpPortRegex = "--httpport\\s*=\\s*[0-9]{4,5}";
			
			//urlRegex matches host name and ip address simultaneously
			String urlRegex = "--url\\s*=\\s*JLoop://[0-9]+@" +
					"([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}|[a-zA-Z0-9]*):[0-9]{4,5}";
			String workDirRegex = "--workdir\\s*=\\s*/[a-zA-Z0-9/ ]*";
			for(String tmp:args_) {
				//need jdk 1.5 or higher
				if(Pattern.matches(urlRegex, tmp.trim())) {
						String url = (tmp.trim().split("="))[1].trim();
						String url_1 = url.trim().split("@")[1];
						String host = (url_1.split(":"))[0].trim();
						if (host.matches("[a-zA-Z0-9]*")) {
							//match the host name.
							this.setMasterIp(InetAddress.getByName(host).getHostAddress());
						}
						else {
							this.setMasterIp(host);
						}
						this.setMasterIp( (url_1.split(":"))[0].trim());
						this.setMasterPort(Integer.parseInt((url_1.split(":"))[1].trim()));
						this.necessaryArgsExist = true;
				}
				else if(Pattern.matches(portRegex, tmp.trim())) {
					this.setPort(Integer.parseInt(
							(tmp.split("="))[1].trim()) );
				}
				else if(Pattern.matches(httpPortRegex, tmp.trim())) {
					this.setHttpServerPort( Integer.parseInt(
							tmp.split("=")[1].trim()) );
				}
				else if(Pattern.matches(workDirRegex, tmp.trim())) {
					String dirname = tmp.split("=")[1].trim();
					File dir = new File(dirname);
					if(! dir.isDirectory()) {
						System.out.println("Configuration error:  \'"+dirname+ "\' unrecognized\n");
						System.exit(1);
					}
					else {
						this.setWorkDir(dirname);
					}
				}
				else if (tmp.trim().equals("--enable-order")) {
					this.setState(SlaveState.SIMPLE_ORDER_EXEC);
				}
				else if (tmp.trim().equals("--help")) {
					printUsage();
				}
				else {
					System.out.println("Configuration error: option \'"+tmp+ "\' unrecognized\n");
					printUsage();
				}	
			}
			if(this.port == this.httpServerPort) {
				LOG.log(Level.SEVERE, "the port which RPC server listens on collides with the port which http server listens on!" );
				System.exit(1);
			}
			if(this.necessaryArgsExist) {
				this.setIp_port(Inet4Address.getLocalHost().getHostAddress() +":"+this.getPort());
			}
			else {
				LOG.log(Level.SEVERE, "'--url=VAL' doesn't exist.");
				printUsage();
			}
		}
	}
}
