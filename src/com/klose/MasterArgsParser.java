package com.klose;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.regex.Pattern;

public class MasterArgsParser {
	private static int port = 6060; //default value is set by 6060.
	private static String URL= ""; // support zookeeper
	private String [] args_;
	
	MasterArgsParser(String [] args){
		args_ = args;
	}
	
	public void printUsage() {
		System.out.print(
				"Usage: " + " [--port=PORT] [--url=URL] [...] "+"\n"
				+ "\n"
				+ "URL (used for leader election with ZooKeeper) may be one of:" + "\n"
			       + "  zoo://host1:port1,host2:port2,..." + "\n"
			       + "  zoofile://file where file has one host:port pair per line" + "\n"
			       + "\n"
			       
			       + "Support options:\n"
			       + "    --help                   display this help and exit.\n" 
			       + "    --port=VAL               port to listen on (default: 6060)\n"
			       + "    --url=VAL                URL used for leader election\n"
		);
		System.exit(1);
	}
	
	public void setPort(int port) {
		this.port =  port;
	}
	
	public int getPort() {
		return this.port;
	}
	public void setURL(String url) {
		this.URL = url;
	}
	public String getURL() {
		return this.URL;
	}
	/**
	 * 
	 * @return the identity of Master by "JLoop://id@***.***.***.***:port"
	 * id refers to different master when there are standby master.
	 * @throws UnknownHostException 
	 */
	public String constructIdentity() throws UnknownHostException{
		return new String ("JLoop://" + 0 + "@"
				+ Inet4Address.getLocalHost().getHostAddress()
				+ ":" +this.port); 
	}
	
	public void loadValue(){
		if(this.args_.length > 2) {
			printUsage();
			System.exit(1);
		}
		else {
			String portRegex= "--port\\s*=\\s*[0-9]{4,5}";
			String urlRegex = "--url\\s*=\\s*[a-zA-Z0-9/:,]*";
			for(String tmp:args_) {
				//need jdk 1.5 or higher
				if(Pattern.matches(portRegex, tmp.trim())) {
						this.setPort(Integer.parseInt(
								(tmp.split("="))[1].trim()) );
				}
				else if(Pattern.matches(urlRegex, tmp.trim())) {
						this.setURL( (tmp.split("="))[1].trim() );
				}
				else if (tmp.trim().equals("--help")) {
					printUsage();
				}
				else {
					System.out.println("Configuration error: option \'"+tmp+ "\' unrecognized\n");
					printUsage();
				}	
			}
		}
	}
	
}
