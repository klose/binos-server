package com.klose;

import java.util.regex.Pattern;

public class SlaveArgsParser {
	private  String masterIp = ""; 
	private  int masterPort ; 
	private static int port = 12132; //default value is set by 12132.
	private String [] args_;
	
	SlaveArgsParser(String [] args){
		args_ = args;
	}
	
	public void printUsage() {
		System.out.print(
				"Usage: Slave" + " --url=MASTER_URL [--port=PORT] [...] "+"\n"
				+"if port is not set, Master will allocate a port number for slave use.\n"
				);
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
	public void setPort(int port) {
		this.port =  port;
	}
	
	public int getPort() {
		return this.port;
	}
	
	public void loadValue(){
		if(this.args_.length < 1) {
			printUsage();
			System.exit(1);
		}
		else {
			String portRegex= "--port\\s*=\\s*[0-9]{4,5}";
			String urlRegex = "--url\\s*=\\s*JLoop://[0-9]+@" +
					"[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}:[0-9]{4,5}";
			for(String tmp:args_) {
				//need jdk 1.5 or higher
				if(Pattern.matches(urlRegex, tmp.trim())) {
						String url = (tmp.trim().split("="))[1].trim();
						String url_1 = url.trim().split("@")[1];
						this.setMasterIp( (url_1.split(":"))[0].trim());
						this.setMasterPort(Integer.parseInt((url_1.split(":"))[1].trim()));
				}
				else if(Pattern.matches(portRegex, tmp.trim())) {
					this.setPort(Integer.parseInt(
							(tmp.split("="))[1].trim()) );
				}
				else {
					printUsage();
					System.exit(1);
				}	
			}
		}
	}
}
