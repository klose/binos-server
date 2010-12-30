package com.klose.common;

import java.io.*;
import java.net.*;

import javax.net.*;
import javax.net.ssl.*;

import org.apache.commons.logging.Log;
import org.apache.http.protocol.HttpRequestExecutor;

import java.security.cert.*;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
public class HttpClient {
	private static int BUFFER_SIZE = 8096;//buffer size
	static Socket socket = null;
	//connection
	private String server_ip;
	private int server_port;
	private static final Logger LOG = Logger.getLogger(HttpClient.class.getName());
	public HttpClient(String ip, int port) {
		this.server_ip = ip;
		this.server_port = port;
	}
	
	public void connect() throws Exception{
		socket = new Socket(this.server_ip, this.server_port);
	}
	public void disconnect(Socket socket) throws Exception{
		socket.close();
	}
	/**
	 * request remote machine to get the file, and put the file into the specified path. 
	 * @param requestFilePath : remote file path
	 * @param DirPath : local directory path which is used to store the file duplicated.
	 * @return 
	 */
	public boolean transFileToDataDir(String requestFilePath, String DirPath) {
		try {
			long startTime = System.currentTimeMillis();
			sendRequestFile(requestFilePath);
			String [] tmp  = requestFilePath.trim().split("/");
			String path = DirPath.trim();
			if( !path.endsWith("/") ) {
				path += "/";
			}
			path += tmp[tmp.length -1];
			getResponseFile(path);
			LOG.log(Level.INFO, "Copy file from "+ this.server_ip + ":"
					+this.server_port + requestFilePath + " to local file "
					+ path + " use " + (System.currentTimeMillis() - startTime) + "ms");
			return true;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.log(Level.WARNING, e.toString());
			return false;
		}
		
	}
	//send request command
	private void sendRequestFile(String uri) throws Exception {
		OutputStream os = socket.getOutputStream();
		os.write(("GET "+uri+" HTTP/1.1\n").getBytes());
	}
	
	private void getResponseFile(String filePath) throws Exception{
		InputStream is = socket.getInputStream();
		
		while(is.available() == 0){
			Thread.sleep(10);
		}
		System.out.println("receive successfully");
		BufferedReader br = new BufferedReader(new InputStreamReader(is,"utf-8"));
		
		saveToFile(br, filePath);
		
	}
	
	// save the file from remote node posted by http
	private void saveToFile(BufferedReader br, String fileName) throws IOException{
		FileOutputStream fos = null;
		BufferedInputStream bis = null;		
		byte[] buf = new byte[BUFFER_SIZE];
		int size= 0;
		File file = new File(fileName);
		if(!file.exists()) {
			file.createNewFile();
		}
		// create the file for saving
		fos = new FileOutputStream(file);

	
		String list = br.readLine();
		while(!list.startsWith("Now is the file Contents: ")){
			System.out.println(list);
			list = br.readLine();
		}
		System.out.println(list);
		list = br.readLine();
		System.out.println(list);
		// save the file
		while(list != null){
			fos.write(list.getBytes());
			list = br.readLine();
		}
		fos.close();
		br.close();
		
	} 
	
	
	//just for test
	public static void main(String[] args){
		HttpClient hc = new HttpClient("localhost", 8081);
		try {
			long start =  System.currentTimeMillis();
			hc.connect();
			hc.sendRequestFile("/tmp/input");
			hc.getResponseFile("/tmp/output");
			System.out.println("sssss");
			hc.disconnect(socket);
			System.out.println("Used total used time:" + (System.currentTimeMillis() - start) + " ms");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
