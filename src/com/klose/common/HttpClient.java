package com.klose.common;

import java.io.*;
import java.net.*;

import javax.net.*;
import javax.net.ssl.*;

import org.apache.http.protocol.HttpRequestExecutor;

import java.security.cert.*;
import java.util.Vector;
public class HttpClient {
	private static int BUFFER_SIZE = 8096;//buffer size
	public final static boolean DEBUG = true; //for debugging
	static Socket socket = null;
	//connection
	private void connect(String serverName, int port) throws Exception{
		socket = new Socket(serverName, port);
	}
	private void disconnect(Socket socket) throws Exception{
		socket.close();
	}
	//send request command
	private void sendRequestFile(String uri) throws Exception {
		OutputStream os = socket.getOutputStream();
		os.write(("GET "+uri+" HTTP/1.1\n").getBytes());
	}
	
	private void getResponseFile(String fileName) throws Exception{
		InputStream is = socket.getInputStream();
		
		while(is.available() == 0){
			Thread.sleep(10);
			
		}
		System.out.println("receive successfully");
		BufferedReader br = new BufferedReader(new InputStreamReader(is,"utf-8"));
		
		saveToFile(br, fileName);
		
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
		
		if(this.DEBUG)
			System.out.println("getting connection and save is as ["+fileName +"]");
		
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
	
	
	
	public static void main(String[] args){
		HttpClient hc = new HttpClient();
		try {
			long start =  System.currentTimeMillis();
			hc.connect("localhost", 8080);
			hc.sendRequestFile("/tmp/input");
			hc.getResponseFile("/tmp/ttt");
			hc.disconnect(socket);
			System.out.println("Used total used time:" + (System.currentTimeMillis() - start) + " ms");
			
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
