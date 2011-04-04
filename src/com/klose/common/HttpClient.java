package com.klose.common;

import java.io.*;
import java.net.*;

import javax.net.*;
import javax.net.ssl.*;

import org.apache.commons.logging.Log;
import org.apache.http.protocol.HttpRequestExecutor;

import com.klose.common.TransformerIO.FileUtililty;

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
	
	private void connect() throws Exception{
		if(socket == null) {
			socket = new Socket(this.server_ip, this.server_port);
		}
	}
	private void disconnect() throws Exception{
		if(!socket.isClosed()) {
			socket.close();
		}
	}
	/**
	 * request remote machine to get the file, and put the file into the specified path. 
	 * @param requestFilePath : remote file path
	 * @param DirPath : local directory path which is used to store the file duplicated.
	 * @return the new path of local file if operates successfully, null if failure.
	 */
	public String transFileToDataDir(String requestFilePath, String dirPath) {
		try {
			connect();
			long startTime = System.currentTimeMillis();
			sendRequestFile(requestFilePath);
			String [] tmp  = requestFilePath.trim().split("/");
			String path = dirPath.trim();
			if( !path.endsWith("/") ) {
				path += "/";
			}
			if(!FileUtililty.mkdirLocalDir(dirPath)) {
				LOG.log(Level.WARNING, "Can't mkdir "+ dirPath);
				return null;
			}
			path += tmp[tmp.length -1];
			if(getResponseFile(path)) {
				LOG.log(Level.INFO, "Copy file from "+ this.server_ip + ":"
						+this.server_port + requestFilePath + " to local file "
						+ path + " use " + (System.currentTimeMillis() - startTime) + "ms");
			}
			else {
				LOG.log(Level.WARNING, "Error occurs when copying file from "+ this.server_ip + ":"
						+this.server_port + requestFilePath + " to local file "
						+ path);
			}
			disconnect();
			return path;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.log(Level.WARNING, e.toString());
			try {
				disconnect();
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			return null;
		} 
		
	}
	//send request command
	private void sendRequestFile(String uri) throws Exception {
		OutputStream os = socket.getOutputStream();
		os.write(("GET "+uri+" HTTP/1.1\n").getBytes());
	}
	/**
	 * retrieve the input stream from the remote machine 
	 * as specified to the remote node's address which request path contains.
	 * . 
	 * @return
	 */
	public BufferedReader getResponseBufferedStream(String uri) {
		try {
			connect();
			sendRequestFile(uri);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			LOG.log(Level.WARNING, e1.getMessage());
			e1.printStackTrace();
		}
		InputStream is;
		try {
			is = socket.getInputStream();
			while(is.available() == 0) {
				Thread.sleep(10);
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(is, "utf-8"));
			String notFoundStatus = "HTTP/1.0 404 Not Found";
			String list = br.readLine();
			while(!list.startsWith("Now is the file Contents: ")){
				System.out.println(list);
				if(list.equals(notFoundStatus)) {
					LOG.log(Level.WARNING, "File NOT FOUND.HTTP/1.0 404");
					return null;
				}
				list = br.readLine();
			}
			LOG.log(Level.INFO, "Getting the input stream.");
			return br;
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOG.log(Level.WARNING, e.getMessage());
			return null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOG.log(Level.WARNING, e.getMessage());
			return null;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOG.log(Level.WARNING, e.getMessage());
			return null;
		}
		
	}
	
	private boolean getResponseFile(String filePath) throws Exception{
		InputStream is = socket.getInputStream();
		
		while(is.available() == 0){
			Thread.sleep(10);
		}
		BufferedReader br = new BufferedReader(new InputStreamReader(is,"utf-8"));
		
		return (saveToFile(br, filePath));
	}
	
	// save the file from remote node passed by http
	private boolean saveToFile(BufferedReader br, String fileName) throws IOException{
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
		String notFoundStatus = "HTTP/1.0 404 Not Found" + "\r\n";
		String list = br.readLine();
		while(!list.startsWith("Now is the file Contents: ")){
			if(list.equals(notFoundStatus)) {
				LOG.log(Level.WARNING, "File NOT FOUND.HTTP/1.0 404");
				return false;
			}
			System.out.println(list);
			list = br.readLine();
		}
		//read the first line from the remote file.
		list = br.readLine();
		// save the file
		while(list != null){
			fos.write(list.getBytes());
			list = br.readLine();
		}
		fos.close();
		br.close();
		return true;
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
			hc.disconnect();
			System.out.println("Used total used time:" + (System.currentTimeMillis() - start) + " ms");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
