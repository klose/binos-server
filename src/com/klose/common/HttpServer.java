package com.klose.common;

import java.net.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import org.apache.http.*;
import org.apache.http.protocol.*;

public class HttpServer {
	public static void main(String[] args){
		int port;
		ServerSocket server_socket;
		try{
			port = Integer.parseInt(args[0]);
		}
		catch(Exception e){
			port = 8080;
		}
		
		try{
			server_socket = new ServerSocket(port);
			System.out.println("httpServer running on port " + server_socket.getLocalPort());
			while(true){
				Socket socket = server_socket.accept();
				System.out.println("New connection accept " + socket.getInetAddress() + ":" + socket.getLocalPort());
				try{
					httpRequestHandler request = new httpRequestHandler(socket);
					Thread thread = new Thread(request);
					thread.start();
				}
				catch(Exception e){
					System.out.println(e);
				}
			}
		}
		catch(IOException e){
			System.out.println(e);
		}
	}
	static class httpRequestHandler implements Runnable{
		final static String CRLF = "\r\n";//"\r" ying hui che
		Socket socket;
		InputStream input;
		OutputStream output;
		BufferedReader br;
		public httpRequestHandler(Socket socket) throws Exception{
			this.socket = socket;
			this.input = socket.getInputStream();
			this.output = socket.getOutputStream();
			this.br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		}
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			try{
				processRequest();
			}
			catch(Exception e){
				System.out.println(e);
			}
		}
		private void processRequest() throws Exception{
			while(true){
				String headerLine = br.readLine();
				System.out.println("The client request is " + headerLine);
				if(headerLine.equals(CRLF) || headerLine.equals(""))
					break;
				StringTokenizer s = new StringTokenizer(headerLine);
				String temp = s.nextToken();
				if(temp.equals("GET")){
					String fileName = s.nextToken();
					System.out.println(fileName);
					FileInputStream fis = null;
					boolean fileExists = true;
					try{
						fis = new FileInputStream(fileName);
					}
					catch(FileNotFoundException e){
						fileExists = false;
					}
					String serverLine = "Server: a simple java httpServer" + CRLF;
					String statusLine = null;
					String contentTypeLine = null;
					String entityBody = null;
					String contentLengthLine = "error";
					if(fileExists){
						statusLine = "HTTP/1.0 200 OK" + CRLF;
						contentTypeLine = "Content-type: " + contentType(fileName)+CRLF;
						contentLengthLine = "Content-Length: " + (new Integer(fis.available())).toString()+CRLF;						
					}
					else
					{
						statusLine = "HTTP/1.0 404 Not Found" + CRLF;
						contentTypeLine = "text/html" + CRLF;
						contentLengthLine = "0" + CRLF;
						entityBody = "HTML";
					}
					output.write(statusLine.getBytes());
					output.write(serverLine.getBytes());
					output.write(contentTypeLine.getBytes());
					output.write(contentLengthLine.getBytes());
					output.write(CRLF.getBytes());
					if(fileExists){
						sendBytes(fis,output);
						fis.close();
					}
					else{
						output.write(entityBody.getBytes());
					}
				}
			}
			try{
				output.close();
				br.close();
				socket.close();
			}
			catch(Exception e){}
		}
	}
		private static void sendBytes(FileInputStream fis, OutputStream os) throws Exception{
			byte[] buffer = new byte[1024];
			int bytes = 0;
			os.write(("Now is the file Contents: "+ "\r\n").getBytes());
			while((bytes = fis.read(buffer)) != -1){
				os.write(buffer,0,bytes);
			}
		}
		private static String contentType(String fileName){
			if(fileName.endsWith(".htm")|| fileName.endsWith(".html"))
			{
				return "text/html";
			}
			return fileName;
		}
		
	
	
}