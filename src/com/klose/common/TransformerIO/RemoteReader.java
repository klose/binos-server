package com.klose.common.TransformerIO;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;

import com.klose.Slave.SlaveArgsParser;
import com.klose.common.HttpClient;

/**
 * RemoteReader: read the file from remote machine.
 * There are two different ways to reading the remote file:
 * 1) save the remote file to local file, and read the local file.
 * 2) retrieve the input Stream from the remote machine.
 * It can be set as to application's requirements. 
 * @author Bing Jiang
 */
public class RemoteReader  implements VertexReader, Serializable{
	private HttpClient readerClient = null;
	private static int NetworkOrCopyFile = 0; // It will choose the network stream as dafault.
	private String remoteFilePath;
	private LocalReader localReader;
	//private InputStream is = null;
	private BufferedReader br = null;
	public RemoteReader(String inputPath) {
		String [] tmp = inputPath.trim().split(":");
		readerClient = new HttpClient(tmp[0], Integer.parseInt(tmp[1]));
		remoteFilePath  = tmp[2];
		initialize();
	}
	public static void setNetworkOrCopyFile(int  networkOrCopyFile) {
		NetworkOrCopyFile = networkOrCopyFile;
	}
	private void initialize() {
		if(NetworkOrCopyFile == 0) {
			this.br = readerClient.getResponseBufferedStream(this.remoteFilePath);
		}
		else {
			String localFilePath = readerClient.transFileToDataDir(this.remoteFilePath,
					SlaveArgsParser.getWorkDir());
			try {
				localReader = new LocalReader(localFilePath);
				
				this.br = new BufferedReader(new InputStreamReader(localReader.getInputStream()));

			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	@Override
	public int read() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	public BufferedReader getBufferedReaderStream() throws IOException {
		// TODO Auto-generated method stub
		return this.br;
	}
	@Override
	public int read(char[] cbuf, int offset, int length) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	public int read(byte[] buf, int offset, int length) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	public String readline() throws IOException {
		// TODO Auto-generated method stub
		if(br == null){
			return null;
		}
		else {
			return br.readLine();
		}
	}
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		br.close();
		
	}
	@Override
	public InputStream getInputStream() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	
}
