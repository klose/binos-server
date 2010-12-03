package com.klose.Slave;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class SlaveOrderExecutor {
	private String order ;
	private String stdout = "";
	private String stderr = ""; 
	private int exitValue = Integer.MAX_VALUE;
	private String fileName = "";
	
	public SlaveOrderExecutor(String order) {
		this.order = order;
	}
	public String getOrder() {
		return order;
	}
	
	public void setOrder(String order) {
		this.order = order;
	}
	
	public String getStdout() {
		return stdout;
	}
	public void setStdout(String stdout) {
		this.stdout = stdout;
	}
	public String getStderr() {
		return stderr;
	}
	public void setStderr(String stderr) {
		this.stderr = stderr;
	}
	
	public void setExitValue(int exitValue) {
		this.exitValue = exitValue;
	}
	public int getExitValue() {
		return exitValue;
	}
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	/*
	 * Put the exeutable order to a bash script file, and construct bash environment.
	 * */
	public void execute() throws IOException {
		String fileName = "/tmp/" + System.currentTimeMillis() + ".sh";
		
		File file = new File(fileName);
		if(!file.exists()) {
			file.createNewFile();
			file.setExecutable(true);
			FileWriter fw = new FileWriter(file);
			fw.write("#!/bin/sh\n");
			fw.write(this.getOrder());
			fw.flush();
		}
		
		ProcessBuilder builder = new ProcessBuilder("/bin/sh", fileName);
		Process process = builder.start();
		InputStream is = process.getInputStream();
		InputStream es = process.getErrorStream();
		InputStreamReader isr = new InputStreamReader(is, "GBK");
		InputStreamReader esr = new InputStreamReader(es, "GBK");
		BufferedReader ibr = new BufferedReader(isr);
		BufferedReader ebr = new BufferedReader(esr);
		String line;
		while ((line = ibr.readLine()) != null) {
			this.stdout += line + "\n";
		}
		while ((line = ebr.readLine()) != null) {
			this.stderr += line + "\n";
		}
		this.setExitValue(process.exitValue());
	}
	/**
	 * this static main function is used for testing.
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		SlaveOrderExecutor a = new SlaveOrderExecutor("echo \"jiangbing\" > /etc/passwd");
		a.execute();
		System.out.println("exitValue:" + a.getExitValue() );
		System.out.println(a.getStdout());
		System.out.println("ERROR:"+ a.getStderr());
	}
}
