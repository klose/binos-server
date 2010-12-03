package com.klose.Master;

public class ExecResult {
	private boolean execSucceed;
	private String response;
	ExecResult(boolean execSucceed, String resp) {
		this.execSucceed = execSucceed;
		this.response = resp;
	}
	public boolean isExecSucceed() {
		return execSucceed;
	}
	public void setExecSucceed(boolean execSucceed) {
		this.execSucceed = execSucceed;
	}
	public String getResponse() {
		return response;
	}
	public void setResponse(String response) {
		this.response = response;
	}
	
}
