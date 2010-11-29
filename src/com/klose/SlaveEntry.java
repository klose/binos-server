package com.klose;

import java.sql.Time;

/**
 * @author jiangbing
 *
 */
public class SlaveEntry {
	private String ip;
	private String port;
	private int state;
	private String info;
	private String login_time = "";
	private String exit_time = "";
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getPort() {
		return port;
	}
	public void setPort(String port) {
		this.port = port;
	}
	public int getState() {
		return state;
	}
	public void setState(int state) {
		this.state = state;
	}
	public String getInfo() {
		return info;
	}
	public void setInfo(String info) {
		this.info = info;
	}
	public String getLogin_time() {
		return login_time;
	}
	public void setLogin_time(String login_time) {
		this.login_time = login_time;
	}
	public void setLoginTime() {
		Time loginTime = new Time(System.currentTimeMillis());
		setLogin_time(loginTime.toGMTString());
	}
	public String getExit_time() {
		return exit_time;
	}
	public void setExit_time(String exit_time) {
		this.exit_time = exit_time;
	}
	public void setExitTime() {
		Time exitTime  = new Time(System.currentTimeMillis());
		setExit_time(exitTime.toGMTString());
	}
	
}
