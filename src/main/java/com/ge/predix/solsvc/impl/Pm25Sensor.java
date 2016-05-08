package com.ge.predix.solsvc.impl;

public class Pm25Sensor {
	private String city;
	private int measure;
	private String pointID;
	private String pointName;
	private String updateTime;
	private String xValue;
	private String yValue;

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public int getMeasure() {
		return measure;
	}

	public void setMeasure(int measure) {
		this.measure = measure;
	}

	public String getPointID() {
		return pointID;
	}

	public void setPointID(String pointID) {
		this.pointID = pointID;
	}

	public String getPointName() {
		return pointName;
	}

	public void setPointName(String pointName) {
		this.pointName = pointName;
	}

	public String getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(String updateTime) {
		this.updateTime = updateTime;
	}

	public String getxValue() {
		return xValue;
	}

	public void setxValue(String xValue) {
		this.xValue = xValue;
	}

	public String getyValue() {
		return yValue;
	}

	public void setyValue(String yValue) {
		this.yValue = yValue;
	}

}
