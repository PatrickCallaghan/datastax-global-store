package com.datastax.timeseries.model;

import javax.xml.bind.annotation.XmlRootElement;

import com.datastax.driver.core.ConsistencyLevel;

@XmlRootElement()
public class ObjectData {
	private String key;
	private String value;
	private int ttl;
	private long writeTime;
	private String cl; 
	
	public ObjectData(){}
	
	public ObjectData(String key, String value) {
		this(key, value, 0, ConsistencyLevel.ONE.toString());
	}

	public ObjectData(String key, String value, int ttl, long writeTime) {
		this(key, value, ttl, writeTime, ConsistencyLevel.ONE);
	}
	
	public ObjectData(String key, String value, int ttl, String cl) {
		super();
		this.key = key;
		this.value = value;
		this.ttl = ttl;
		this.cl = cl;
	}

	public ObjectData(String key, String value, int ttl, long writeTime, ConsistencyLevel cl) {
		super();
		this.key = key;
		this.value = value;
		this.ttl = ttl;
		this.writeTime = writeTime;
		this.cl = cl.name();
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public int getTtl() {
		return ttl;
	}

	public void setTtl(int ttl) {
		this.ttl = ttl;
	}

	public String getCl() {
		return cl;
	}

	public void setCl(String cl) {
		this.cl = cl;
	}

	public long getWriteTime() {
		return writeTime;
	}

	public void setWriteTime(long writeTime) {
		this.writeTime = writeTime;
	}
	
	@Override
	public String toString() {
		return "ObjectData [key=" + key + ", value=" + value + ", ttl=" + ttl + ", cl=" + cl + ", writetime=" + writeTime + "]";
	}

	public boolean isEmpty() {
		return key == null;
	}
}
