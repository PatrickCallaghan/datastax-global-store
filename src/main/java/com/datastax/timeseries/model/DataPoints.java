package com.datastax.timeseries.model;

import java.util.Arrays;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement()
public class DataPoints{
	
	private String[] names;
	private Double[] values;
	private String key;
	
	public DataPoints(){
		
	}
	
	public DataPoints(String key, String[] names, Double[] doubles) {
		this.key = key;
		this.names = names;
		this.values = doubles;
	}

	public String[] getNames() {
		return names;
	}

	public Double[] getValues() {
		return values;
	}

	public String getKey() {
		return key;
	}

	public void setNames(String[] names) {
		this.names = names;
	}

	public void setValues(Double[] values) {
		this.values = values;
	}

	public void setKey(String key) {
		this.key = key;
	}

	@Override
	public String toString() {
		return "DataPoints [names=" + Arrays.toString(names) + ", values=" + Arrays.toString(values) + ", key=" + key
				+ "]";
	}
	
	
}
