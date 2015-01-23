package com.datastax.timeseries.model;

public class DataPoints{
	
	private String[] names;
	private Double[] values;
	private String key;
	
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
}
