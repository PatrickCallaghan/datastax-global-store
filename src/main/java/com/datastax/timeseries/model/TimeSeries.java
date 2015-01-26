package com.datastax.timeseries.model;

import java.util.Arrays;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang.ArrayUtils;
import org.joda.time.DateTime;

@XmlRootElement()
public class TimeSeries {

	private String key;
	private long[] dates;
	private double[] values;
	
	public TimeSeries(){}
	
	public TimeSeries(String key, long date, double value) {
		super();
		this.key = key;
		this.dates = new long[]{date};
		this.values = new double[]{value};
	}
	
	public TimeSeries(String key, long[] dates, double[] values) {
		super();
		this.key = key;
		this.dates = dates;
		this.values = values;
	}

	public String getKey() {
		return key;
	}

	public long[] getDates() {
		return dates;
	}

	public double[] getValues() {
		return values;
	}
	
	public void setKey(String key) {
		this.key = key;
	}

	public void setDates(long[] dates) {
		this.dates = dates;
	}

	public void setValues(double[] values) {
		this.values = values;
	}

	public void reverse(){
		ArrayUtils.reverse(dates);
		ArrayUtils.reverse(values);
	}

	@Override
	public String toString() {
		return "TimeSeries [symbol=" + key + ", dates=" + Arrays.toString(dates) + ", values="
				+ Arrays.toString(values) + "]";
	}
	public String toFormatterString() {
		StringBuffer buffer = new StringBuffer();
		
		buffer.append("TimeSeries - symbol=" + key + "\n");
		
		for (int i=0; i < dates.length; i++){
			buffer.append(new DateTime(dates[i]).toString() + " - " + values[i] + "\n");
		}
			
		return buffer.toString(); 	
	}	
	
}
