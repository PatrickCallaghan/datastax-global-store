package com.datastax.demo.service;

import java.io.IOException;

import org.joda.time.DateTime;

import com.datastax.global.dao.GlobalStoreDAO;
import com.datastax.timeseries.model.DataPoints;
import com.datastax.timeseries.model.ObjectData;
import com.datastax.timeseries.model.Periodicity;
import com.datastax.timeseries.model.TimeSeries;
import com.datastax.timeseries.utils.PeriodicityProcessor;

public class GlobalStoreService {

 	private GlobalStoreDAO globalStoreDAO = new GlobalStoreDAO(new String[]{"127.0.0.1"}); 

	public GlobalStoreService() {
		
	}
	
	//Cache
	public void putObjectData(ObjectData objectData){
		try {
			this.globalStoreDAO.putObjectInStore(objectData.getKey(), objectData.getValue());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public ObjectData getObjectData(String key){
		try {
			return this.globalStoreDAO.getObjectFromStore(key);
		} catch (Exception e) {
			e.printStackTrace();
			return new ObjectData();
		}
	}

	//Time Series	
	public void insertClusterTimeSeries(TimeSeries timeSeries) {
		this.globalStoreDAO.insertClusterTimesSeries(timeSeries);
	}

	public TimeSeries getTimeSeries(String name, DateTime start, DateTime end, int limit) {

		if (limit == 0)
			limit = 100000000;

		return this.globalStoreDAO.getTimeSeries(name, start != null ? start.getMillis() : 0,
				end != null ? end.getMillis() : Long.MAX_VALUE, limit);
	}

	public TimeSeries getTimeSeries(String name, DateTime start, DateTime end, Periodicity periodicity) {

		TimeSeries timeSeries = this.getTimeSeries(name, start, end, 0);
		if (periodicity != null) {
			return PeriodicityProcessor.getTimeSeriesByPeriod(timeSeries, periodicity, start);
		}

		return timeSeries;
	}

	public TimeSeries getTimeSeries(String name, long start, long end, Periodicity periodicity) {

		if (end==0) end=Long.MAX_VALUE;
		
		return this.getTimeSeries(name, 
				new DateTime().withMillis(start), 
				new DateTime().withMillis(end), 
				periodicity);
	}

	
	public TimeSeries getTimeSeries(String name, Periodicity periodicity) {
		return this.getTimeSeries(name, 0, 0, periodicity);
	}

	
	//Full Time Series
	public void insertFullTimeSeries(TimeSeries timeSeries) {
		try {
			this.globalStoreDAO.insertTimeSeriesFull(timeSeries);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public TimeSeries getTimeSeries(String name) {

		return this.globalStoreDAO.getTimeSeriesFull(name);
	}

	//Data Points
	public DataPoints getDataPoints(String key) {
		return this.globalStoreDAO.getDataPoints(key);
	}

	public void putDataPoints(DataPoints dataPoints) {
	
		this.globalStoreDAO.insertDataPoints(dataPoints);
	}

}
