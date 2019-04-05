package com.datastax.demo.service;

import java.io.IOException;

import org.joda.time.DateTime;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.global.dao.GlobalStoreDAO;
import com.datastax.shaded.jackson.databind.deser.impl.ObjectIdValueProperty;
import com.datastax.timeseries.model.DataPoints;
import com.datastax.timeseries.model.ObjectData;
import com.datastax.timeseries.model.Periodicity;
import com.datastax.timeseries.model.TimeSeries;
import com.datastax.timeseries.utils.PeriodicityProcessor;

public class GlobalStoreService {

	private final String WRITE_OBJECT_STORE = "WRITE_OBJECT_STORE";
	private final String WRITE_TIME_SERIES = "WRITE_TIME_SERIES";
	private final String WRITE_DATA_POINTS = "WRITE_DATA_POINTS";

	private final String READ_OBJECT_STORE = "READ_OBJECT_STORE";
	private final String READ_TIME_SERIES = "READ_TIME_SERIES";
	private final String READ_DATA_POINTS = "READ_DATA_POINTS";

	private GlobalStoreDAO globalStoreDAO = new GlobalStoreDAO();	
	private static GlobalStoreService service = new GlobalStoreService();

	public static GlobalStoreService getInstance() {
		return service;
	}

	public GlobalStoreService() {
		
	}
	
	//Cache
	public void putObjectData(ObjectData objectData){
		try {
			this.globalStoreDAO.addServiceUsage(objectData.getKey(), WRITE_OBJECT_STORE);	
			
			ConsistencyLevel cl = objectData.getCl() == null ? ConsistencyLevel.ONE : ConsistencyLevel.valueOf(objectData.getCl());
			
			this.globalStoreDAO.putObjectInStore(objectData.getKey(), objectData.getValue(), cl, objectData.getTtl());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public ObjectData getObjectData(String key, ConsistencyLevel consistencyLevel){
		try {
			this.globalStoreDAO.addServiceUsage(key, READ_OBJECT_STORE);
			return this.globalStoreDAO.getObjectFromStore(key, consistencyLevel);
		} catch (Exception e) {
			e.printStackTrace();
			return new ObjectData();
		}
	}

	//Time Series	
	public void insertClusterTimeSeries(TimeSeries timeSeries) {
		this.globalStoreDAO.addServiceUsage(timeSeries.getKey(), WRITE_TIME_SERIES);
		this.globalStoreDAO.insertClusterTimesSeries(timeSeries);
	}

	public TimeSeries getTimeSeries(String name, DateTime start, DateTime end, int limit) {

		if (limit == 0)
			limit = 100000000;

		return this.globalStoreDAO.getTimeSeries(name, start != null ? start.getMillis() : 0,
				end != null ? end.getMillis() : Long.MAX_VALUE, limit);
	}

	public TimeSeries getTimeSeries(String key, DateTime start, DateTime end, Periodicity periodicity) {

		this.globalStoreDAO.addServiceUsage(key, READ_TIME_SERIES);
		
		TimeSeries timeSeries = this.getTimeSeries(key, start, end, 0);
		if (periodicity != null) {
			return PeriodicityProcessor.getTimeSeriesByPeriod(timeSeries, periodicity, start);
		}

		return timeSeries;
	}

	public TimeSeries getTimeSeries(String key, long start, long end, Periodicity periodicity) {

		if (end==0) end=Long.MAX_VALUE;
		
		return this.getTimeSeries(key, 
				new DateTime().withMillis(start), 
				new DateTime().withMillis(end), 
				periodicity);
	}

	
	public TimeSeries getTimeSeries(String key, Periodicity periodicity) {
		return this.getTimeSeries(key, 0, 0, periodicity);
	}

	
	//Full Time Series
	public void insertFullTimeSeries(TimeSeries timeSeries) {
		try {
			this.globalStoreDAO.addServiceUsage(timeSeries.getKey(), WRITE_TIME_SERIES);
			this.globalStoreDAO.insertTimeSeriesFull(timeSeries);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public TimeSeries getTimeSeries(String key) {
		this.globalStoreDAO.addServiceUsage(key, READ_TIME_SERIES);
		return this.globalStoreDAO.getTimeSeriesFull(key);
	}

	//Data Points
	public DataPoints getDataPoints(String key) {
		this.globalStoreDAO.addServiceUsage(key, READ_DATA_POINTS);
		return this.globalStoreDAO.getDataPoints(key);
	}

	public void putDataPoints(DataPoints dataPoints) {
		this.globalStoreDAO.addServiceUsage(dataPoints.getKey(), WRITE_DATA_POINTS);
		this.globalStoreDAO.insertDataPoints(dataPoints);
	}	

	public static void main(String args[]){
		
		GlobalStoreService g = GlobalStoreService.getInstance();
		
		System.out.println(g.getObjectData("FX/test", ConsistencyLevel.ALL));	 	
	}
}
