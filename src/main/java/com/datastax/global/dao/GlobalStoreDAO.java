package com.datastax.global.dao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cern.colt.list.DoubleArrayList;
import cern.colt.list.LongArrayList;

import com.datastax.demo.utils.ByteUtils;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.timeseries.model.DataPoints;
import com.datastax.timeseries.model.TimeSeries;

public class GlobalStoreDAO {
	
	private Logger logger = LoggerFactory.getLogger(GlobalStoreDAO.class);

	private Cluster cluster;
	private Session session;
	
	private static final String defaultKeyspace = "datastax_global_store";
	private static final String storeObjectTableName = defaultKeyspace + ".object";
	private static final String timeSeriesTableName = defaultKeyspace + ".timeseries";
	private static final String dataPointTableName = defaultKeyspace + ".datapoints";
		
	private static final String getFromStoreCQL = "select * from " + storeObjectTableName + " where key = ?";
	private static final String putInStoreCQL = "insert into " + storeObjectTableName + " (key, value) values (?,?)";
	
	private static final String insertTimeSeriesCQL = "Insert into " + timeSeriesTableName + " (key,dates,values) values (?,?,?);";
	private static final String selectTimeSeriesCQL = "Select key, dates, ticks from " + timeSeriesTableName + " where key = ?";

	private static final String insertClusterDataPointsCQL = "Insert into " + dataPointTableName + " (key,name,value) values (?,?,?);";
	private static final String selectClusterDataPointsCQL = "Select key, date, value from " + dataPointTableName + " where key = ? and date < ? and date >= ? limit ?";
	
	private static final String insertClusterTimeSeriesCQL = "Insert into " + timeSeriesTableName + " (key,date,value) values (?,?,?);";
	private static final String selectClusterTimeSeriesCQL = "Select key, name, value from " + timeSeriesTableName + " where key = ?";

	
	private PreparedStatement putInStore;
	private PreparedStatement getFromStore;	
	
	private PreparedStatement selectTimeSeries;
	private PreparedStatement insertTimeSeries;

	private PreparedStatement selectClusterDataPoints;
	private PreparedStatement insertClusterDataPoints;

	private PreparedStatement selectClusterTimeSeries;
	private PreparedStatement insertClusterTimeSeries;

	public GlobalStoreDAO(String[] contactPoints) {
		cluster = Cluster.builder().addContactPoints(contactPoints).build();
		session = cluster.connect();
		
		this.getFromStore = session.prepare(getFromStoreCQL);
		this.putInStore = session.prepare(putInStoreCQL);
		
		this.selectTimeSeries = session.prepare(selectTimeSeriesCQL);
		this.insertTimeSeries = session.prepare(insertTimeSeriesCQL);

		this.selectClusterDataPoints = session.prepare(selectClusterDataPointsCQL);
		this.insertClusterDataPoints = session.prepare(insertClusterDataPointsCQL);
		
		this.selectClusterTimeSeries = session.prepare(selectClusterTimeSeriesCQL);
		this.insertClusterTimeSeries = session.prepare(insertClusterTimeSeriesCQL);
	}
	
	public Object getObjectFromStore(String key) throws Exception{
		
		BoundStatement bound = this.getFromStore.bind(ByteUtils.toByteBuffer(key));
		
		ResultSet rs = session.execute(bound);
		
		if (rs.isFullyFetched()){
			logger.info("No results found for symbol : " + key);
			throw new RuntimeException ("Object not found for key :"+  key);
		}else{
			return ByteUtils.fromByteBuffer(rs.one().getBytes("value"));
		}
	}
	
	public void putObjectInStore(String key, Object value) throws IOException{
		
		BoundStatement bound = this.putInStore.bind(key, ByteUtils.toByteBuffer(value));
		
		session.execute(bound);
	}
	
	
	public TimeSeries getTimeSeries(String key){
		
		BoundStatement boundStmt = new BoundStatement(this.selectTimeSeries);
		boundStmt.setString(0, key);
		
		ResultSet resultSet = session.execute(boundStmt);		
		
		DoubleArrayList valueArray = new DoubleArrayList(20000);
		LongArrayList dateArray = new LongArrayList(20000);

		if (resultSet.isExhausted()){
			logger.info("No results found for key : " + key);
			dateArray.trimToSize();
			valueArray.trimToSize();
			
			return new TimeSeries(key, dateArray.elements(), valueArray.elements());
		}
		
		Row row = resultSet.one();
		
		LongBuffer dates = row.getBytes("dates").asLongBuffer();		
		DoubleBuffer ticks = row.getBytes("values").asDoubleBuffer();
		
		while (dates.hasRemaining()){
			dateArray.add(dates.get());
			valueArray.add(ticks.get());	
		}
		
		dateArray.trimToSize();
		valueArray.trimToSize();
		
		return new TimeSeries(key, dateArray.elements(), valueArray.elements());
	}

	public void insertTimeSeries(TimeSeries timeSeries) throws Exception{
		logger.info("Writing " + timeSeries.getSymbol());
		
		BoundStatement boundStmt = new BoundStatement(this.insertTimeSeries);
		
		ByteBuffer datesBuffer = ByteBuffer.allocate((timeSeries.getDates().length + 1)*8);
		ByteBuffer pricesBuffer = ByteBuffer.allocate((timeSeries.getDates().length + 1)*8);
		
		long[] dates = timeSeries.getDates();
		double[] values = timeSeries.getValues();
		
		for (int i=0; i <dates.length; i++) {
			
			datesBuffer.putLong(dates[i]);
			pricesBuffer.putDouble(values[i]);
		}
				
		session.execute(boundStmt.bind(timeSeries.getSymbol(), datesBuffer.flip(), pricesBuffer.flip()));		
		
		datesBuffer.clear();
		pricesBuffer.clear();
		
		return;
	}
	
	public DataPoints getDataPoints(String key){
		
		BoundStatement boundStmt = this.selectClusterDataPoints.bind(key);
		
		ResultSet rs = session.execute(boundStmt);
		List<Row> all = rs.all();
				
		List<String> names = new ArrayList<String>();
		List<Double> values = new ArrayList<Double>();
		
		for (Row row : all){
			names.add(row.getString("name"));
			values.add(row.getDouble("value"));
		}
		
		
		return new DataPoints(key, names.toArray(new String[0]), values.toArray(new Double[0]));
	}

	public void insertDataPoints(String key, String name, double value) throws Exception{
		
		BoundStatement boundStmt = this.insertClusterDataPoints.bind(key, name, value);
					
		session.execute(boundStmt);
	}
	
	public TimeSeries getTimeSeries(String key, long start, long end, int limit){
		
		BoundStatement boundStmt = this.selectClusterTimeSeries.bind(key, start, end, limit);
		
		ResultSet rs = session.execute(boundStmt);
		List<Row> all = rs.all();
				
		DoubleArrayList valueArray = new DoubleArrayList(20000);
		LongArrayList dateArray = new LongArrayList(20000);
		
		for (Row row : all){
			dateArray.add(row.getLong("date"));
			valueArray.add(row.getDouble("value"));	
		}
		
		dateArray.trimToSize();
		valueArray.trimToSize();
		
		return new TimeSeries(key, dateArray.elements(), valueArray.elements());
	}	

	public void insertTimesSeries(String key, long date, double value) {
		
		BoundStatement boundStmt = this.insertClusterTimeSeries.bind(key, date, value);
					
		session.execute(boundStmt);
	}
}
