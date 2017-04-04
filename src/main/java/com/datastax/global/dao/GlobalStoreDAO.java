package com.datastax.global.dao;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.LongBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cern.colt.list.DoubleArrayList;
import cern.colt.list.LongArrayList;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.timeseries.model.DataPoints;
import com.datastax.timeseries.model.ObjectData;
import com.datastax.timeseries.model.TimeSeries;

/** 
 * Global Cassandra DAO 
 * 
 * Provide insert/select statement for Object stores.
 * @author patrickcallaghan
 *
 */
public class GlobalStoreDAO {

	private Logger logger = LoggerFactory.getLogger(GlobalStoreDAO.class);

	private Cluster cluster;
	private Session session;

	private static final DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
	private static final String defaultKeyspace = "datastax_global_store";
	private static final String storeObjectTableName = defaultKeyspace + ".object";
	private static final String timeSeriesTableName = defaultKeyspace + ".timeseries";
	private static final String timeSeriesTableFullName = defaultKeyspace + ".timeseries_full";
	private static final String dataPointTableName = defaultKeyspace + ".datapoints";
	
	private static final String serviceUsageTableName = defaultKeyspace + ".service_usage";
		
	private static final String getFromStoreCQL = "select key, value from " + storeObjectTableName + " where key = ?";
	private static final String putInStoreCQL = "insert into " + storeObjectTableName + " (key, value) values (?,?)";
	
	private static final String insertTimeSeriesCQL = "Insert into " + timeSeriesTableFullName + " (key,dates,values) values (?,?,?);";
	private static final String selectTimeSeriesCQL = "Select key, dates, values from " + timeSeriesTableFullName + " where key = ?";

	private static final String insertClusterDataPointsCQL = "Insert into " + dataPointTableName + " (key,name,value) values (?,?,?);";
	private static final String selectClusterDataPointsCQL = "Select key, name, value from " + dataPointTableName + " where key = ?";
	
	private static final String insertClusterTimeSeriesCQL = "Insert into " + timeSeriesTableName + " (key,date,value) values (?,?,?);";
	private static final String selectClusterTimeSeriesCQL = "Select key, date, value from " + timeSeriesTableName + " where key = ? and date > ? and date <= ? limit ?";

	private static final String incrServiceUsageCQL = "update " + serviceUsageTableName
			+ " set count = count + 1 where namespace=? AND key=? AND service=? and ddmmyyyy=?";
	
	private PreparedStatement putInStore;
	private PreparedStatement getFromStore;	
	
	private PreparedStatement selectTimeSeries;
	private PreparedStatement insertTimeSeries;

	private PreparedStatement selectClusterDataPoints;
	private PreparedStatement insertClusterDataPoints;

	private PreparedStatement selectClusterTimeSeries;
	private PreparedStatement insertClusterTimeSeries;
	private PreparedStatement incrServiceUsage;

	public GlobalStoreDAO() {
		String contactPoints = PropertyHelper.getProperty("contactPoints", "127.0.0.1");
		
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
		
		this.incrServiceUsage = session.prepare(incrServiceUsageCQL);
	}
	
	public ObjectData getObjectFromStore(String key) throws Exception{
		
		BoundStatement bound = this.getFromStore.bind(key);
		
		ResultSet rs = session.execute(bound);
		if (rs != null && !rs.isExhausted()){
			return new ObjectData(key, rs.one().getString("value"));
		}else{
			return new ObjectData();
		}
	}
	
	public void putObjectInStore(String key, String value) throws IOException{
		
		BoundStatement bound = this.putInStore.bind(key, value);		
		session.execute(bound);
	}
	
	
	public TimeSeries getTimeSeriesFull(String key){
		
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

	public void insertTimeSeriesFull(TimeSeries timeSeries) throws Exception{
		logger.info("Writing " + timeSeries.getKey());
		
		BoundStatement boundStmt = new BoundStatement(this.insertTimeSeries);
		
		ByteBuffer datesBuffer = ByteBuffer.allocate((timeSeries.getDates().length + 1)*8);
		ByteBuffer pricesBuffer = ByteBuffer.allocate((timeSeries.getDates().length + 1)*8);
		
		long[] dates = timeSeries.getDates();
		double[] values = timeSeries.getValues();
		
		for (int i=0; i <dates.length; i++) {
			
			datesBuffer.putLong(dates[i]);
			pricesBuffer.putDouble(values[i]);
		}
				
		session.execute(boundStmt.bind(timeSeries.getKey(), datesBuffer.flip(), pricesBuffer.flip()));		
		
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

	public void insertDataPoints(DataPoints dataPoints){
		
		BoundStatement boundStmt = new BoundStatement(insertClusterDataPoints);					
		List<ResultSetFuture> results = new ArrayList<ResultSetFuture>();
		
		String[] names = dataPoints.getNames();
		
		for (int i=0; i < names.length; i++){
			boundStmt.setString(0, dataPoints.getKey());
			boundStmt.setString(1, dataPoints.getNames()[i]);
			boundStmt.setDouble(2, dataPoints.getValues()[i]);
			
			results.add(session.executeAsync(boundStmt));
		}			
		
		//Wait till we have everything back.
		boolean wait = true;
		while (wait) {
			// start with getting out, if any results are not done, wait is
			// true.
			wait = false;
			for (ResultSetFuture result : results) {
				if (!result.isDone()) {
					wait = true;
					break;
				}
			}
		}
		
		return;
	}
	
	public TimeSeries getTimeSeries(String key, long start, long end, int limit){
		
		BoundStatement boundStmt = this.selectClusterTimeSeries.bind(key, new Date(start), new Date(end), limit);

		logger.info("" +  new Date(start) + " - " + new Date(end) + " " + limit);
		ResultSet rs = session.execute(boundStmt);
				
		List<Row> all = rs.all();
				
		DoubleArrayList valueArray = new DoubleArrayList(20000);
		LongArrayList dateArray = new LongArrayList(20000);
		
		for (Row row : all){
			dateArray.add(row.getTimestamp("date").getTime());
			valueArray.add(row.getDouble("value"));	
		}
		
		dateArray.trimToSize();
		valueArray.trimToSize();
		
		return new TimeSeries(key, dateArray.elements(), valueArray.elements());
	}	

	public void insertClusterTimesSeries(TimeSeries timeSeries) {
		
		BoundStatement boundStmt = new BoundStatement(this.insertClusterTimeSeries);					
		List<ResultSetFuture> results = new ArrayList<ResultSetFuture>();
		
		long[] dates = timeSeries.getDates();
		
		for (int i=0; i < dates.length; i++){
			boundStmt.setString(0, timeSeries.getKey());
			boundStmt.setTimestamp(1, new Date(timeSeries.getDates()[i]));
			boundStmt.setDouble(2, timeSeries.getValues()[i]);
			
			results.add(session.executeAsync(boundStmt));
		}			
		
		//Wait till we have everything back.
		boolean wait = true;
		while (wait) {
			// start with getting out, if any results are not done, wait is
			// true.
			wait = false;
			for (ResultSetFuture result : results) {
				if (!result.isDone()) {
					wait = true;
					break;
				}
			}
		}		
		return;
	}
	
	public void addServiceUsage(String key, String service){
		
		
		String namespace =  key.contains("/") ? key.substring(0, key.indexOf('/')) : key;
		
		BoundStatement boundStmt = this.incrServiceUsage.bind(namespace, key, service, df.format(new Date()));
		session.execute(boundStmt);
	}
}
