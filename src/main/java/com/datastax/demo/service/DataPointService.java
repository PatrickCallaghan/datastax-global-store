package com.datastax.demo.service;

import com.datastax.timeseries.model.DataPoints;

public interface DataPointService {

	DataPoints getDataPoints(String key);
	
	void putDataPoints(String key, DataPoints dataPoints);
}
