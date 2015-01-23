package com.datastax.demo.service;

import org.joda.time.DateTime;

import com.datastax.timeseries.model.Periodicity;
import com.datastax.timeseries.model.TimeSeries;

public interface TimeSeriesService {

	TimeSeries getTimeSeries(String name, DateTime start, DateTime end);

	TimeSeries getTimeSeries(String name);

	TimeSeries getTimeSeries(String name, DateTime start, DateTime end, Periodicity periodicity);

	TimeSeries getTimeSeries(String name, Periodicity periodicity);

	void insertTimeSeries(TimeSeries timeSeries);
}
