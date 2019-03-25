package com.datastax.global.webservices;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.service.GlobalStoreService;
import com.datastax.timeseries.model.DataPoints;
import com.datastax.timeseries.model.ObjectData;
import com.datastax.timeseries.model.Periodicity;
import com.datastax.timeseries.model.TimeSeries;

/**
 * Web Service for global service.
 * @author patrickcallaghan
 *
 */
@Path("/")
public class GlobalStoreWS {

	private Logger logger = LoggerFactory.getLogger(GlobalStoreWS.class);

	//Service Layer.
	private GlobalStoreService globalStoreService =  GlobalStoreService.getInstance();

	@GET
	@Path("/get/object")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getObjectByKey(@QueryParam("key") String key) {

		ObjectData object = this.globalStoreService.getObjectData(key);
		return Response.status(201).entity(object.getValue()).build();
	}

	@GET
	@Path("/get/timeseriesfull/")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getTimesSeriesFullByKey(@QueryParam("key") String key) {

		TimeSeries timeSeries = this.globalStoreService.getTimeSeries(key);
		return Response.status(201).entity(timeSeries).build();
	}

	@GET
	@Path("/get/timeseries/")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getTimesSeriesByKey(@QueryParam("key") String key, @QueryParam("start") long start,
			@QueryParam("start") long end, @QueryParam("periodicity") String periodicityStr) {

		
		Periodicity periodicity = null;
		if (periodicityStr!=null){
			periodicity = Periodicity.valueOf(periodicityStr);
		}
		
		TimeSeries timeSeries = this.globalStoreService.getTimeSeries(key, periodicity);
		
		return Response.status(201).entity(timeSeries).build();
	}

	@GET
	@Path("/get/datapoints/")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getDataPointsByKey(@QueryParam("key") String key) {

		DataPoints dataPoints = this.globalStoreService.getDataPoints(key);
		return Response.status(201).entity(dataPoints).build();
	}

	// -------------------------------------------------------------------
	@POST
	@Path("/post/timeseriesfull/")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response insertTimesSeriesFull(TimeSeries timeSeries) {

		logger.info(timeSeries.toString());
		globalStoreService.insertFullTimeSeries(timeSeries);
		return Response.status(201).build();
	}

	@POST
	@Path("/post/timeseries/")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response insertTimesSeries(TimeSeries timeSeries) {

		logger.info(timeSeries.toString());
		globalStoreService.insertClusterTimeSeries(timeSeries);

		return Response.status(201).build();
	}

	@POST
	@Path("/post/object")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response postObjectByKey(ObjectData objectData) {

		logger.info(objectData.toString());
		this.globalStoreService.putObjectData(objectData);
		return Response.status(201).build();
	}

	@POST
	@Path("/post/datapoints")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response postDataPoints(DataPoints dataPoints) {

		logger.info(dataPoints.toString());
		this.globalStoreService.putDataPoints(dataPoints);
		return Response.status(201).build();
	}
}
