package com.datastax.global.webservices;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.datastax.global.dao.GlobalStoreDAO;

@Path("/globalstore/")
public class GlobalStoreWS {
	
	private GlobalStoreDAO orderManagementDAO = new GlobalStoreDAO(new String[]{"127.0.0.1"});
	
	@GET
	@Path("/data/get/")
	@Produces(MediaType.APPLICATION_JSON)
	public Object getObjectByKey (@QueryParam("key") String key){
		
		return null;
	}
	
	@PUT
	@Path("/data/put/")
	@Produces(MediaType.APPLICATION_JSON)
	public void saveObjectByKey (@QueryParam("key") String key){
		
	}
}

