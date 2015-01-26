package com.datastax.global.test;

import static org.junit.Assert.*;

import org.joda.time.DateTime;
import org.junit.Test;

import com.datastax.timeseries.model.TimeSeries;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class ServiceTest {

	@Test
	public void postTimeSeries() {

		try {
			Client client = Client.create();

			WebResource webResource = client
					.resource("http://localhost:8080/datastax-global-store/rest/post/timeseriesfull");

			String input = "{\"key\":\"test\",\"dates\":[1395792120000,1395883380000,1395883440000,1395885840000,1395886320000,1395886980000,1395887040000,1395889140000,1395889260000,1395889380000,1395890100000,1395890760000,1395891240000,1395891540000,1395893520000,1395894180000,1395894300000,1395894360000,1395895140000,1395895740000,1395895920000,1395896340000,1395896580000,1395896640000,1395898440000,1395898920000,1395898980000,1395899040000,1395899640000,1395899700000,1395899820000,1395899940000,1395900060000,1395900240000,1395900600000,1395900780000,1395900960000,1395901200000,1395901560000,1395901740000,1395902280000,1395902460000,1395902520000,1395902760000,1395902940000],\"values\":[5.0,6.0,4.0,3.0,4.0,5.0,6.0,7.0,5.0,5.0,6.0,7.0,8.0,5.0,5.0,6.0,7.0,8.0,9.0,7.0,6.0,5.0,6.0,4.0,3.0,5.0,6.0,7.0,5.0,4.0,6.0,3.0,4.0,5.0,7.0,5.0,4.0,6.0,7.0,5.0,5.0,4.0,6.0,7.0,8.0]}";

			ClientResponse response = webResource.type("application/json").post(ClientResponse.class, input);

			if (response.getStatus() != 201) {
				throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
			}

			System.out.println("Output from Server .... \n");
			String output = response.getEntity(String.class);
			System.out.println(output);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
