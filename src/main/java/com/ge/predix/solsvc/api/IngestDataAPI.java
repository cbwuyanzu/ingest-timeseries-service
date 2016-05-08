package com.ge.predix.solsvc.api;

import java.io.InputStream;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

/**
 * 
 * @author predix -
 */
@Consumes({ "application/json", "application/xml" })
@Produces({ "application/json", "application/xml" })
@Path("/pm25services")
public interface IngestDataAPI {
	/**
	 * @return -
	 */
	@GET
	@Path("/ping")
	public Response greetings();

	
	@GET
	@Path("/ingest")
	public Response postDataPoints(@DefaultValue("50") @QueryParam("measure") String measure,
			@DefaultValue("CHEN") @QueryParam("tag") String tag);


}
