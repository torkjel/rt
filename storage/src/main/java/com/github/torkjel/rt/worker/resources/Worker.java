package com.github.torkjel.rt.worker.resources;

import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import com.github.torkjel.rt.worker.Services;
import com.github.torkjel.rt.worker.model.Event;
import com.github.torkjel.rt.worker.model.StorageService;

import lombok.extern.log4j.Log4j;

import javax.ws.rs.container.AsyncResponse;

@Path("/worker")
@Log4j
public class Worker {

    private static AtomicInteger submitCount = new AtomicInteger(0);

    @POST
    public void submit(
            @Suspended AsyncResponse response,
            @QueryParam("timestamp") Long ts,
            @QueryParam("user") String user,
            @QueryParam("click") String click,
            @QueryParam("impression") String impression,
            @Context UriInfo uriInfo) {

        log.info("POST /worker #" + submitCount.incrementAndGet());

        if (click == null && impression == null)
            response.resume(new WebApplicationException(400));

        String action = click != null ? "click" : "impression";

        Event event = Event.builder().action(action).timestamp(ts).user(user).build();

        storageService(uriInfo).store(event);

        response.resume(Response.accepted().build());
    }

    @GET
    public void retrieve(
            @Suspended AsyncResponse response,
            @QueryParam("timestamp") Long ts,
            @Context UriInfo uriInfo) {

        response.resume(storageService(uriInfo).retrieve(ts).toString());
    }

    private StorageService storageService(UriInfo uriInfo) {
        return Services.instance().getStorageServiceForNode(uriInfo.getBaseUri().getPort());
    }

}
