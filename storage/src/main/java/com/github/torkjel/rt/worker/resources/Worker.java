package com.github.torkjel.rt.worker.resources;

import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
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
    public Response submit(@QueryParam("slice") Long slice,
            @QueryParam("user") String user,
            @QueryParam("click") String click,
            @QueryParam("impression") String impression,
            @Context UriInfo uriInfo) {

        log.info("POST /worker #" + submitCount.incrementAndGet());

        if (click == null && impression == null)
            return Response.status(400).build();

        String action = click != null ? "click" : "impression";

        Event event = Event.builder().action(action).slice(slice).user(user).build();

        return Response
                .ok(storageService(uriInfo).store(event).toString())
                .type(MediaType.TEXT_PLAIN)
                .build();
    }

    @GET
    public void retrieve(
            @Suspended AsyncResponse response,
            @QueryParam("slice") Long slice,
            @Context UriInfo uriInfo) {

        response.resume(storageService(uriInfo).retrieve(slice).toString());
    }

    private StorageService storageService(UriInfo uriInfo) {
        return Services.instance().getStorageServiceForNode(uriInfo.getBaseUri().getPort());
    }

}
