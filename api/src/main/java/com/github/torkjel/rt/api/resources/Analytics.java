package com.github.torkjel.rt.api.resources;

import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;

import com.github.torkjel.rt.api.Services;
import com.github.torkjel.rt.api.dispatcher.Dispatcher;
import com.github.torkjel.rt.api.model.Event;

import lombok.extern.log4j.Log4j;

import javax.ws.rs.container.AsyncResponse;

@Path("/analytics")
@Log4j
public class Analytics {

    private static AtomicInteger submitCount = new AtomicInteger(0);

    @POST
    public Response submit(
            @QueryParam("timestamp") Long ts,
            @QueryParam("user") String user,
            @QueryParam("click") String click,
            @QueryParam("impression") String impression) {

        log.info("POST /analytics #" + submitCount.incrementAndGet());

        if (click == null && impression == null)
            throw new WebApplicationException(400);

        Event event = Event.builder()
                .action(click != null ? "click" : "impression")
                .timestamp(ts)
                .user(user)
                .build();

        dispatcher().submit(event);

        log.info("POST /analytics DONE");
        return Response.accepted().build();
    }

    @GET
    public void retrieve(
            @Suspended AsyncResponse response,
            @QueryParam("timestamp") Long timestamp) {

        log.info("GET /analytics");

        dispatcher().retrieve(
                timestamp,
                stats -> response.resume(stats.toString()));

        log.info("GET /analytics DONE");
    }

    @GET
    @Path("/perworker")
    public void retrievePerWorker(
            @Suspended AsyncResponse response,
            @QueryParam("timestamp") Long timestamp) {

        log.info("GET /analytics/perworker");

        dispatcher().retrieveDetailed(
                timestamp,
                stats -> response.resume(stats));

        log.info("GET /analytics/perworker DONE");
    }

    private Dispatcher dispatcher() {
        return Services.instance().getDispatcher();
    }


}
