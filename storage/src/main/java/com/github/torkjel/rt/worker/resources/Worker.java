package com.github.torkjel.rt.worker.resources;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;

import com.github.torkjel.rt.worker.Services;
import com.github.torkjel.rt.worker.model.Event;

import javax.ws.rs.container.AsyncResponse;

@Path("/worker")
public class Worker {

    @POST
    public void submit(
            @Suspended AsyncResponse response,
            @QueryParam("timestamp") Long ts,
            @QueryParam("user") String user,
            @QueryParam("click") String click,
            @QueryParam("impression") String impression) {

        if (click == null && impression == null)
            response.resume(new WebApplicationException(400));

        String action = click != null ? "click" : "impression";

        Event event = Event.builder().action(action).timestamp(ts).user(user).build();

        Services.instance().getAnalyticsService().store(event);

        response.resume(Response.accepted().build());
    }

    @GET
    public void retrieve(
            @Suspended AsyncResponse response,
            @QueryParam("timestamp") Long ts) {

        response.resume(Services.instance().getAnalyticsService().retrieve(ts).toString());
    }

}
