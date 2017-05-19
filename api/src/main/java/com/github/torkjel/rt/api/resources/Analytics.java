package com.github.torkjel.rt.api.resources;

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

import javax.ws.rs.container.AsyncResponse;

@Path("/analytics")
public class Analytics {

    @POST
    public Response submit(
            @QueryParam("timestamp") Long ts,
            @QueryParam("user") String user,
            @QueryParam("click") String click,
            @QueryParam("impression") String impression) {

        if (click == null && impression == null)
            throw new WebApplicationException(400);

        Event event = Event.builder()
                .action(click != null ? "click" : "impression")
                .timestamp(ts)
                .user(user)
                .build();

        dispatcher().submit(event);

        return Response.accepted().build();
    }

    @GET
    public void retrieve(
            @Suspended AsyncResponse response,
            @QueryParam("timestamp") Long ts) {

        dispatcher().retrieve(
                ts,
                stats -> response.resume(stats.toString()));
    }

    private Dispatcher dispatcher() {
        return Services.instance().getDispatcher();
    }


}
