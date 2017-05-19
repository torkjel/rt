package com.github.torkjel.rt.api;

import io.vertx.core.AbstractVerticle;
import io.vertx.examples.resteasy.util.Runner;
import org.jboss.resteasy.plugins.server.vertx.VertxRequestHandler;
import org.jboss.resteasy.plugins.server.vertx.VertxResteasyDeployment;

import com.github.torkjel.rt.api.resources.Analytics;

/*
 * Init code shamelessly stolen from https://github.com/vert-x3/vertx-examples/tree/master/resteasy-examples/src/main/java/io/vertx/examples/resteasy
 */
public class Main extends AbstractVerticle {

    public static void main(String[] args) {
        Runner.runExample(Main.class);
    }

    @Override
    public void start() throws Exception {
        VertxResteasyDeployment deployment = new VertxResteasyDeployment();
        deployment.start();
        deployment.getRegistry().addPerInstanceResource(Analytics.class);
        
        vertx.createHttpServer()
        	    .requestHandler(new VertxRequestHandler(vertx, deployment))
        	    .listen(8080, ar -> {
        	        System.out.println("Server started on port "+ ar.result().actualPort());
        	    });
        
        Services.instance().setMain(this);
    }

    public void stop() {
        vertx.close();
    }
}
