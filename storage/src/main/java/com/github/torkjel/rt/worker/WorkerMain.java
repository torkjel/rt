package com.github.torkjel.rt.worker;

import io.vertx.core.AbstractVerticle;
import io.vertx.examples.resteasy.util.Runner;
import lombok.extern.log4j.Log4j;

import org.jboss.resteasy.plugins.server.vertx.VertxRequestHandler;
import org.jboss.resteasy.plugins.server.vertx.VertxResteasyDeployment;

import com.github.torkjel.rt.worker.resources.Worker;

/*
 * Init code based on https://github.com/vert-x3/vertx-examples/tree/master/resteasy-examples/src/main/java/io/vertx/examples/resteasy
 */
@Log4j
public class WorkerMain extends AbstractVerticle {

    public static void main(String[] args) {
        Services.instance().setConfig(Config.parse(args));
        Runner.runExample(WorkerMain.class);
        Runtime.getRuntime().addShutdownHook(new Thread(Services.instance()::close));
    }

    @Override
    public void start() throws Exception {

        // Bring up one http server per configured port. This allows us to run multiple worker
        // nodes in one jvm, which is really useful for testing.
        for (int port : Services.instance().getConfig().getPorts()) {

            VertxResteasyDeployment deployment = new VertxResteasyDeployment();
            deployment.start();
            deployment.getRegistry().addPerInstanceResource(Worker.class);

            vertx.createHttpServer()
                    .requestHandler(new VertxRequestHandler(vertx, deployment))
                    .listen(port, ar -> {
                        log.info("Server started on port "+ ar.result().actualPort());
                    });
        }

        Services.instance().setMain(this);
    }

    public void stop() {
        vertx.close();
    }
}
