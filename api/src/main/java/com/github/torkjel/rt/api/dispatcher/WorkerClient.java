package com.github.torkjel.rt.api.dispatcher;

import java.util.function.Consumer;

import org.asynchttpclient.*;

import com.github.torkjel.rt.api.model.Event;
import com.github.torkjel.rt.api.model.HourStats;

import lombok.extern.log4j.Log4j;

@Log4j
public class WorkerClient {

    private final String baseUrl;
    private final AsyncHttpClient asyncHttpClient;

    public WorkerClient(AsyncHttpClient client, String url) {
        this.baseUrl = url;
        this.asyncHttpClient = client;
    }

    public void submit(Event e) {

        String url = baseUrl + "?" + e.toUrlQueryPart();
        log.info("POSTing " + url);

        asyncHttpClient
            .preparePost(url)
            .execute(
                    new AsyncCompletionHandler<Response>(){
                        @Override
                        public Response onCompleted(Response response) throws Exception{
                            log.info("POSTed " + url + " : " + response.getStatusCode() + "\n" + response.getResponseBody());
                            return response;
                        }
                        @Override
                        public void onThrowable(Throwable t){
                            // TODO: metrics should report errors.
                            log.error("Request " + url + " failed", t);
                            t.printStackTrace();
                        }
                    });
    }

    public void retrieve(long timestamp, Consumer<HourStats> callback) {

        String url = baseUrl + "?timestamp=" + timestamp;

        log.info("GETing " + url);

        asyncHttpClient
            .prepareGet(url)
            .execute(
                    new AsyncCompletionHandler<Response>(){
                        @Override
                        public Response onCompleted(Response response) throws Exception{
                            log.info("GOT " + url + " : " + response.getStatusCode() + " \n " + response.getResponseBody());
                            callback.accept(HourStats.parse(response.getResponseBody()));
                            return response;
                        }
                        @Override
                        public void onThrowable(Throwable t){
                            // TODO: metrics should report errors.
                            log.error("Request " + url + " failed", t);
                        }
                    });
    }

}
