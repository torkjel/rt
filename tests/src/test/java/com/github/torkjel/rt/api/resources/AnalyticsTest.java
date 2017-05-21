package com.github.torkjel.rt.api.resources;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.torkjel.rt.api.ApiMain;
import com.github.torkjel.rt.api.model.HourStats;
import com.github.torkjel.rt.worker.WorkerMain;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.request.HttpRequest;

import static com.mashape.unirest.http.Unirest.*;

import static org.assertj.core.api.Assertions.*;

public class AnalyticsTest {

    @Before
    public void setUp() throws Exception {
        ApiMain.main(new String[] {"0"});
        WorkerMain.main(new String[] {"0"});

        Thread.sleep(1000);
    }

    @After
    public void shutDown() {
        com.github.torkjel.rt.api.Services.instance().close();
        com.github.torkjel.rt.worker.Services.instance().clearData();
        com.github.torkjel.rt.worker.Services.instance().close();
    }

    @Test
    public void testNull() throws Exception {


        HttpRequest get = get("http://localhost:8000/analytics?timestamp=" + now());
        HttpResponse<String> response = get.asString();
        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(response.getBody()).isEqualTo(new HourStats(0, 0, 0).toString());
    }

    @Test
    public void testSimple() throws Exception {
        HttpRequest post = post("http://localhost:8000/analytics?timestamp=" + now() + "&user=foo&impression");
        assertThat(post.asBinary().getStatus()).isEqualTo(202);

        com.github.torkjel.rt.api.Services.instance().blockUtilIdle();

        HttpRequest get = get("http://localhost:8000/analytics?timestamp=" + now());
        HttpResponse<String> response = get.asString();
        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(response.getBody()).isEqualTo(new HourStats(1, 0, 1).toString());
    }

    private long now() {
        return System.currentTimeMillis() / 1000;
    }
}

