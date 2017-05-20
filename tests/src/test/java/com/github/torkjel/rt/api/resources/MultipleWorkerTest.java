package com.github.torkjel.rt.api.resources;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.torkjel.rt.api.ApiMain;
import com.github.torkjel.rt.api.model.HourStats;
import com.github.torkjel.rt.worker.WorkerMain;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.HttpRequest;

import lombok.extern.log4j.Log4j;

import static com.mashape.unirest.http.Unirest.*;

import static org.assertj.core.api.Assertions.*;

import java.util.stream.LongStream;

@Log4j
public class MultipleWorkerTest {

    @Before
    public void setUp() throws Exception {
        ApiMain.main(new String[] {"0", "/cluster2.json"});

        // start two http servers, at 9000 and 9001
        WorkerMain.main(new String[] {"0"});
        Thread.sleep(1000);
    }

    @After
    public void shutDown() {
        com.github.torkjel.rt.api.Services.instance().getMain().stop();
        com.github.torkjel.rt.worker.Services.instance().getMain().stop();
    }

    @Test
    public void testNull() throws Exception{
        HttpRequest get = get("http://localhost:8000/analytics?timestamp=1495135798");
        HttpResponse<String> response = get.asString();
        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(response.getBody()).isEqualTo(new HourStats(0, 0, 0).toString());
    }

    @Test
    public void testSimple() throws Exception{
        HttpRequest post = post("http://localhost:8000/analytics?timestamp=1495135798&user=foo&impression");
        assertThat(post.asBinary().getStatus()).isEqualTo(202);

        HttpRequest get = get("http://localhost:8000/analytics?timestamp=1495135798");
        HttpResponse<String> response = get.asString();
        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(response.getBody()).isEqualTo(new HourStats(1, 0, 1).toString());
    }

    @Test
    public void testLoad() throws Exception {

        // Generate 100k events from 1k users. 50/50% impressions and clicks.

        int userCount = 1000;
        int eventCount = 100000;

        long hour = System.currentTimeMillis() / 3600 * 3600;
        LongStream.range(0, eventCount)
            .parallel()
            .map(i -> hour + (int)(Math.random() * (i % 3600)))
            .forEach(timestamp -> {
                String url = "http://localhost:8000/analytics" + createQueryString(timestamp, userCount);
                log.info(url);
                HttpRequest postReq = post(url);
                try {
                    HttpResponse<String> response = postReq.asString();
                    assertThat(response.getStatus()).isEqualTo(202);
                } catch (UnirestException e) {
                    throw new RuntimeException(e);
                }
            });

        HttpRequest get = get("http://localhost:8000/analytics?timestamp=" + hour);
        HttpResponse<String> response = get.asString();
        assertThat(response.getStatus()).isEqualTo(200);
        HourStats stats = HourStats.parse(response.getBody());
        assertThat(stats.getClicks() + stats.getImpressions()).isEqualTo(eventCount);
        assertThat(stats.getUniqueUsers()).isEqualTo(userCount);
    }

    private String createQueryString(long timestamp, int userCount) {
        return "?timestamp=" + timestamp +
                "&user=user" + (timestamp % userCount) +
                "&" + ((timestamp & 1) == 1 ? "impression" : "click");
    }

}



