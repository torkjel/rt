package com.github.torkjel.rt.api.resources;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.torkjel.rt.api.ApiMain;
import com.github.torkjel.rt.api.Services;
import com.github.torkjel.rt.api.model.HourStats;
import com.github.torkjel.rt.worker.WorkerMain;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.HttpRequest;

import lombok.extern.log4j.Log4j;

import static com.mashape.unirest.http.Unirest.*;

import static org.assertj.core.api.Assertions.*;

@Log4j
public class MultipleSlicesTest {

    @Before
    public void setUp() throws Exception {
        ApiMain.main(new String[] {"0", "/cluster-slizing.json"});

        // start worker http servers, at 9000, 9001 and 9002
        WorkerMain.main(new String[] {"0", "1", "2"});

        // Startup happens asynchronously behind our backs. Give it a grace period.
        Thread.sleep(1000);
    }

    @After
    public void shutDown() {
        com.github.torkjel.rt.api.Services.instance().close();
        com.github.torkjel.rt.worker.Services.instance().clearData();
        com.github.torkjel.rt.worker.Services.instance().close();
    }

    @Test
    public void testSequentiallyOver10Slices() throws Exception {

        long startTime = Services.instance().getConfig().getCluster().getStartOfFirstSlice();
        long sliceLength = Services.instance().getConfig().getCluster().getLengthOfSlice();

        for (int n = 0; n < sliceLength * 10; n++) {
            String url = "http://localhost:8000/analytics" + createQueryString(startTime + n, n, 5);
            log.info(url);
            HttpRequest postReq = post(url);
            try {
                HttpResponse<String> response = postReq.asString();
                assertThat(response.getStatus()).isEqualTo(202);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        com.github.torkjel.rt.api.Services.instance().blockUtilIdle();

        for (int n = 0; n < 10; n++) {
            verifyStats(5, sliceLength, startTime + sliceLength * n);
        }
    }

    private void verifyStats(int userCount, long eventCount, long timestamp) throws UnirestException {
        HttpRequest get = get("http://localhost:8000/analytics?timestamp=" + timestamp);
        HttpResponse<String> response = get.asString();
        assertThat(response.getStatus()).isEqualTo(200);
        HourStats stats = HourStats.parse(response.getBody());
        assertThat(stats.getClicks() + stats.getImpressions()).isEqualTo(eventCount);
        assertThat(stats.getUniqueUsers()).isEqualTo(userCount);

        HttpRequest getDetailed = get("http://localhost:8000/analytics/perworker?timestamp=" + timestamp);
        HttpResponse<String> detailedresponse = getDetailed.asString();
        System.out.println(detailedresponse.getBody());

    }

    private String createQueryString(long timestamp, long i, int userCount) {
        return "?timestamp=" + timestamp +
                "&user=user" + (i % userCount) +
                "&" + ((i & 1) == 1 ? "impression" : "click");
    }
}



