package com.github.torkjel.rt.api.resources;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.torkjel.rt.api.ApiMain;
import com.github.torkjel.rt.api.Services;
import com.github.torkjel.rt.worker.WorkerMain;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.HttpRequest;

import static com.mashape.unirest.http.Unirest.*;

import static org.assertj.core.api.Assertions.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class AcceptanceTest {

    private File tempFile;

    @Before
    public void setUp() throws Exception {
        tempFile = File.createTempFile("cluster", "json");

        ApiMain.main(new String[] {"0", saveClusterConfig(tempFile, clusterStart)});

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
    public void test() throws Exception {

        Random r = new Random();

        long startTime = Services.instance().getConfig().getCluster().getStartOfFirstSlice();
        long sliceLength = Services.instance().getConfig().getCluster().getLengthOfSlice();

        // run for 10 slices (10 minutes)
        for (long n = startTime; n < startTime + sliceLength * 10; n++) {

            // Update cluster just before starting second slice.
            if ((n - startTime) == sliceLength) {
                saveClusterConfig(tempFile, clusterUpdated);
                Services.instance().getConfig().reloadUpdatedCluster();
            }

            // Update cluster again
            if ((n - startTime) == sliceLength * 5) {
                saveClusterConfig(tempFile, clusterUpdated2);
                Services.instance().getConfig().reloadUpdatedCluster();
            }

            // print slice/worker status
            if ((n - startTime) % sliceLength == 0) {
                for (long i = startTime; i <= n; i += sliceLength) {
                    String sliceInfo = getAsString("/perworker?timestamp=" + i);
                    System.out.println(sliceInfo);
                }
            }

            // sustained load of 500 per second.
            for (int m = 0; m < 500; m++) {

                long timestamp;
                double ran = r.nextDouble();
                if (ran <= 0.05) {
                    // timestamp older than one slice length
                    timestamp = (long)((n - sliceLength) * r.nextDouble());
                } else {
                    // timestamp within one slice length
                    timestamp = (long)(n - r.nextDouble() * sliceLength);
                }

                // don't go before Beginning Of Time.
                timestamp = Math.max(timestamp, startTime);


                if (r.nextDouble() <= 0.1) {
                    // 10% lookups
                    HttpResponse<String> response = get("http://localhost:8000/analytics?timestamp=" + timestamp).asString();
                    assertThat(response.getStatus()).isEqualTo(200);
                } else {
                    // 90% new events
                    int user = r.nextInt(10000);
                    boolean click = r.nextBoolean();
                    String url = "http://localhost:8000/analytics" + createQueryString(timestamp, user, click);
                    HttpRequest postReq = post(url);
                    HttpResponse<String> response = postReq.asString();
                    assertThat(response.getStatus()).isEqualTo(202);
                }
            }
        }

        com.github.torkjel.rt.api.Services.instance().blockUtilIdle();

        for (long n = startTime; n < startTime + sliceLength * 10; n += sliceLength) {
            String sliceInfo = getAsString("?timestamp=" + n);
            System.out.println(sliceInfo);
        }

    }

    public String getAsString(String query) throws UnirestException {
        return get("http://localhost:8000/analytics" + query).asString().getBody();
    }

    private String createQueryString(long timestamp, int user, boolean click) {
        return "?timestamp=" + timestamp +
                "&user=user" + user +
                "&" + (click ? "impression" : "click");
    }

    private String saveClusterConfig(File f, String cluster) throws IOException {
        FileWriter fw = new FileWriter(f);
        fw.write(cluster);
        fw.close();;
        return f.getAbsolutePath();
    }

    private String clusterStart =
            "{\n" +
            "    \"api-nodes\" : {\n" +
            "        \"0\" : \"http://localhost:8000/analytics\"\n" +
            "    },\n" +
            "\n" +
            "    \"worker-nodes\" : {\n" +
            "        \"0\" : \"http://localhost:9000/worker\"\n" +
            "    },\n" +
            "\n" +
            "    \"length-of-time-slice\" : 60,\n" +
            "    \"first-slice\" : \"2017-05-20T00:00:00\",\n" +
            "\n" +
            "    \"routing\" : [\n" +
            "        { \"slice\" : 0, \"nodes\" : [\"0\"] }\n" +
            "    ]\n" +
            "}\n";

    private String clusterUpdated =
            "{\n" +
            "    \"api-nodes\" : {\n" +
            "        \"0\" : \"http://localhost:8000/analytics\"\n" +
            "    },\n" +
            "\n" +
            "    \"worker-nodes\" : {\n" +
            "        \"0\" : \"http://localhost:9000/worker\",\n" +
            "        \"1\" : \"http://localhost:9001/worker\",\n" +
            "        \"2\" : \"http://localhost:9002/worker\"\n" +
            "    },\n" +
            "\n" +
            "    \"length-of-time-slice\" : 60,\n" +
            "    \"first-slice\" : \"2017-05-20T00:00:00\",\n" +
            "\n" +
            "    \"routing\" : [\n" +
            "        { \"slice\" : 0, \"nodes\" : [\"0\"] },\n" +
            "        { \"slice\" : 1, \"nodes\" : [\"0\", \"1\", \"2\"] }\n" +
            "    ]\n" +
            "}\n";

    private String clusterUpdated2 =


            "{\n" +
            "    \"api-nodes\" : {\n" +
            "        \"0\" : \"http://localhost:8000/analytics\"\n" +
            "    },\n" +
            "\n" +
            "    \"worker-nodes\" : {\n" +
            "        \"0\" : \"http://localhost:9000/worker\",\n" +
            "        \"1\" : \"http://localhost:9001/worker\",\n" +
            "        \"2\" : \"http://localhost:9002/worker\"\n" +
            "    },\n" +
            "\n" +
            "    \"length-of-time-slice\" : 60,\n" +
            "    \"first-slice\" : \"2017-05-20T00:00:00\",\n" +
            "\n" +
            "    \"routing\" : [\n" +
            "        { \"slice\" : 0, \"nodes\" : [\"0\"] },\n" +
            "        { \"slice\" : 1, \"nodes\" : [\"0\", \"1\", \"2\"] },\n" +
            "        { \"slice\" : 7, \"nodes\" : [\"1\", \"2\"] }\n" +
            "    ]\n" +
            "}\n";

}



