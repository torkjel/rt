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

import lombok.extern.log4j.Log4j;

import static com.mashape.unirest.http.Unirest.*;

import static org.assertj.core.api.Assertions.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

@Log4j
public class AddNodeToClusterTest {

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
    public void testReloadCluster() throws Exception {

        long startTime = Services.instance().getConfig().getCluster().getStartOfFirstSlice();
        long sliceLength = Services.instance().getConfig().getCluster().getLengthOfSlice();

        for (int n = 0; n < sliceLength * 4; n++) {
            String url = "http://localhost:8000/analytics" + createQueryString(startTime + n, n, 5);

            if (n == 8) {
                saveClusterConfig(tempFile, clusterUpdated);
                Services.instance().getConfig().reloadUpdatedCluster();
            }

            log.info(url);
            HttpRequest postReq = post(url);
            HttpResponse<String> response = postReq.asString();
            assertThat(response.getStatus()).isEqualTo(202);
        }

        com.github.torkjel.rt.api.Services.instance().blockUtilIdle();

        // 5 lines per worker that serves a slice.
        String firstSlice = getAsString("/perworker?timestamp=" + startTime);
        assertThat(firstSlice.split("\n")).hasSize(5);

        String secondSlice = getAsString("/perworker?timestamp=" + (startTime + sliceLength));
        assertThat(secondSlice.split("\n")).hasSize(15);

        String thirdSlice = getAsString("/perworker?timestamp=" + (startTime + sliceLength * 2));
        assertThat(thirdSlice.split("\n")).hasSize(15);

        String forthSlice = getAsString("/perworker?timestamp=" + (startTime + sliceLength * 3));
        assertThat(forthSlice.split("\n")).hasSize(5);

    }

    public String getAsString(String query) throws UnirestException {
        return get("http://localhost:8000/analytics" + query).asString().getBody();
    }

    private String createQueryString(long timestamp, long i, int userCount) {
        return "?timestamp=" + timestamp +
                "&user=user" + (i % userCount) +
                "&" + ((i & 1) == 1 ? "impression" : "click");
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
            "    \"length-of-time-slice\" : 10,\n" +
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
            "    \"length-of-time-slice\" : 10,\n" +
            "    \"first-slice\" : \"2017-05-20T00:00:00\",\n" +
            "\n" +
            "    \"routing\" : [\n" +
            "        { \"slice\" : 0, \"nodes\" : [\"0\"] },\n" +
            "        { \"slice\" : 1, \"nodes\" : [\"0\", \"1\", \"2\"] },\n" +
            "        { \"slice\" : 3, \"nodes\" : [\"0\"] }\n" +
            "    ]\n" +
            "}\n";

}



