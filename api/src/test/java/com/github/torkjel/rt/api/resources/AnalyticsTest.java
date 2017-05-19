package com.github.torkjel.rt.api.resources;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.torkjel.rt.api.Main;
import com.github.torkjel.rt.api.Services;
import com.github.torkjel.rt.api.model.HourStats;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.request.HttpRequest;

import static com.mashape.unirest.http.Unirest.*;

import static org.assertj.core.api.Assertions.*;

public class AnalyticsTest {
    
    @After
    public void shutDown() {
        Services.instance().getMain().stop();
    }

    @Before
    public void setUp() throws Exception {
        Main.main(null);
        Thread.sleep(500);
    }
 
    @Test
    public void testNull() throws Exception{
        HttpRequest get = get("http://localhost:8080/analytics?timestamp=1495135798");
        HttpResponse<String> response = get.asString();  
        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(response.getBody()).isEqualTo(new HourStats(0, 0, 0).toString());
    }
    
    @Test
    public void testSimple() throws Exception{
        HttpRequest post = post("http://localhost:8080/analytics?timestamp=1495135798&user=foo&impression");
        assertThat(post.asBinary().getStatus()).isEqualTo(202);
        
        HttpRequest get = get("http://localhost:8080/analytics?timestamp=1495135798");
        HttpResponse<String> response = get.asString();  
        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(response.getBody()).isEqualTo(new HourStats(1, 0, 1).toString());
    }

}
