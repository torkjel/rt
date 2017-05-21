package com.github.torkjel.rt.api.config;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

public class ClusterTest {

    private Cluster c;

    @Before
    public void setup() {
        c = Cluster.parseResource("/test-cluster.json");
    }

    @Test
    public void testLoadCluster() {
        assertThat(c.getLengthOfSlice()).isEqualTo(3600L);
        assertThat(c.getStartOfFirstSlice()).isBetween(System.currentTimeMillis() / 1000 - 2,  System.currentTimeMillis() / 1000);

        Routing first = c.getRouting().get(0);
        assertThat(first.getSlice()).isEqualTo(0);
        assertThat(first.getNodes().size()).isEqualTo(1);
        assertThat(first.getNodes().get(0)).isEqualTo("0");

        Routing forth = c.getRouting().get(3);
        assertThat(forth.getSlice()).isEqualTo(3);
        assertThat(forth.getNodes().size()).isEqualTo(3);
        assertThat(forth.getNodes().get(0)).isEqualTo("0");
        assertThat(forth.getNodes().get(1)).isEqualTo("2");
        assertThat(forth.getNodes().get(2)).isEqualTo("1");
    }

    @Test
    public void testRouting() {

        Routing first = c.getRouting().get(0);
        assertThat(first.getNode(6)).isEqualTo("0");
        assertThat(first.getNode(7)).isEqualTo("0");
        assertThat(first.getNode(8)).isEqualTo("0");

        Routing forth = c.getRouting().get(3);
        assertThat(forth.getNode(6)).isEqualTo("0");
        assertThat(forth.getNode(7)).isEqualTo("2");
        assertThat(forth.getNode(8)).isEqualTo("1");
    }

    @Test
    public void testResolveUrl() {
        assertThat(c.getUrlFor(c.getStartOfFirstSlice(), 0)).isEqualTo("http://localhost:9000/worker");
        assertThat(c.getUrlFor(c.getStartOfFirstSlice(), 1)).isEqualTo("http://localhost:9000/worker");
        assertThat(c.getUrlFor(c.getStartOfFirstSlice(), 2)).isEqualTo("http://localhost:9000/worker");

        assertThat(c.getUrlFor(c.getStartOfFirstSlice() + c.getLengthOfSlice() * 3 + 0, 6)).isEqualTo("http://localhost:9000/worker");
        assertThat(c.getUrlFor(c.getStartOfFirstSlice() + c.getLengthOfSlice() * 3 + 1, 7)).isEqualTo("http://localhost:9002/worker");
        assertThat(c.getUrlFor(c.getStartOfFirstSlice() + c.getLengthOfSlice() * 3 + 2, 8)).isEqualTo("http://localhost:9001/worker");
    }

}
