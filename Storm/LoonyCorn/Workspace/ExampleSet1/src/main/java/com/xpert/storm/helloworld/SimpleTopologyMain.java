package com.xpert.storm.helloworld;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.topology.TopologyBuilder;

import javax.print.DocFlavor;
import java.util.List;
import java.util.stream.Collectors;

import static com.xpert.storm.Ultities.topologyExists;

public class SimpleTopologyMain {

    private static final String TOPOLOGY_NAME = "SimpleTopology";

    public static void main(String[] args) throws Exception {

        // Build Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("My-First-Simple-Spout", new SimpleSpout() );
        builder.setBolt("My-First-Simple-Bolt", new SimpleBolt()).shuffleGrouping("My-First-Simple-Spout");

        // Configuration
        Config config = new Config();
        config.setDebug(true);

        /*
        // Submit Topology to Local Cluster
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
            Thread.sleep(1000);

            while (com.xpert.storm.Ultities.topologyExists(TOPOLOGY_NAME, cluster)){
                Thread.sleep(1000);
            }
        }
        finally {
            cluster.killTopology(TOPOLOGY_NAME);
            cluster.shutdown();
        }
        */

        // Submit Topology to a remote cluster
        StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
    }

}
