package com.xpert.storm.grouping;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import topologycomponents.DirectGroupingSpout;
import topologycomponents.FileWriterBolt;

public class DirectGroupingMain {

    private static final String TOPOLOGY_NAME = "DirectGroupingTopology";

    public static void main(String[] args) throws InterruptedException {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DirectGroupingSpout", new DirectGroupingSpout());
        builder.setBolt("DirectGroupingBolt", new FileWriterBolt(), 2)
                .directGrouping("DirectGroupingSpout");

        Config conf = new Config();
        conf.setDebug(true);
        conf.put("dir-to-write", "C:\\temp\\storm-outputs\\" + TOPOLOGY_NAME);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
            Thread.sleep(60000);

            /*while (Utilities.topologyExists(TOPOLOGY_NAME, cluster)){
                Thread.sleep(1000);
            }*/
        }
        finally {
            cluster.killTopology(TOPOLOGY_NAME);
            cluster.shutdown();
        }
    }
}
