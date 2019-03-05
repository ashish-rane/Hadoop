package com.xpert.storm.grouping;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import topologycomponents.FileWriterBolt;
import topologycomponents.IntegerSpout;
import topologycomponents.MyBucketGrouping;

public class CustomGroupingMain {

    private static final  String TOPOLOGY_NAME = "CustomGroupingTopology";

    public static void main(String[] args) throws InterruptedException {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("CustomGroupingSpout", new IntegerSpout());
        builder.setBolt("CustomGroupingBolt", new FileWriterBolt(), 2).
                customGrouping("CustomGroupingSpout", new MyBucketGrouping());

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
