package com.xpert.storm.grouping;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import topologycomponents.FileWriterBolt;
import topologycomponents.IntegerSpout;

public class FieldsGroupingMain {

    private static final String TOPOLOGY_NAME = "FieldsGroupingTopology";

    public static void main(String[] args) throws InterruptedException {

        // Define Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("FieldsGroupingSpout", new IntegerSpout());
        builder.setBolt("FieldsGroupingBolt", new FileWriterBolt(), 2)
                .fieldsGrouping("FieldsGroupingSpout", new Fields("bucket"));

        // Configuration
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
