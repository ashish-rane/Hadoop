package com.xpert.storm.tupledemo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {

    static final String TOPOLOGY_NAME = "TupleDemoTopology";

    public static void main(String[] args) throws InterruptedException {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("TupleDemoSpout", new TupleDemoSpout());
        builder.setBolt("TupleDemoBolt", new TupleDemoBolt()).shuffleGrouping("TupleDemoSpout");

        Config conf = new Config();
        conf.setDebug(true);

        // NOTE : This is only for testing purpose to keep files in same place as workspace.
        // Normally the input dir/files should be given through command line args.
        String path = System.getProperty("user.dir") + "\\ExampleSet1";
        conf.put("file-to-read", path + "\\stocks.csv");
        conf.put("dir-to-write", path);

        // Submit Topology to Cluster
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
            Thread.sleep(1000);

            while (com.xpert.storm.Ultities.topologyExists(TOPOLOGY_NAME, cluster)){
                Thread.sleep(1000);
            }
        }
        finally {
            cluster.killTopology(TOPOLOGY_NAME);
            cluster.shutdown();
        }
    }
}
