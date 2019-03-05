package com.xpert.storm.readfromfile;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {

    static final String TOPOLOGY_NAME = "FileReadTopology";

    public static void main(String[] args) throws InterruptedException {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("FileReaderSpout", new FileReaderSpout());
        builder.setBolt("SimpleBolt", new SimpleBolt()).shuffleGrouping("FileReaderSpout");

        Config conf = new Config();
        conf.setDebug(true);
        // Assuming we have our files in Workspace level
        String path = System.getProperty("user.dir") + "\\ExampleSet1" ;
        conf.put("file-to-read", path + "\\words.txt");

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
