package com.xpert.storm.failures.manual;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {

    public static void main(String[] args) throws InterruptedException {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("ManualFailureSpout", new IntegerSpout());
        builder.setBolt("ManualFailureBolt", new RandomFailureBolt())
                    .shuffleGrouping("ManualFailureSpout");

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("ManualFailureTopology", conf, builder.createTopology());

        Thread.sleep(60000);

        cluster.killTopology("ManualFailureTopology");
        cluster.shutdown();
    }
}
