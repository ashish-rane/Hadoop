package com.xpert.storm.trident.twitter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.operation.builtin.FirstN;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;

public class TopologyMain {

    public static final String TOPOLOGY_NAME = "TwitterHashtagTopology";

    public static void main(String[] args) throws InterruptedException {

        final Config  conf = new Config();
        conf.setDebug(true);

        // Spout
        final IBatchSpout spout = new TwitterTridentSpout();

        final TridentTopology topology = new TridentTopology();
        // Define the topology
        topology.newStream("twitterStream", spout) // create stream from spout
                .each(new Fields("tweet"), new HashTagExtractor(), new Fields("hashtag")) // apply custom function on each tuple
                .groupBy(new Fields("hashtag")) // group by the hashtag
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count") ) // aggregate to get count per hashtag
                .newValuesStream() // convert the resulting state object into a stream of values (hastag, count)
                .applyAssembly(new FirstN(10, "count")) // sort according to count and pick first 10.
                .each(new Fields("hashtag", "count"), new Debug()); // apply builtin function debug

        final LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology(TOPOLOGY_NAME, conf, topology.build());
            Thread.sleep(30000);
        }
        finally {
            cluster.shutdown();
        }
    }
}
