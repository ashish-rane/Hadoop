package com.xpert.storm.trident.multiplefunction;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * This program calculates the wordcount across tuples instead of per tuple.
 * In this case we use a persistantAggregate function instead of aggregate function
 */
public class WordCountAcrossSentencesMain {

    public static final String TOPOLOGY_NAME="WordCountPersistanceAggregateTopology";

    public static void main(String[] args) throws InterruptedException {

        // The fixed batch spout is built in and it cycles through the provided
        // values.
        // the spout emits a single field call sentence.
        FixedBatchSpout spout = new FixedBatchSpout(
                new Fields("sentence"),
                3,
                new Values("the cow jumped over the moon and man"),
                new Values("the man is a great cow"),
                new Values("the cow will come home")
        );

        // set the spout to continously cycle through the values as long as topology is alive.
        spout.setCycle(true);

        // required to query the persistent aggregate.
        LocalDRPC drpc = new LocalDRPC();

        TridentTopology topology = new TridentTopology();

        // The persistent aggregate function returns a trident state instead of a stream.
        // We use MemoryMapState to calculate count per word. If we want overall count
        // we use MemCachedState
        TridentState wordcounts = topology.newStream("Spout-1", spout)
                                    .map(new LowerCaseFunction())
                                    .flatMap(new SplitFunction())
                                    .groupBy(new Fields("sentence")) // the map, flatmap do not change the schema of the tuple.
                                    .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("wc"));

        // Query the trident state.
        // The drpc stream has a default field called args and hte field to query is wc and we want perform
        // a get function on the map.
        topology.newDRPCStream("words", drpc)
                .stateQuery(wordcounts, new Fields("args"), new MapGet(), new Fields("wc"));

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology(TOPOLOGY_NAME, conf, topology.build());
            Thread.sleep(60000);

            // We query for the counts for the following words. This will give counts
            // at the moment of query which may change later.
            for (String s : new String[]{"cow", "man", "dog"}) {
                System.out.println("Result for word " + s + drpc.execute("words", s));
            }
        }
        finally {
            cluster.shutdown();
        }

    }
}
