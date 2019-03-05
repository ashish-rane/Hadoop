package com.xpert.storm.trident.multiplefunction;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.tuple.Fields;

public class WordcountPerSentenceMain {

    public static void main(String[] args) {

        LocalDRPC drpc = new LocalDRPC();
        TridentTopology topology = new TridentTopology();
        topology.newDRPCStream("Test", drpc)
                .map(new LowerCaseFunction())
                .flatMap(new SplitFunction())
                .filter(new FilterShortWordsFunction())
                .groupBy(new Fields("args")) // The new Map, Flatmap and filter function do not change the schema of tuples
                                            // hence we can refer the field as args which is what the drpc stream provides.
                .aggregate(new Count(), new Fields("wc"));

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("MultiFuncTopology", conf, topology.build());

        String [] sentences = new String[]{
                "This is the first sentence",
                "This is the second sentence",
                "and finally the last sentence"
        };

        for( String str: sentences){
            System.out.println("Result for: " + str + " - [" + drpc.execute("Test", str) + "]" );
        }

        cluster.shutdown();
    }
}
