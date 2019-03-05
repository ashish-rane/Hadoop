package com.xpert.storm.trident.multiplefunction;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.tuple.Fields;

public class NumWordsInSentenceMain {

    public static void main(String[] args){
        LocalDRPC drpc = new LocalDRPC();
        TridentTopology topology = new TridentTopology();
        topology.newDRPCStream("Test", drpc)
                .map(new LowerCaseFunction())   // convert the words in sentence to lowercase
                .flatMap(new SplitFunction())   // split the sentence into words
                .filter(new FilterShortWordsFunction()) // filter all words with length <=3
                .aggregate(new Count(), new Fields("wc")); // Count is builtin function, the output tuple has field name wc.

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("MultiFunctionTopology", conf, topology.build());

        String [] sentences = new String[]{
          "This is the first sentence",
          "This is the second sentence",
          "and finally the last sentence"
        };

        for(String str: sentences){
            System.out.println("Result for " + str + " - [" + drpc.execute("Test", str) + "]");
        }

        cluster.shutdown();
    }
}
