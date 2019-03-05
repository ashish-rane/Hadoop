package com.xpert.storm.trident.simplefunction;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;

/**
 * Hello world!
 *
 */
public class TopologyMain
{
    public static void main( String[] args ) throws InterruptedException {
        // Create a drpc client.
        LocalDRPC localDRPC = new LocalDRPC();
        // Create a trident topology
        TridentTopology tridentTopology = new TridentTopology();
        tridentTopology.newDRPCStream("simple", localDRPC)
                        .each(new Fields("args"), new SimpleFunction(), new Fields("processed_word"));

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("SimpleTridentTopology", conf, tridentTopology.build());

        for (String str: new String[]{"word1", "word2", "word3"}){
            // NOTE: The 1st arg is the function name to execute which must be same as
            // what is specified when creating new DRPC Stream above
            System.out.println("Result for " + str + localDRPC.execute("simple", str));
        }

        //Thread.sleep(6000);
        //cluster.killTopology("SimpleTridentTopology");
        cluster.shutdown();
    }
}
