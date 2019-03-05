package com.xpert.storm.trident.joinmerge;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;

public class TopologyMain {

    private static final String TOPOLOGY_NAME = "JoinMergeTopology";

    public static void main(String[] args) throws InterruptedException {

        TridentTopology topology = new TridentTopology();

        // Create a source stream
        Stream source = topology.newStream("pair-spout", new PairSpout());

        // Create a sum stream with stream from pair spout as input
        // Params (input fields, function to apply on each tuple, output fields.
        Stream sum  = source.each(new Fields("x1", "x2"), new AddFunction(), new Fields("sum"));

        // Create a sum stream with stream from pair spout as input
        // Params (input fields, function to apply on each tuple, output fields.
        Stream product = source.each(new Fields("x1", "x2"), new MultiplyFunction(), new Fields("product"));

        // Merge the two streams
        topology.merge(sum, product)
                .peek(new Consumer() {
                    @Override
                    public void accept(TridentTuple tridentTuple) {
                        System.out.println( "From Merge stream: " + tridentTuple);
                    }
                });


        // Join the two streams on the fields x1 and x2
        // Params : first stream and its fields to join
        //          second stream and its fields to join
        //          fields of the output tuples.
        topology.join(sum, new Fields("x1", "x2"), product, new Fields("x1", "x2"),
                        new Fields("x1", "x2", "sum", "product"))
                .peek(new Consumer() {
                    @Override
                    public void accept(TridentTuple tridentTuple) {
                        System.out.println("From Join Stream: " + tridentTuple);
                    }
                });

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        try{
            cluster.submitTopology(TOPOLOGY_NAME, conf, topology.build());
            Thread.sleep(30000);
        }
        finally {
            cluster.shutdown();
        }
    }
}