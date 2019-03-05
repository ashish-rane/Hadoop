package com.xpert.storm.trident.windowing;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.windowing.InMemoryWindowsStoreFactory;
import org.apache.storm.trident.windowing.WindowsStoreFactory;
import org.apache.storm.trident.windowing.config.SlidingCountWindow;
import org.apache.storm.trident.windowing.config.SlidingDurationWindow;
import org.apache.storm.trident.windowing.config.TumblingCountWindow;
import org.apache.storm.trident.windowing.config.WindowConfig;
import org.apache.storm.tuple.Fields;

import java.util.concurrent.TimeUnit;

/**
 * This program demonstrates how to use windows for calculating aggregate
 * not per tuple or overall but within a time window or a over a certain number of tuples (count window)
 */
public class WindowTopologyMain {

    public static final String TOPOLOGY_NAME = "WindowTopology";

    public static void main(String[] args) throws InterruptedException {

        // Where to store the window related information. In this case we store in-memory.
        // we could write our custom store to write a database.
        WindowsStoreFactory windowStore = new InMemoryWindowsStoreFactory();

        // Which type of window and other window specific parameters
        // Sliding count window
        //WindowConfig windowConfig = SlidingCountWindow.of(100, 10);

        // Tumbling window (no overlap between windows)
        WindowConfig windowConfig = TumblingCountWindow.of(100);

        // Sliding Duration window
        //WindowConfig windowConfig = SlidingDurationWindow.of(
        //        new BaseWindowedBolt.Duration(100, TimeUnit.MILLISECONDS),
        //        new BaseWindowedBolt.Duration(10, TimeUnit.MILLISECONDS));

        TridentTopology topology = new TridentTopology();

        topology.newStream("log-spout", new LogSpout())
                .window(windowConfig,
                        windowStore,
                        new Fields("log"),  // Input fields
                        new ErrorAggregator(), // what kind of aggregations
                        new Fields("count")) // output field
                .each(new Fields("count"), new Debug()); // for each window write to debug

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();

        try{
            cluster.submitTopology(TOPOLOGY_NAME, conf, topology.build());
            Thread.sleep(20000);
        }
        finally {
            cluster.shutdown();
        }
    }
}
