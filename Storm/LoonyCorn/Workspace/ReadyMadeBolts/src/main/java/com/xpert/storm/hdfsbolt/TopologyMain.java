package com.xpert.storm.hdfsbolt;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.topology.TopologyBuilder;


public class TopologyMain {

    private static final String TOPOLOGY_NAME = "ReadymadeBoltTopology";

    public static void  main(String[] args) throws InterruptedException {

        // Create a record format object for specifying the file format in the hdfs
        // could be delimited,parquet, sequence etc..
        RecordFormat recordFormat = new DelimitedRecordFormat().withFieldDelimiter(",");
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        // Rotate the files after size reaches 127 MB
        FileRotationPolicy fileRotationPolicy = new FileSizeRotationPolicy(127.0f,FileSizeRotationPolicy.Units.MB);

        // File naming in hdfs
        DefaultFileNameFormat fileNameFormat = new DefaultFileNameFormat();
        // specify target folder
        fileNameFormat.withPath("/user/root/projects/outputs/storm-data");

        // filename prefix
        fileNameFormat.withPrefix("records-");
        // filename extension
        fileNameFormat.withExtension(".csv");

        // Create HDFS bolt
        HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl("hdfs://sandbox-hdp.hortonworks.com:8020") // Specify namenode (fs.hadoopFS property)
                                .withFileNameFormat(fileNameFormat)
                                .withRotationPolicy(fileRotationPolicy)
                                .withSyncPolicy(syncPolicy)
                                .withRecordFormat(recordFormat);

        // Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("FileReadSpout", new ReadFieldsSpout());
        builder.setBolt("FilterFieldsBolt", new FilterFieldsBolt()).shuffleGrouping("FileReadSpout");
        builder.setBolt("HDFSBolt", hdfsBolt).shuffleGrouping("FilterFieldsBolt");

        Config conf = new Config();
        conf.setDebug(true);

        String path = System.getProperty("user-dir");
        //conf.put("file-to-read", path + "\\..\\ExampleSet1\\stocks.csv");
        conf.put("file-to-read", args[0]);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
        Thread.sleep(60000);

        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }

}
