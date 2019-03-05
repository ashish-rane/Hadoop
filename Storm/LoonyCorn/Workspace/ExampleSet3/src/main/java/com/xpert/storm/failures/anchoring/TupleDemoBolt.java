package com.xpert.storm.failures.anchoring;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.PrintWriter;
import java.util.Map;

/**
 * To recover from failures we inherit the Bolt from Base Rich Bolt which provides
 * us with a output collector in the prepare method.
 */
public class TupleDemoBolt extends BaseRichBolt {

    private PrintWriter writer;
    private OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector outputCollector) {
        String path = conf.get("dir-to-write").toString();
        cleanupTargetFoler(path);

        String filename = "output-" + context.getThisTaskId()+"-" + context.getThisComponentId()+".csv";
        try{
            FileWriter fileWriter = new FileWriter(path + "\\" + filename, false);
            writer = new PrintWriter(fileWriter, true );
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
        this.collector = outputCollector;
    }

    private void cleanupTargetFoler(String path) {
        final File dir = new File(path);
        final File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.matches("output-*.csv");
            }
        });

        for(File file : files){
            if(!file.delete()){
                System.out.println("Can't remove : " + file.getAbsolutePath());
            }
        }
    }



    @Override
    public void execute(Tuple tuple) {
        String ticker = tuple.getStringByField("ticker");
        String ymd = tuple.getString(1);
        float close = Float.parseFloat(tuple.getStringByField("close"));

        writer.println(ticker + "|" + ymd + "|" + close);

        // The output collector provides an emit() overload in which we specify the input tuple
        // alongwith values to be passed to the next component.
        // if the next components down the line fail to process this tuple, Storm uses the
        // input tuple passed to replay the processing.
        // We can also supply a list of tuples in first parameter (called anchor) which allows us
        // anchor multiple input tuples to be replayed.
        // NOTE : If we inherit from BaseBasicBolt the job of anchoring the input tuple as well
        // as providing ack or fail to the collector is automatically handled.
        collector.emit(tuple,
                new Values(tuple.getString(0), tuple.getString(1), Float.parseFloat(tuple.getString(5))));

        // When using Base Rich Bolt we have to manually ack or fail each tuple to the collector.
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        writer.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ticker", "ymd" ,"close"));
    }
}
