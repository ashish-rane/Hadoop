package com.xpert.storm.tupledemo;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Map;

public class TupleDemoBolt extends BaseBasicBolt {

    private PrintWriter writer;

    @Override
    public void prepare(Map conf, TopologyContext context) {
        super.prepare(conf, context);

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
    public void cleanup() {
        writer.close();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        String ticker = tuple.getStringByField("ticker");
        String ymd = tuple.getString(1);
        float close = Float.parseFloat(tuple.getStringByField("close"));

        writer.println(ticker + "|" + ymd + "|" + close);

        basicOutputCollector.emit(
                new Values(tuple.getString(0), tuple.getString(1), Float.parseFloat(tuple.getString(5))));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ticker", "ymd" ,"close"));
    }
}
