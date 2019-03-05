package com.xpert.storm.fluxdemo;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Map;

public class ReadFieldsSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private BufferedReader reader;
    private boolean completed;


    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            String filename = conf.get("file-to-read").toString();
        File file = new File(filename);
        if(!file.exists()){
            return;
        }

        try {
            reader = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        try {
            if (!completed && reader != null) {
                String line = reader.readLine();
                if(line == null){
                    completed = true;
                    reader.close();
                    return;
                }

                this.collector.emit(new Values(line.split(",")));
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ticker","ymd", "open", "high", "low", "close", "volume"));
    }
}
