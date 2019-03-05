package com.xpert.storm.failures.anchoring;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

public class TupleDemoSpout extends BaseRichSpout {

    private boolean completed;
    private BufferedReader reader;
    private SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        String filename = conf.get("file-to-read").toString();
        File file = new File(filename);
        if(!file.exists()){
            System.out.println("File does not exist");
            return;
        }

        try {

            this.reader = new BufferedReader(new FileReader(filename));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        this.collector = spoutOutputCollector;
        completed  = false;
    }

    @Override
    public void nextTuple() {

        try {
            if (!completed && reader != null) {
                String line = reader.readLine();
                if (null == line) {
                    completed = true;
                    reader.close();
                } else {
                    this.collector.emit(new Values(line.split(",")));
                }
            }
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ticker","ymd","open", "high", "low", "close", "volume"));
    }
}
