package com.xpert.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.Map;

public class DocumentReaderSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private BufferedReader reader;
    private boolean completed;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector spoutOutputCollector) {
         String filename = conf.get("file-to-read").toString();
         File file = new File(filename);
         if(!file.exists()){
             System.out.println("File does not exists");
             return;
         }
         try{
             reader = new BufferedReader(new FileReader(filename));
         }
         catch (Exception ex){
             ex.printStackTrace();
         }

         this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        if(!completed && reader != null){
            try {
                String line = reader.readLine();
                if (line == null) {
                    completed = true;
                    reader.close();
                } else {
                    this.collector.emit(new Values(line));
                }
            }
            catch (Exception ex){
                ex.printStackTrace();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }
}
