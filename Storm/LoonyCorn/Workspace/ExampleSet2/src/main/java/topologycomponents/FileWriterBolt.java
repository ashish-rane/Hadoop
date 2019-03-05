package topologycomponents;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

public class FileWriterBolt extends BaseBasicBolt {

    private PrintWriter writer;

    @Override
    public void prepare(Map conf, TopologyContext context) {
        String path = conf.get("dir-to-write").toString();
        File dir = new File(path);
        if(!dir.exists()){
            dir.mkdirs();
        }

        String filename = "output-" + context.getThisTaskId() + "-" +
                context.getThisComponentId()+".txt";

        File file = new File(path+ "\\" + filename);
        if(file.exists()){
            file.delete();
           /* try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }*/
        }

        try {
            PrintWriter printWriter = writer = new PrintWriter(new FileWriter(file), true);
        }
        catch (IOException ex){
            ex.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String str = tuple.getStringByField("number") + " - " +
                tuple.getStringByField("bucket");
        writer.println(str);
        basicOutputCollector.emit(new Values(str));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("field"));
    }

    @Override
    public void cleanup() {
        writer.close();
    }
}
