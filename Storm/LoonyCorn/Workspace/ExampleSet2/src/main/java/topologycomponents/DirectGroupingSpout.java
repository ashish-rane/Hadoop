package topologycomponents;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;

public class DirectGroupingSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    Integer i = 0;
    private List<Integer> boltIds;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;

        // Get the list of Task Ids which correspond to the FileWriteBolt.
        // The name of the bolt should be the same as specified here
        boltIds = topologyContext.getComponentTasks("DirectGroupingBolt");
    }

    @Override
    public void nextTuple() {
        if(i <= 100){

            Integer bucket = i % 10;
            this.collector.emitDirect(boltIds.get(getBoltIndex(bucket)), new Values(i.toString(), bucket.toString()));
            i += 1;
        }
    }

    private int getBoltIndex(Integer bucket) {
        return bucket % boltIds.size();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("number", "bucket"));
    }
}
