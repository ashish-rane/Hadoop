package topologycomponents;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class MyBucketGrouping implements CustomStreamGrouping, Serializable {

    private List<Integer> targetTasks;

    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId,
                        List<Integer> targetTasks) {
        // Store the task ids of the target bolts to which this component is connected to.
        this.targetTasks = targetTasks;
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {

        List<Integer> boltIds = new ArrayList<>();
        // Values contains the tuple to be sent.
        // Based on the bucket value we get a number which is less than the number of target tasks
        // using the % operator with the size of target tasks.
        // we then use the result as an index in our list of target tasks to pick a particular task
        // to forward this tuple to.
        Integer boltNumber = Integer.parseInt(values.get(1).toString()) % targetTasks.size();
        boltIds.add(targetTasks.get(boltNumber));

        return boltIds;
    }
}
