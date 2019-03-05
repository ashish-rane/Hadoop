package com.xpert.storm.trident.joinmerge;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class AddFunction extends BaseFunction {
    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        tridentCollector.emit(new Values(Integer.parseInt(tridentTuple.getString(0)) +
                                            Integer.parseInt(tridentTuple.getString(1))));
    }
}
