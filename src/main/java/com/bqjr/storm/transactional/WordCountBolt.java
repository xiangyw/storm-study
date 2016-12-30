package com.bqjr.storm.transactional;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by hp on 2016/12/28.
 */
public class WordCountBolt extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String word=tuple.getString(0);
        Integer count = counts.get(word);
        if (count == null){
            count = 0;
        }
        count++;
        counts.put(word, count);
        basicOutputCollector.emit(new Values(word, count));
        System.out.println("counts--->"+counts);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word","count"));
    }
}
