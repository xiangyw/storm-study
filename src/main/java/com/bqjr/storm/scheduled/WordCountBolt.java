package com.bqjr.storm.scheduled;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by hp on 2016/12/28.
 * 定时的bolt，通过重写getComponentConfiguration 配置定时任务间隔；
 * 我的理解是：按时间间隔定时emit特定tuple，execute接收到指定tuple就开始执行定时任务
 */
public class WordCountBolt extends BaseBasicBolt {
    static Map<String, Integer> counts = new ConcurrentHashMap<String, Integer>();
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if(tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) &&
                tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
            System.out.println(new Date()+"###counts------------------------------->db"+counts);
            return;
        }else{
            String word=tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null){
                count = 0;
            }
            count++;
            counts.put(word, count);
            basicOutputCollector.emit(new Values(word, count));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word","count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
        return conf;
    }
}
