package com.bqjr.storm.basic;


import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * Created by hp on 2016/12/28.
 */
public class RandomSpout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    private Random random;
    private static String[] sentences = new String[] {"Japanese Designer Creates Solar-Powered Coat That Charges Gadgets","edi:I'm happy", "marry:I'm angry","Knowledge is a treasure but practice is the key to it."};
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
        random=new Random();
    }

    @Override
    public void nextTuple() {
        String sentence=sentences[random.nextInt(sentences.length)];
        this.spoutOutputCollector.emit(new Values(sentence));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }
}
