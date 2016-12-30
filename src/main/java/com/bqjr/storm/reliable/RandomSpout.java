package com.bqjr.storm.reliable;


import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * Created by hp on 2016/12/28.
 */
public class RandomSpout extends BaseRichSpout {
    //模拟消息缓存
    private static HashMap<Object,Object> msgCache=new HashMap<Object,Object>();
    private static boolean first =true;
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
        String sentence="";
        if(first){
            sentence="i am first tuple";
            first=false;
        }else{
            sentence=sentences[random.nextInt(sentences.length)];
        }
        Object msgId=UUID.randomUUID();
        this.spoutOutputCollector.emit(new Values(sentence), msgId);//1.指定msgId
        msgCache.put(msgId,new Values(sentence));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }

    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
        msgCache.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
        this.spoutOutputCollector.emit((Values)msgCache.get(msgId), msgId);//1.指定msgId
    }
}
