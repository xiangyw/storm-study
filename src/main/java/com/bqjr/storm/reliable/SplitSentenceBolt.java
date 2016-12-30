package com.bqjr.storm.reliable;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by hp on 2016/12/28.
 */
public class SplitSentenceBolt extends BaseRichBolt {
    OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        boolean exception=true;
        try{
            //做数据处理
            String sentence=tuple.getString(0);
            for (String word: sentence.split("\\s+")) {
            /*
            每个单词元组是通过把输入的元组作为emit函数中的第一个参数来做锚定的。通过锚定，Storm就能够得到元组之间的关联关系(输入元组触发了新的元组)，继而构建出Spout元组触发的整个消息树。所以当下游处理失败时，就可以通知Spout当前消息树根节点的Spout元组处理失败，让Spout重新处理。相反，如果在emit的时候没有指定输入的元组，叫做不锚定：
            outputCollector.emit(new Values(word));
            一个输出的元组可以被锚定到多个输入元组上，叫做多锚定(multi-anchoring)。这在做流的合并或者聚合的时候非常有用。一个多锚定的元组处理失败，会导致Spout上重新处理对应的多个输入元组。多锚定是通过指定一个多个输入元组的列表而不是单个元组来完成的。例如：

            List<Tuple> anchors = new ArrayList<Tuple>();
            anchors.add(tuple1);
            anchors.add(tuple2);
            outputCollector.emit(anchors, new Values(word));
            多锚定会把这个新输出的元组添加到多棵消息树上。注意多锚定可能会打破消息的树形结构，变成有向无环图(DAG)，Storm的实现既支持树形结构，也支持有向无环图(DAG)。
            */
                outputCollector.emit(tuple,new Values(word, 1));//2.锚定(anchoring)
            }
            if(exception){
                throw new Exception("数据处理异常 fail tuple");
            }
            outputCollector.ack(tuple);//3.确认
        }catch(Exception e){
            outputCollector.fail(tuple);//失败确认
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word","count"));
    }
}
