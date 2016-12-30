package com.bqjr.storm.reliable;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * Created by hp on 2016/12/28.
 * 消息可靠性
 * 有三种方法来去掉可靠性：

 设置conf.setNumAckers(0)。这种情况下，Storm会在Spout吐出一个元组后立马调用Spout的ack函数。这个元组树不会被跟踪。
 当产生一个新元组调用emit函数的时候通过忽略消息message-id参数来关闭这个元组的跟踪机制。
 如果你不关心某一类特定的元组处理失败的情况，可以在调用emit的时候不要使用锚定。由于它们没有被锚定到某个Spout元组上，所以当它们没有被成功处理，不会导致Spout元组处理失败。
 */
public class WordCountTopology {
    public static void main(String args[]) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder=new TopologyBuilder();
        //spout 是数据发射源
        builder.setSpout("RandomSpout",new RandomSpout());
        //bolt 是数据处理者
        builder.setBolt("SplitSentenceBolt",new SplitSentenceBolt(),12).shuffleGrouping("RandomSpout");
        builder.setBolt("WordCountBolt",new WordCountBolt(),12).fieldsGrouping("SplitSentenceBolt", new Fields("word","count"));
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumAckers(1);//ack 线程
        String name = "word_count_test_";
        if (args != null && args.length > 0) {
            //集群模式，是通过storm jar命令提交运行
            name = name+"cluster";
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
        } else {
            //本地模式，主要用于开发和测试
            name = name+"Local";
            conf.setNumWorkers(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name, conf, builder.createTopology());
            Utils.sleep(100000);
            cluster.killTopology(name);
            cluster.shutdown();
        }
    }
}
