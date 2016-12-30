package com.bqjr.storm.basic;

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
 * 基础例子
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
