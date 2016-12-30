package com.bqjr.storm.scheduled;

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
 * 定时任务
 */
public class WordCountTopology {
    public static void main(String args[]) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder=new TopologyBuilder();
        //spout 是数据发射源
        builder.setSpout("RandomSpout",new RandomSpout());
        //bolt 是数据处理者
        builder.setBolt("SplitSentenceBolt",new SplitSentenceBolt(),12).shuffleGrouping("RandomSpout");
        /*流分组策略----Stream Grouping
        Stream Grouping，告诉topology如何在两个组件之间发送tuple
        定义一个topology的其中一步是定义每个bolt接收什么样的流作为输入。stream grouping就是用来定义一个stream应该如果分配数据给bolts上面的多个tasks
        Storm里面有7种类型的stream grouping，你也可以通过实现CustomStreamGrouping接口来实现自定义流分组
        1. Shuffle Grouping
        随机分组，随机派发stream里面的tuple，保证每个bolt task接收到的tuple数目大致相同。
        2. Fields Grouping
        按字段分组，比如，按"user-id"这个字段来分组，那么具有同样"user-id"的 tuple 会被分到相同的Bolt里的一个task， 而不同的"user-id"则可能会被分配到不同的task。
        3. All Grouping
        广播发送，对亍每一个tuple，所有的bolts都会收到
        4. Global Grouping
        全局分组，整个stream被分配到storm中的一个bolt的其中一个task。再具体一点就是分配给id值最低的那个task。
        5. None Grouping
        不分组，这个分组的意思是说stream不关心到底怎样分组。目前这种分组和Shuffle grouping是一样的效果， 有一点不同的是storm会把使用none grouping的这个bolt放到这个bolt的订阅者同一个线程里面去执行（如果可能的话）。
        6. Direct Grouping
        指向型分组， 这是一种比较特别的分组方法，用这种分组意味着消息（tuple）的发送者指定由消息接收者的哪个task处理这个消息。只有被声明为 Direct Stream 的消息流可以声明这种分组方法。而且这种消息tuple必须使用 emitDirect 方法来发射。消息处理者可以通过 TopologyContext 来获取处理它的消息的task的id (OutputCollector.emit方法也会返回task的id)
        7. Local or shuffle grouping
        本地或随机分组。如果目标bolt有一个或者多个task与源bolt的task在同一个工作进程中，tuple将会被随机发送给这些同进程中的tasks。否则，和普通的Shuffle Grouping行为一致。
        */
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
            Utils.sleep(60*1000);
            cluster.killTopology(name);
            cluster.shutdown();
        }
    }
}
