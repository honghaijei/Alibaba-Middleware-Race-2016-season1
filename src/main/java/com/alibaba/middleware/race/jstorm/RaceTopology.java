package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import ch.qos.logback.classic.Level;
import com.alibaba.middleware.race.RaceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import backtype.storm.LocalCluster;

import java.io.FileOutputStream;
import java.io.PrintStream;


/**
 * 这是一个很简单的例子
 * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
 * 我们会将选手拓扑杀掉。
 */

/**
 * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
 * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.RaceTopology；
 * 所以这个主类路径一定要正确
 */
public class RaceTopology {

    private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);

    public static void main(String[] args) throws Exception {
        if (RaceConfig.LOCAL && !RaceConfig.LOCAL_CLUSTER) {
            System.setOut(new PrintStream(new FileOutputStream("log_haijie.log")));
        }
        /*
        if (!RaceConfig.LOCAL) {
            ch.qos.logback.classic.Logger rootLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(
                    ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME
            );
            rootLogger.setLevel(Level.toLevel("error"));
        }
        */
        LOG.trace("test trace");
        LOG.debug("test debug");
        LOG.info("test info.");
        LOG.error("test error");


        Config conf = new Config();
        LocalCluster cluster = null;
        if (RaceConfig.LOCAL && !RaceConfig.LOCAL_CLUSTER) {
            cluster = new LocalCluster();
            //conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
        }
        int spout_Parallelism_hint = 1;
        int split_Parallelism_hint = 4;
        int count_Parallelism_hint = 1;

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RaceSentenceSpout(), spout_Parallelism_hint);
        builder.setBolt("split", new SplitSentence(), split_Parallelism_hint).fieldsGrouping("spout", "count", new Fields("platform"));
        builder.setBolt("count", new WordCount(), count_Parallelism_hint).shuffleGrouping("spout", "ratio");
        String topologyName = RaceConfig.JstormTopologyName;

        try {
            if (RaceConfig.LOCAL && !RaceConfig.LOCAL_CLUSTER) {
                cluster.submitTopology("SequenceTest", conf, builder.createTopology());
                Thread.sleep(300000);
                cluster.killTopology("SequenceTest");
                cluster.shutdown();
            } else {
                StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}