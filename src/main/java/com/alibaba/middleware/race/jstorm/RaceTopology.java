package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alibaba.middleware.race.RaceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import backtype.storm.LocalCluster;

import java.io.FileOutputStream;
import java.io.PrintStream;


/**
 * ����һ���ܼ򵥵�����
 * ѡ�ֵ������ύ����Ⱥ���������г�ʱ���õġ�ÿ��ѡ�ֵ����������20���ӣ�һ���������ʱ��
 * ���ǻὫѡ������ɱ����
 */

/**
 * ѡ����������࣬���Ƕ��������com.alibaba.middleware.race.jstorm.RaceTopology
 * ��Ϊ���Ǻ�̨��ѡ�ֵ�git�������ش�����������е������Ĭ����com.alibaba.middleware.race.jstorm.RaceTopology��
 * �����������·��һ��Ҫ��ȷ
 */
public class RaceTopology {

    private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);

    public static void main(String[] args) throws Exception {
        if (RaceConfig.DEBUG) {
            System.setOut(new PrintStream(new FileOutputStream("log_haijie.txt")));
        }


        Config conf = new Config();
        LocalCluster cluster = null;
        if (RaceConfig.DEBUG) {
            cluster = new LocalCluster();
            //conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
        }
        int spout_Parallelism_hint = 1;
        int split_Parallelism_hint = 4;
        int count_Parallelism_hint = 1;

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RaceSentenceSpout(), spout_Parallelism_hint);
        // builder.setBolt("split", new SplitSentence(), split_Parallelism_hint).shuffleGrouping("spout");
        builder.setBolt("split", new SplitSentence(), split_Parallelism_hint).fieldsGrouping("spout", "count", new Fields("platform"));
        builder.setBolt("count", new WordCount(), count_Parallelism_hint).fieldsGrouping("spout", "ratio", new Fields("timestamp"));
        String topologyName = RaceConfig.JstormTopologyName;

        try {
            if (RaceConfig.DEBUG) {
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