package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import ch.qos.logback.classic.Level;
import com.alibaba.middleware.race.MiddlewareRaceConfig;
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
        if (MiddlewareRaceConfig.LOCAL && !MiddlewareRaceConfig.LOCAL_CLUSTER) {
            System.setOut(new PrintStream(new FileOutputStream("log_haijie.log")));
        }

        if (!MiddlewareRaceConfig.LOCAL) {
            ch.qos.logback.classic.Logger rootLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(
                    ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME
            );
            rootLogger.setLevel(Level.toLevel("error"));
        }


        Config conf = new Config();

        LocalCluster cluster = null;
        if (MiddlewareRaceConfig.LOCAL && !MiddlewareRaceConfig.LOCAL_CLUSTER) {
            cluster = new LocalCluster();
            //conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
        }
        if (MiddlewareRaceConfig.LOCAL_CLUSTER || !MiddlewareRaceConfig.LOCAL) {
            conf.setNumWorkers(4);
        }

        conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
        conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);

//        conf.put("storm.messaging.netty.transfer.async.batch", true);
        conf.put(Config.STORM_NETTY_MESSAGE_BATCH_SIZE, 262144);
        Config.setNumAckers(conf, 0);
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RaceMessagePollSpout(), 4);
        builder.setBolt("classify", new ClassifyPlatform(), 4).fieldsGrouping("spout", new Fields("orderId"));
        builder.setBolt("minute_counter", new MessageCounter(), 2).fieldsGrouping("classify", "count", new Fields("platform"));
        builder.setBolt("ratio_counter", new RatioCounter(), 1).shuffleGrouping("classify", "ratio");
        TairWriter tairWriter = new TairWriter();
        builder.setBolt("tair_writer", tairWriter, 1).shuffleGrouping("minute_counter").shuffleGrouping("ratio_counter");
        String topologyName = MiddlewareRaceConfig.JstormTopologyName;
        try {
            if (MiddlewareRaceConfig.LOCAL && !MiddlewareRaceConfig.LOCAL_CLUSTER) {
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