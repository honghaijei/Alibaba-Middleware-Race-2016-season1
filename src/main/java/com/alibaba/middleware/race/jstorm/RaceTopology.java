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
        if (RaceConfig.LOCAL && !RaceConfig.LOCAL_CLUSTER) {
            System.setOut(new PrintStream(new FileOutputStream("log_haijie.log")));
        }

        if (!RaceConfig.LOCAL) {
            ch.qos.logback.classic.Logger rootLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(
                    ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME
            );
            rootLogger.setLevel(Level.toLevel("error"));
        }

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
        if (RaceConfig.LOCAL_CLUSTER || !RaceConfig.LOCAL) {
            conf.setNumWorkers(1);
        }
        /*
        conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
        conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
        conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
        conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
*/
//        conf.put("storm.messaging.netty.transfer.async.batch", true);
        conf.put(Config.STORM_NETTY_MESSAGE_BATCH_SIZE, 262144);
        conf.put("worker.cpu.slot.num", 6);
        Config.setNumAckers(conf, 0);
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RaceMessageSpout(), 1);
        builder.setBolt("classify", new ClassifyPlatform(), 4).fieldsGrouping("spout", "count", new Fields("orderId"));
        builder.setBolt("split", new MessageCounter(), 2).fieldsGrouping("classify", new Fields("platform"));
        builder.setBolt("count", new RatioCount(), 1).shuffleGrouping("spout", "ratio");
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