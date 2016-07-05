package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hahong on 2016/7/3.
 */


public class ClassifyPlatform implements IBasicBolt {
    private static Logger LOG = LoggerFactory.getLogger(ClassifyPlatform.class);
    Map<Long, Integer> orderType = new HashMap<Long, Integer>(100000);
    Map<Long, List<Tuple>> paymentCache = new HashMap<Long, List<Tuple>>(100000);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        long orderId = tuple.getLong(0);
        int platform = tuple.getInteger(1);
        long minute = tuple.getLong(2);
        double amount = tuple.getDouble(3);
        //LOG.info(String.format("receive tuple, orderId: %d, platform: %d, minute: %d, amount: %f", orderId, platform, minute, amount));
        if (platform != -1) {
            orderType.put(orderId, platform);
            List<Tuple> left = paymentCache.get(orderId);
            if (left != null) {
                for (Tuple e : left) {
                    basicOutputCollector.emit(new Values(platform, e.getLong(2), e.getDouble(3)));
                }
                left.clear();
            }
        } else {
            Integer type = orderType.get(orderId);
            if (type == null) {
                List<Tuple> left = paymentCache.get(orderId);
                if (left == null) {
                    left = new ArrayList<Tuple>();
                    left.add(tuple);
                    paymentCache.put(orderId, left);
                } else {
                    left.add(tuple);
                }
            } else {
                basicOutputCollector.emit(new Values((int)type, minute, amount));
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("platform", "minute", "amount"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        orderType.put(-1L, 0);
        orderType.put(-2L, 1);
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}
