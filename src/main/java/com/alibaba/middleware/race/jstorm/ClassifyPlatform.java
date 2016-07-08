package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.LRUMap;
import com.alibaba.middleware.race.model.OrderBalance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by hahong on 2016/7/3.
 */


public class ClassifyPlatform implements IBasicBolt {
    private static Logger LOG = LoggerFactory.getLogger(ClassifyPlatform.class);
    Map<Long, OrderBalance> orderType = new HashMap<Long, OrderBalance>(100000);
    Map<Long, List<Tuple>> paymentCache = new HashMap<Long, List<Tuple>>(100000);
    LRUMap<String, Boolean> msgIdset = new LRUMap<String, Boolean>(100000);
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        long orderId = tuple.getLong(0);
        int platform = tuple.getInteger(1);
        long minute = tuple.getLong(2);
        double amount = tuple.getDouble(3);
        String messageId = tuple.getString(4);
        int payPlatform = tuple.getInteger(5);
        if (messageId != "") {
            if (msgIdset.containsKey(messageId)) {
                return;
            } else {
                msgIdset.put(messageId, true);
            }
        }
        //LOG.info(String.format("receive tuple, orderId: %d, platform: %d, minute: %d, amount: %f", orderId, platform, minute, amount));
        if (platform != -1) {
            List<Tuple> left = paymentCache.get(orderId);
            if (left != null) {
                for (Tuple e : left) {
                    basicOutputCollector.emit("count", new Values(platform, e.getLong(2), e.getDouble(3)));
                    amount -= e.getDouble(3);
                }
                left.clear();
            }
            if (Math.abs(amount) > 0.001) {
                orderType.put(orderId, new OrderBalance(platform, amount));
            }
        } else {
            basicOutputCollector.emit("ratio", new Values((int) payPlatform, minute, amount));
            OrderBalance entry = orderType.get(orderId);

            if (entry == null) {
                List<Tuple> left = paymentCache.get(orderId);
                if (left == null) {
                    left = new ArrayList<Tuple>();
                    left.add(tuple);
                    paymentCache.put(orderId, left);
                } else {
                    left.add(tuple);
                }
            } else {
                int type = entry.type;
                double balance = entry.balance - amount;
                basicOutputCollector.emit("count", new Values((int)type, minute, amount));
                if (Math.abs(balance) < 0.001) {
                    orderType.remove(orderId);
                }
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("count", new Fields("platform", "minute", "amount"));
        declarer.declareStream("ratio", new Fields("platform", "minute", "amount"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        orderType.put(-1L, new OrderBalance(0, 1.0));
        orderType.put(-2L, new OrderBalance(1, 1.0));
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
