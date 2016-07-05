package com.alibaba.middleware.race.jstorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.LRUFilter;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

public class RaceMessageSpout implements IRichSpout {
    private static Logger LOG = LoggerFactory.getLogger(RaceMessageSpout.class);
    SpoutOutputCollector _collector;
    Random _rand;
    long sendingCount;
    long startTime;
    boolean isStatEnable;
    int sendNumPerNexttuple;
    LinkedBlockingDeque<PaymentMessage> paymentMessagesQueue;
    LinkedBlockingDeque<OrderMessage> orderMessagesQueue;
    ConcurrentHashMap<Long, Boolean> isTaobaoOrder;
    LRUFilter done;
    long recvCount = 0;
    long shootCount = 0;
    long lastEndSignal = System.currentTimeMillis();
    private static final String[] CHOICES = {"marry had a little lamb whos fleese was white as snow",
            "and every where that marry went the lamb was sure to go",
            "one two three four five six seven eight nine ten",
            "this is a test of the emergency broadcast system this is only a test",
            "peter piper picked a peck of pickeled peppers"};

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        LOG.info("open spout.");
        TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
        tairOperator.write("start_flag", 0);
        _rand = new Random();
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RaceConfig.LOCAL ? ((Long)_rand.nextLong()).toString() : RaceConfig.MetaConsumerGroup);

        if (RaceConfig.LOCAL) {
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.setNamesrvAddr(RaceConfig.MqNamesrvAddr);
        }
        paymentMessagesQueue = new LinkedBlockingDeque<PaymentMessage>(200000);
        orderMessagesQueue = new LinkedBlockingDeque<OrderMessage>(200000);
        isTaobaoOrder = new ConcurrentHashMap<Long, Boolean>();
        done = new LRUFilter(10000);
        try {
            consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
            consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
            consumer.subscribe(RaceConfig.MqPayTopic, "*");

            consumer.registerMessageListener(new MessageListenerConcurrently() {

                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                                ConsumeConcurrentlyContext context) {
                    for (MessageExt msg : msgs) {

                        byte[] body = msg.getBody();
                        if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                            continue;
                        }
                        if (msg.getTopic().equals(RaceConfig.MqPayTopic)) {
                            PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
                            //LOG.info("get " + paymentMessage.toString() + ", count="+(recvCount++));
                            try {
                                paymentMessagesQueue.put(paymentMessage);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        } else {
                            OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
                            try {
                                orderMessage.setBuyerId(msg.getTopic());
                                orderMessagesQueue.put(orderMessage);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            //LOG.info("get " + orderMessage.toString());
                        }
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            consumer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }

        _collector = collector;

        sendingCount = 0;
        startTime = System.currentTimeMillis();
        sendNumPerNexttuple = JStormUtils.parseInt(conf.get("send.num.each.time"), 1);
        isStatEnable = JStormUtils.parseBoolean(conf.get("is.stat.enable"), false);
    }

    @Override
    public void nextTuple() {
        //int n = sendNumPerNexttuple;
        int n = 10000;
        while (--n >= 0) {

            if (!orderMessagesQueue.isEmpty()) {
                OrderMessage orderMessages = null;
                try {
                    orderMessages = orderMessagesQueue.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                int platform = orderMessages.getBuyerId().equals(RaceConfig.MqTaobaoTradeTopic) ? 0 : 1;
                _collector.emit("count", new Values(orderMessages.getOrderId(), platform, -1L, -1.0));
                continue;
            }
            if (!paymentMessagesQueue.isEmpty()) {
                PaymentMessage paymentMessages = null;
                try {
                    paymentMessages = paymentMessagesQueue.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                _collector.emit("count", new Values(paymentMessages.getOrderId(), -1, paymentMessages.getCreateTime() / 1000 / 60, paymentMessages.getPayAmount()));
                _collector.emit("ratio", new Values((int)paymentMessages.getPayPlatform(), paymentMessages.getCreateTime() / 1000 / 60, paymentMessages.getPayAmount()));
                continue;
            }
            if (System.currentTimeMillis() - lastEndSignal > 30000) {
                _collector.emit("count", new Values(-1L, -1, _rand.nextLong(),  -1.0));
                _collector.emit("count", new Values(-2L, -1, _rand.nextLong(),  -1.0));
                _collector.emit("ratio", new Values((int) 0,  _rand.nextLong(),  -1.0));
                _collector.emit("ratio", new Values((int) 1,  _rand.nextLong(),  -1.0));
                LOG.error("shoot end signal.");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                lastEndSignal = System.currentTimeMillis();
            }


            //String sentence = CHOICES[_rand.nextInt(CHOICES.length)];
            //_collector.emit(new Values(sentence));
        }
        //updateSendTps();
    }

    @Override
    public void ack(Object id) {
        // Ignored
    }

    @Override
    public void fail(Object id) {
        _collector.emit(new Values(id), id);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declarer.declare(new Fields("platform", "timestamp", "amount"));
        declarer.declareStream("count", new Fields("orderId", "platform", "minute", "amount"));
        declarer.declareStream("ratio", new Fields("platform", "minute", "amount"));
    }


    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    public void activate() {
        // TODO Auto-generated method stub

    }

    @Override
    public void deactivate() {
        // TODO Auto-generated method stub

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}