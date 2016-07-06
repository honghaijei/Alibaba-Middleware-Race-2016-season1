package com.alibaba.middleware.race.jstorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.MiddlewareRaceConfig;
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
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class RaceMessagePollSpout implements IRichSpout {
    private static Logger LOG = LoggerFactory.getLogger(RaceMessagePollSpout.class);
    SpoutOutputCollector _collector;
    Random _rand;
    long sendingCount;
    long startTime;
    boolean isStatEnable;
    int sendNumPerNexttuple;
    LinkedBlockingDeque<MessageExt> messagesQueue;
    long recvCount = 0;
    long shootCount = 0;
    boolean timelimitExceed = false;
    long lastEndSignal = System.currentTimeMillis();
    private static final String[] CHOICES = {"marry had a little lamb whos fleese was white as snow",
            "and every where that marry went the lamb was sure to go",
            "one two three four five six seven eight nine ten",
            "this is a test of the emergency broadcast system this is only a test",
            "peter piper picked a peck of pickeled peppers"};

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        LOG.info("open spout.");
        TairOperatorImpl tairOperator = new TairOperatorImpl(MiddlewareRaceConfig.TairConfigServer, MiddlewareRaceConfig.TairSalveConfigServer,
                MiddlewareRaceConfig.TairGroup, MiddlewareRaceConfig.TairNamespace);
        tairOperator.write("start_flag", 0);
        _rand = new Random();
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(MiddlewareRaceConfig.LOCAL ?"789" : MiddlewareRaceConfig.MetaConsumerGroup);

        if (MiddlewareRaceConfig.LOCAL) {
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.setNamesrvAddr(MiddlewareRaceConfig.MqNamesrvAddr);
        }

        consumer.setConsumeMessageBatchMaxSize(128);
        consumer.setMessageModel(MessageModel.CLUSTERING);
        messagesQueue = new LinkedBlockingDeque<MessageExt>(50000);
        try {
            consumer.subscribe(MiddlewareRaceConfig.MqTaobaoTradeTopic, "*");
            consumer.subscribe(MiddlewareRaceConfig.MqTmallTradeTopic, "*");
            consumer.subscribe(MiddlewareRaceConfig.MqPayTopic, "*");

            consumer.registerMessageListener(new MessageListenerConcurrently() {

                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                                ConsumeConcurrentlyContext context) {
                    for (MessageExt msg : msgs) {
                        byte[] body = msg.getBody();
                        if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                            continue;
                        }
                        try {
                            messagesQueue.put(msg);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
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
        int n = 1;
        while (--n >= 0) {
            try {
                MessageExt msg = messagesQueue.poll(5, TimeUnit.SECONDS);
                if (msg != null) {
                    if (msg.getTopic().equals(MiddlewareRaceConfig.MqPayTopic)) {
                        PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, msg.getBody());
                        //LOG.info("get " + paymentMessage.toString() + ", count="+(recvCount++));
                        _collector.emit(new Values(paymentMessage.getOrderId(), -1, paymentMessage.getCreateTime() / 1000 / 60, paymentMessage.getPayAmount(), msg.getMsgId(), (int)paymentMessage.getPayPlatform()));
                        continue;
                    } else {
                        OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, msg.getBody());
                        int platform = msg.getTopic().equals(MiddlewareRaceConfig.MqTaobaoTradeTopic) ? 0 : 1;
                        _collector.emit(new Values(orderMessage.getOrderId(), platform, -1L, -1.0, "", -1));
                    }
                    continue;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (System.currentTimeMillis() - startTime > 18 * 60000 && !timelimitExceed) {
                timelimitExceed = true;
                _collector.emit( new Values(-1L, -1, _rand.nextLong(),  -1.0, "", -1));
                _collector.emit( new Values(-2L, -1, _rand.nextLong(),  -1.0, "", -1));
                LOG.error("shoot end signal.");
                lastEndSignal = System.currentTimeMillis();
            }
            if (System.currentTimeMillis() - lastEndSignal > 15000) {
                _collector.emit(new Values(-1L, -1, _rand.nextLong(),  -1.0, "", -1));
                _collector.emit(new Values(-2L, -1, _rand.nextLong(),  -1.0, "", -1));
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
        declarer.declare(new Fields("orderId", "platform", "minute", "amount", "messageId", "type"));
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