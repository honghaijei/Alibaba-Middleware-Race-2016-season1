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
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import io.netty.util.internal.ConcurrentSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class RaceSentenceSpout implements IRichSpout {
    private static Logger LOG = LoggerFactory.getLogger(RaceSentenceSpout.class);
    SpoutOutputCollector _collector;
    Random _rand;
    long sendingCount;
    long startTime;
    boolean isStatEnable;
    int sendNumPerNexttuple;
    LinkedBlockingDeque<PaymentMessage> paymentMessagesQueue;
    ConcurrentHashMap<Long, Boolean> isTaobaoOrder;
    LRUFilter done;
    long recvCount = 0;
    long shootCount = 0;
    private static final String[] CHOICES = {"marry had a little lamb whos fleese was white as snow",
            "and every where that marry went the lamb was sure to go",
            "one two three four five six seven eight nine ten",
            "this is a test of the emergency broadcast system this is only a test",
            "peter piper picked a peck of pickeled peppers"};
    public class PaymentMessageKey {

        private final long x;
        private final long y;
        private final double z;

        public PaymentMessageKey(long x, long y, double z) {
            this.x = x;
            this.y = y;
            this.z = z;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof PaymentMessageKey)) return false;
            PaymentMessageKey key = (PaymentMessageKey) o;
            return x == key.x && y == key.y && z == key.z;
        }

        @Override
        public int hashCode() {
            int result = ((Long)x).hashCode() * 137 + ((Long)y).hashCode();
            result = result * 137 + ((Double)z).hashCode();
            return result;
        }

    }
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        LOG.info("open spout.");
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);

        //�ڱ��ش��broker��,�ǵ�ָ��nameServer�ĵ�ַ
        if (RaceConfig.DEBUG) {
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.setNamesrvAddr(RaceConfig.MqNamesrvAddr);
        }
        paymentMessagesQueue = new LinkedBlockingDeque<PaymentMessage>(100000000);
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
                                String str = msg.toString();
                                paymentMessagesQueue.put(paymentMessage);


                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        } else {
                            OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
                            //LOG.info("get " + orderMessage.toString());
                            if (msg.getTopic().equals(RaceConfig.MqTaobaoTradeTopic)) {
                                isTaobaoOrder.put(orderMessage.getOrderId(), true);
                                //LOG.info(orderMessage.getOrderId() + " is taobao id." + ", count="+(recvCount++));
                            } else {
                                isTaobaoOrder.put(orderMessage.getOrderId(), false);
                                //LOG.info(orderMessage.getOrderId() + " is tmall id." + ", count="+(recvCount++));
                            }
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
        _rand = new Random();
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
            if (!paymentMessagesQueue.isEmpty()) {
                PaymentMessage paymentMessage = null;
                try {
                    paymentMessage = paymentMessagesQueue.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Boolean isTaobao = isTaobaoOrder.get(paymentMessage.getOrderId());
                if (isTaobao == null) {
                    try {
                        paymentMessagesQueue.put(paymentMessage);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    String str = paymentMessage.toString();
                    _collector.emit("ratio", new Values((int) paymentMessage.getPayPlatform(), paymentMessage.getCreateTime(), paymentMessage.getPayAmount()));
                    //_collector.emit("count", new Values(isTaobao ? 0 : 1, paymentMessage.getCreateTime() / 1000 / 60, paymentMessage.getPayAmount()));

                    //LOG.info("shoot " + paymentMessage.toString() + " to " + (isTaobao ? "taobao" : "tmall") + ", count=" + (shootCount++));
                }
            } else {
                _collector.emit("count", new Values((int)0, (long)_rand.nextLong(), (double)-1.0));
                _collector.emit("count", new Values((int)1, (long)_rand.nextLong(), (double)-1.0));
                _collector.emit("ratio", new Values((int)0, (long)_rand.nextLong(), (double)-1.0));
                _collector.emit("ratio", new Values((int)1, (long)_rand.nextLong(), (double)-1.0));
                LOG.error("shoot end signal.");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
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
        declarer.declareStream("count", new Fields("platform", "minute", "amount"));
        declarer.declareStream("ratio", new Fields("platform", "timestamp", "amount"));
    }

    private void updateSendTps() {
        if (!isStatEnable)
            return;

        sendingCount++;
        long now = System.currentTimeMillis();
        long interval = now - startTime;
        if (interval > 60 * 1000) {
            LOG.info("Sending tps of last one minute is " + (sendingCount * sendNumPerNexttuple * 1000) / interval);
            startTime = now;
            sendingCount = 0;
        }
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