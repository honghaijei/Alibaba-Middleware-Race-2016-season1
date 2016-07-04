package com.alibaba.middleware.race;

import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.jstorm.SplitSentence;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.slf4j.Logger;


public class RaceUtils {
    /**
     * ���������ǽ���Ϣ����Kryo���л��󣬶ѻ���RocketMq������ѡ����Ҫ��metaQ��ȡ��Ϣ��
     * �����г���Ϣģ�ͣ�ֻҪ��Ϣģ�͵Ķ���������OrderMessage��PaymentMessage����
     * @param object
     * @return
     */
    public static byte[] writeKryoObject(Object object) {
        Output output = new Output(1024);
        Kryo kryo = new Kryo();
        kryo.writeObject(output, object);
        output.flush();
        output.close();
        byte [] ret = output.toBytes();
        output.clear();
        return ret;
    }

    public static <T> T readKryoObject(Class<T> tClass, byte[] bytes) {
        Kryo kryo = new Kryo();
        Input input = new Input(bytes);
        input.close();
        T ret = kryo.readObject(input, tClass);
        return ret;
    }
    public static void save(Logger LOG, TairOperatorImpl tairOperator, String key, double value) {
        boolean succ = tairOperator.write(key, value);
        if (succ) {
            LOG.info("Write to tair success, " + key + "\t" + value);
        } else {
            LOG.error("Write to tair error, " + key + "\t" + value);
        }
    }

}
