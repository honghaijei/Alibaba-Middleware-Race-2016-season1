package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
 * ��дtair����Ҫ�ļ�Ⱥ��Ϣ����masterConfigServer/slaveConfigServer��ַ/
 * group ��namespace���Ƕ�������ʽ�ύ����ǰ��֪ѡ��
 */
public class TairOperatorImpl {
    DefaultTairManager tairManager;
    int namespace;
    public TairOperatorImpl(String masterConfigServer,
                            String slaveConfigServer,
                            String groupName,
                            int namespace) {
        this.namespace = namespace;
        List<String> confServers = new ArrayList<String>();
        confServers.add(masterConfigServer);
        confServers.add(slaveConfigServer);
        tairManager = new DefaultTairManager();
        tairManager.setConfigServerList(confServers);
        // ��������
        tairManager.setGroupName(groupName);

        tairManager.init();
    }

    public boolean write(Serializable key, Serializable value) {
        ResultCode rc = tairManager.put(namespace, key, value);
        if (rc.isSuccess()) {
            return true;
        } else {
            return false;
        }
    }

    public Object get(Serializable key) {
        return tairManager.get(namespace, key).getValue().getValue();
    }

    public boolean remove(Serializable key) {
        return false;
    }

    public void close(){
    }

    //��è�ķ��ӽ��׶�д��tair
    public static void main(String [] args) throws Exception {
        TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
        //�������Ǹ���ʱ��
        Long millisTime = System.currentTimeMillis();
        //��������ʱ�����10λ����������Ҫת��������ʱ���
        Long minuteTime = (millisTime / 1000 / 60) * 60;
        //������һ���ӵĽ��׶���100;
        Double money = 100.0;
        //д��tair
        System.out.println(RaceConfig.prex_tmall + minuteTime);
        boolean success = tairOperator.write(RaceConfig.prex_tmall + minuteTime, money);
        System.out.println(success);
        System.out.println(tairOperator.get(RaceConfig.prex_tmall + minuteTime));
    }
}
