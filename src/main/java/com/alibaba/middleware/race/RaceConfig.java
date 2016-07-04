package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {
    public static boolean LOCAL = false;
    public static boolean LOCAL_CLUSTER = false;
    //��Щ��дtair key��ǰ׺
    public static String prex_tmall = "platformTmall_";
    public static String prex_taobao = "platformTaobao_";
    public static String prex_ratio = "ratio_";
    public static String team_code = "3509496lg7_";

    //��Щjstorm/rocketMq/tair �ļ�Ⱥ������Ϣ����Щ������Ϣ����ʽ�ύ����ǰ�ᱻ����
    public static String JstormTopologyName = "3509496lg7";
    public static String MetaConsumerGroup = "3509496lg7";
    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";
    public static String MqNamesrvAddr = "10.60.45.78:9876";
    public static String TairConfigServer = LOCAL ? "10.60.45.77:5198" : "10.101.72.127:5198";
    public static String TairSalveConfigServer = LOCAL ? "10.60.45.77:5198" : "10.101.72.128:5198";
    public static String TairGroup = LOCAL ? "group_1" : "group_tianchi";
    public static Integer TairNamespace = LOCAL ? 0 : 31943;
}
