package com.alibaba.middleware.race.model;

/**
 * Created by hahong on 2016/7/8.
 */
public class OrderBalance {
    public int type;
    public double balance;
    public OrderBalance(int type, double balance) {
        this.type = type;
        this.balance = balance;
    }
}
