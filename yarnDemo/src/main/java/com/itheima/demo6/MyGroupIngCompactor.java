package com.itheima.demo6;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupIngCompactor extends WritableComparator {
    public MyGroupIngCompactor(){
        super(OrderBean.class,true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean first = (OrderBean) a;
        OrderBean second = (OrderBean) b;
        return first.getOrderId().compareTo(second.getOrderId());

    }
}
