package com.itheima.demo6;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartitioner extends Partitioner<OrderBean,Text>{
    @Override
    public int getPartition(OrderBean orderBean, Text text, int numReduceTasks) {
        return (orderBean.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
