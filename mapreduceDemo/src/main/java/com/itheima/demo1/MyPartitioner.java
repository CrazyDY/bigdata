package com.itheima.demo1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text,NullWritable> {
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {
        String result = text.toString().split("\t")[5];
        System.out.println("result = " + result);
        if (Integer.parseInt(result) > 15) {
            return 1;
        } else {
            return 0;
        }
    }
}
