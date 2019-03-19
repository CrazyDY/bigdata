package com.itheima.demo4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowSortMapper extends Mapper<LongWritable,Text,FlowSortBean,Text> {
    FlowSortBean flowSortBean = new FlowSortBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split("\t");
        flowSortBean.setUpFlow(Integer.valueOf(split[1]));
        flowSortBean.setDownFlow(Integer.valueOf(split[2]));
        flowSortBean.setUpCountFlow(Integer.valueOf(split[3]));
        flowSortBean.setDownCountFlow(Integer.valueOf(split[4]));
        context.write(flowSortBean,new Text(split[0]));
    }
}
