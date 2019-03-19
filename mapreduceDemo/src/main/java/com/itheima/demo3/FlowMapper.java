package com.itheima.demo3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean> {
    FlowBean flowBean = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] split = s.split("\t");
        flowBean.setUpFlow(Integer.valueOf(split[6]));
        flowBean.setDownFlow(Integer.valueOf(split[7]));
        flowBean.setUpCountFlow(Integer.valueOf(split[8]));
        flowBean.setDownCountFlow(Integer.valueOf(split[9]));
        context.write(new Text(split[1]),flowBean);
    }
}
