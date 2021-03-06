package com.itheima.demo5;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReduceJoinMapper extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        if (s.startsWith("p")) {
            String[] split = s.split(",");
            context.write(new Text(split[0]), value);
        } else {
            String[] split = s.split(",");
            context.write(new Text(split[2]),value);
        }
    }
}
