package com.itheima.demo1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FriendStep1Mapper extends Mapper<LongWritable,Text,Text,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //value格式 A:B,C,D,F,E,O
        String[] split = value.toString().split(":");
        String[] friends = split[1].split(",");
        for (String friend : friends) {
            context.write(new Text(friend),new Text(split[0]));
        }
    }
}
