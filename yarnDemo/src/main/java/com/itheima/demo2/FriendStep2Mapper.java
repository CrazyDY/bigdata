package com.itheima.demo2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class FriendStep2Mapper extends Mapper<LongWritable,Text,Text,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //读取到的数据格式：F-D-O-I-H-B-K-G-C-	A
        String[] split = value.toString().split("\t");
        String[] friends = split[0].split("-");
        Arrays.sort(friends);
        for (int i = 0; i < friends.length - 1 ; i++) {
            for (int j = i+1; j < friends.length; j++) {
                context.write(new Text(friends[i]+"-"+friends[j]),new Text(split[1]));
            }
        }
    }
}
