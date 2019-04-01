package com.itheima;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MovieConutMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // value 河谷镇第二季,2018-03-30
        String[] split = value.toString().split(",");
        String[] date = split[1].split("-");
        context.write(new Text(date[0]),new IntWritable(1));
    }
}
