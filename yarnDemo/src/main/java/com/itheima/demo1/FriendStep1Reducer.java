package com.itheima.demo1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FriendStep1Reducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //接收到的数据格式：A <B,C,D....>
        StringBuffer buffer = new StringBuffer();
        for (Text value : values) {
           buffer.append(value.toString()).append("-");
        }
        context.write(new Text(buffer.toString()),key);
    }
}
