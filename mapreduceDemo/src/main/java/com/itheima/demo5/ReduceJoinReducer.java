package com.itheima.demo5;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceJoinReducer extends Reducer<Text,Text,Text,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String firstPart = "";
        String secondPard = "";
        for (Text value : values) {
            if (value.toString().startsWith("p")) {
                firstPart = value.toString();
            } else {
                secondPard = value.toString();
            }
        }
        context.write(new Text(firstPart+"\t"+secondPard),NullWritable.get());
    }
}
