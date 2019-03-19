package com.itheima.demo2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<PairSortBean,IntWritable,Text,IntWritable> {
    /**
     * 自定义计数器
     */
    public static enum Counter{
        REDUCE_INPUT_RECORDS,
        REDUCE_INPUT_VAL_NUMS,
    }
    @Override
    protected void reduce(PairSortBean key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        context.getCounter(Counter.REDUCE_INPUT_RECORDS).increment(1L);
        for (IntWritable value : values) {
            context.getCounter(Counter.REDUCE_INPUT_VAL_NUMS).increment(1L);
            context.write(new Text(key.getFirst()),value);
        }
    }
}
