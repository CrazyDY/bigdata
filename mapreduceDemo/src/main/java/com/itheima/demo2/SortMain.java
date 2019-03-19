package com.itheima.demo2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
//        conf.set("mapreduce.framework.name","local");
        Job job = Job.getInstance(conf, SortMain.class.getName());

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("file:///F:\\每日资料\\hadoop\\Hadoop课程资料\\4、大数据离线第四天\\排序\\input"));

        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(PairSortBean.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("file:///F:\\每日资料\\hadoop\\Hadoop课程资料\\4、大数据离线第四天\\排序\\output_sort"));


        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new SortMain(), args);
        System.exit(run);
    }
}
