package com.itheima.demo6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class OrderMain extends Configured implements Tool {


    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), "group");
        //打包到线上运行，需要这一句
        job.setJarByClass(OrderMain.class);

        //第一步：读取文件
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("file:///F:\\每日资料\\hadoop\\Hadoop课程资料\\5、大数据离线第五天\\自定义groupingComparator\\input"));

        //第二步：自定义map逻辑
        job.setMapperClass(OrderMapper.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(Text.class);

        //第三步：分区  排序
        job.setPartitionerClass(OrderPartitioner.class);

        //第六步：分组
        job.setGroupingComparatorClass(MyGroupIngCompactor.class);

        //第七步：reduce阶段
        job.setReducerClass(OrderReducer.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(Text.class);

        //第八步：输出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("file:///F:\\每日资料\\hadoop\\Hadoop课程资料\\5、大数据离线第五天\\自定义groupingComparator\\output"));
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new OrderMain(), args);
        System.exit(run);
    }

}
