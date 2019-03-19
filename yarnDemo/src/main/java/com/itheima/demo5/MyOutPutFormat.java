package com.itheima.demo5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyOutPutFormat extends FileOutputFormat<Text,NullWritable> {

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Configuration configuration = taskAttemptContext.getConfiguration();
        FileSystem fileSystem = FileSystem.get(configuration);
        FSDataOutputStream goodComment = fileSystem.create(new Path("file:///F:\\每日资料\\hadoop\\Hadoop课程资料\\5、大数据离线第五天\\自定义outputformat\\good_comment\\good_comment.txt"));
        FSDataOutputStream badComment = fileSystem.create(new Path("file:///F:\\每日资料\\hadoop\\Hadoop课程资料\\5、大数据离线第五天\\自定义outputformat\\bad_comment\\bad_comment.txt"));
        MyRecordWriter myRecordWriter = new MyRecordWriter(goodComment, badComment);
        return myRecordWriter;
    }
}
