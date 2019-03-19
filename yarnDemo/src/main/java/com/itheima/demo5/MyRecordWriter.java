package com.itheima.demo5;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class MyRecordWriter extends RecordWriter<Text,NullWritable> {
    private FSDataOutputStream goodComment;
    private FSDataOutputStream badComment;

    public MyRecordWriter() {
    }

    public MyRecordWriter(FSDataOutputStream goodComment, FSDataOutputStream badComment) {
        this.goodComment = goodComment;
        this.badComment = badComment;
    }

    /**
     * 将我们的数据往外写
     * @param text  我们一行的评论数据
     * @param nullWritable
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        //1	2018-03-15 22:29:06	2018-03-15 22:29:06	我想再来一个	\N	1	3	hello	来就来吧	0	2018-03-14 22:29:03
        String[] split = text.toString().split("\t");
        String commentStatus = split[9];
        if (Integer.valueOf(commentStatus) <= 1) {//好评
            goodComment.write(text.toString().getBytes());
            goodComment.write("\r\n".getBytes());
        } else {//中评或差评
            badComment.write(text.toString().getBytes());
            badComment.write("\r\n".getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(goodComment);
        IOUtils.closeStream(badComment);
    }
}
