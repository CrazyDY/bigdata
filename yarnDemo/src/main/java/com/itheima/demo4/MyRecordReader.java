package com.itheima.demo4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MyRecordReader extends RecordReader<NullWritable,BytesWritable> {
    private FileSplit fileSplit;
    private Configuration configuration;
    private BytesWritable bytesWritable = new BytesWritable();
    private boolean processed = false;
    /**
     * 初始化调用一次
     *
     * @param inputSplit  文件的切片  文件的内容都在这个切片里面
     * @param taskAttemptContext  上下文对象  可以获取我们的一些配置信息等
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) inputSplit;
        this.configuration = taskAttemptContext.getConfiguration();
    }

    /**
     *这个方法就是我们去读取文件时候使用的
     * 如果读取完成，返回true，表示我们已经读取完成
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!processed) {
            //在这个方法里面读取我们文件的切片，然后将我们的数据都读取出来，封装到BytesWritable里面去
            //javaSE的时候可不可以把一个文件的内容全部读取到  byte[]
            byte[] bytes = new byte[(int) fileSplit.getLength()];
            FileSystem fileSystem = FileSystem.get(configuration);
            FSDataInputStream inputStream = fileSystem.open(fileSplit.getPath());
            IOUtils.readFully(inputStream,bytes,0,bytes.length);
            //将我们的字节数组的内容全部塞到BytesWritable里面去
            bytesWritable.set(bytes,0,bytes.length);
            IOUtils.closeStream(inputStream);
            fileSystem.close();
            processed = true;
            return true;
        }
        return false;
    }

    /**
     * 这个方法返回的是 我们的k1
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    /**
     * 这个方法里面返回我们的v1
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }


    /**
     * 获取我们文件的读取进度
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    /**
     * 关闭的
     * @throws IOException
     */
    @Override
    public void close() throws IOException {

    }
}
