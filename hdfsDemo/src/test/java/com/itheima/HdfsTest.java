package com.itheima;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class HdfsTest {

    @Test
    public void copyFileFromHdfs() throws Exception {
        //第一步：注册hdfs 的url，让java代码能够识别hdfs的url形式
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        //定义文件访问的url地址
        InputStream inputStream = new URL("hdfs://node01:8020/for.sh").openStream();
        //打开文件输入流
        FileOutputStream outputStream = new FileOutputStream("E://aaa.txt");
        IOUtils.copy(inputStream,outputStream);
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
    }
    @Test
    public void getFileSystem() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), conf);
        System.out.println(fileSystem.toString());
    }
    @Test
    public void getFileSystem2() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        System.out.println(fileSystem.toString());

    }
    @Test
    public void getFileSystem3() throws URISyntaxException, IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://node01:8082"), configuration);
        System.out.println(fileSystem.toString());
    }
    @Test
    public void getFileSystem4() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.newInstance(configuration);
        System.out.println("fileSystem = " + fileSystem);
    }

    /**
     * 递归遍历HDFS文件系统
     */
    @Test
    public void listFiles() throws URISyntaxException, IOException {
        //获取HDFS文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        //获取指定目录下的文件列表
        FileStatus[] listStatus = fileSystem.listStatus(new Path("/"));
        //遍历文件列表
        for (FileStatus status : listStatus) {
            if (status.isDirectory()) {
                Path path = status.getPath();
                listDirectoy(fileSystem, path);
            } else {
                System.out.println("文件路径为："+status.getPath().toString());
            }
        }
    }

    private void listDirectoy(FileSystem fileSystem, Path path) throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(path);
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isDirectory()) {
                listDirectoy(fileSystem,fileStatus.getPath());
            } else {
                System.out.println("文件路径为："+fileStatus.getPath().toString());
            }

        }
    }

    /**
     * 通过官方API递归遍历指定文件
     */

    @Test
    public void listFiles2() throws URISyntaxException, IOException {
        //获取HDFS文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        //获取RemoteIterator 得到所有的文件或者文件夹，第一个参数指定遍历的路径，第二个参数表示是否要递归遍历
        RemoteIterator<LocatedFileStatus> remoteIterator = fileSystem.listFiles(new Path("/"), true);
        while (remoteIterator.hasNext()) {
            LocatedFileStatus fileStatus = remoteIterator.next();
            System.out.println("文件路径为："+fileStatus.getPath().toString());

        }
    }
    /**
     * 从hdfs拷贝文件到本地
     */
    @Test
    public void copyFileToLocal() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        //指定要拷贝的文件的路径
        FSDataInputStream open = fileSystem.open(new Path("/aaa"));

        //指定拷贝的目的地
        FileOutputStream outputStream = new FileOutputStream(new File("e:/"));
        IOUtils.copy(open,outputStream);
        IOUtils.closeQuietly(open);
        IOUtils.closeQuietly(outputStream);
        fileSystem.close();
    }
}
