package com.itheima;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

public class FileHandle {
    /**
     * 设置hadoop HDFS 初始化配置方法
     * @throws IOException
     */
    public static FileSystem init(){
        Configuration config=new Configuration();
        config.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fs=null;
        try{
            fs=FileSystem.get(config);
        }catch(Exception e){
            throw new RuntimeException("初始化异常");
        }
        return fs;
    }



    /**
     * 将HDFS上文件复制到HDFS上
     * @param src   原目标
     * @param dsc   复制到的目标
     * @throws Exception
     * @throws IllegalArgumentException
     */
    public static void copy(String src,String dsc) throws IllegalArgumentException, Exception{
        /**
         * 1:建立输入流
         * 2：建立输出流
         * 3:两个流的对接
         * 4:资源的关闭
         */
        FileSystem fs=init();

        //1:建立输入流
        FSDataInputStream input=fs.open(new Path(src));

        //2:建立输出流
        FSDataOutputStream output=fs.create(new Path(dsc));

        //3:两个流的对接
        byte[] b= new byte[1024];
        int hasRead=0;
        while((hasRead=input.read(b))>0){
            output.write(b, 0, hasRead);
        }
        //4:资源的关闭
        input.close();
        output.close();
        fs.close();

    }
    /**
     * 复制一个目录下面的所有文件
     * @param src   需要复制的文件夹或文件
     * @param dsc   目的地
     * @throws Exception
     * @throws FileNotFoundException
     */
    public static void copyDir(String src,String dsc) throws FileNotFoundException, Exception{
        //获取hdfs文件系统
        FileSystem fs=init();
        //获取源文件路径
        Path srcPath=new Path(src);
        //获取源文件最后一级目录名
        String[] strs=src.split("/");
        String lastName=strs[strs.length-1];

        if(fs.isDirectory(srcPath)){//如果源文件为文件夹
            //在目标路径下创建名为“lastName”的文件夹
            fs.mkdirs(new Path(dsc+"/"+lastName));

            //遍历目标文件夹
            FileStatus[] fileStatus=fs.listStatus(srcPath);
            for(FileStatus fileSta:fileStatus){
                copyDir(fileSta.getPath().toString(),dsc+"/"+lastName);
            }

        }else{//如果是文件
            fs.mkdirs(new Path(dsc));
            System.out.println("src"+src+"\n"+dsc+"/"+lastName);
            copy(src,dsc+"/"+lastName);
        }
    }
    @Test
    public void copyToLoacl() throws Exception {
        copyDir("/aaa","/aaa1");
    }

}
