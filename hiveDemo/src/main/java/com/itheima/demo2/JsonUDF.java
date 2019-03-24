package com.itheima.demo2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.testng.annotations.Test;

import java.io.IOException;

public class JsonUDF extends UDF {
    public Text evaluate(String inputPath,String outputPath) throws IOException {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        FSDataInputStream inputStream = fileSystem.open(new Path(inputPath));
//        BufferedReader reader = new BufferedReader();

        return null;
    }
    @Test
    public void testPartition (){
        for (int i = 1; i <5 ; i++) {
            System.out.println(new Integer(i).hashCode() & Integer.MAX_VALUE);
        }
    }
}
