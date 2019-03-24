package com.itheima.demo1;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class UpperUDF extends UDF {
    public Text evaluate(Text text){
        if (text == null) {
            return null;
        }
        return new Text(text.toString().toUpperCase());
    }
}
