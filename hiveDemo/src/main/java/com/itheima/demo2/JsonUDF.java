package com.itheima.demo2;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class JsonUDF extends UDF {
    public Text evaluate(Text line) {
        if (line != null && !line.equals("")) {
            MovieBean movieBean = JSONObject.parseObject(line.toString(), MovieBean.class);
            return new Text(movieBean.toString());
        }
        return null;
    }
}
