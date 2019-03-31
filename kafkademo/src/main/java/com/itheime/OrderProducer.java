package com.itheime;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class OrderProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties props  = new Properties();

        props.put("bootstrap.servers", "node01:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 10; i++) {
            // 发送数据 ,需要一个producerRecord对象,最少参数 String topic, V value
            producer.send(new ProducerRecord<String, String>("order", "订单信息！"+i));
            Thread.sleep(100);
        }
    }

}

