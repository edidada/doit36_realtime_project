package com.test.bigdata.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class EventsMoni {
    public static void main(String[] args) throws InterruptedException {

        Properties pros = new Properties();
        //指定kafka集群的地址
        pros.setProperty("bootstrap.servers", "doitedu:9092");
        //指定key的序列化方式
        pros.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //指定value的序列化方式
        pros.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //ack模式，取值有0，1，-1（all），all是最慢但最安全的  服务器应答生产者成功的策略
        pros.put("acks", "0");
        //这是kafka发送数据失败的重试次数，这个可能会造成发送数据的乱序问题
        pros.setProperty("retries", "3");
        //数据发送批次的大小 单位是字节
        pros.setProperty("batch.size", "10000");
        //一次数据发送请求所能发送的最大数据量
        pros.setProperty("max.request.size", "102400");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(pros);

        ProducerRecord<String, String> record1 = new ProducerRecord<>("dwd-events-log", "", "{\"user_id\":20,\"event_id\":\"item_collect\",\"event_time\":1678777685000,\"properties\":{\"item_id\":\"p04\"}}");
        ProducerRecord<String, String> record2 = new ProducerRecord<>("dwd-events-log", "", "{\"user_id\":20,\"event_id\":\"xxxxxxxxx\",\"event_time\":1678777685000,\"properties\":{\"item_id\":\"p04\"}}");
        ProducerRecord<String, String> record3 = new ProducerRecord<>("dwd-events-log",  "", "{\"user_id\":20,\"event_id\":\"ad_click\",\"event_time\":1678777689000,\"properties\":{\"item_id\":\"p04\"}}");
        for(;;){
            //3.调用kafka的api去发送消息
            kafkaProducer.send(record2);
            kafkaProducer.send(record2);
            kafkaProducer.send(record2);
            kafkaProducer.send(record2);
            kafkaProducer.send(record2);
            kafkaProducer.send(record2);
            kafkaProducer.send(record2);
            kafkaProducer.send(record2);
            kafkaProducer.send(record2);
            kafkaProducer.send(record2);
            Thread.sleep(10);
        }

    }
}
