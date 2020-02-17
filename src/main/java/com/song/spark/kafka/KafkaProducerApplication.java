package com.song.spark.kafka;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.UUID;

/**
 * @author songshiyu
 * @date 2020/2/16 16:34
 **/
public class KafkaProducerApplication {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("metadata.broker.list",KafkaProperties.BROKER_LIST);
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        properties.put("request.required.acks","1");
        Producer producer = new Producer(new ProducerConfig(properties));

        for (int i = 0; i < 100;i++){
            producer.send(new KeyedMessage(KafkaProperties.TOPIC,"宋时雨准备数据:" + UUID.randomUUID()));
        }
        System.out.println("宋时雨准备数据发送完毕");
    }
}
