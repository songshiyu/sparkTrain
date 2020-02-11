package com.song.spark.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author songshiyu
 * @date 2020/2/10 20:47
 *
 * Kafka生产者
 **/
public class KafkaProducer extends Thread{

    private String topic;
    Producer<Integer,String> producer;

    public KafkaProducer(String topic){
        this.topic = topic;

        Properties properties = new Properties();
        properties.put("metadata.broker.list",KafkaProperties.BROKER_LIST);
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        properties.put("request.required.acks","1");


        producer = new Producer<Integer, String>(new ProducerConfig(properties));
    }

    @Override
    public void run() {
        int theadNum = 1;
        while (true){
            String message = "message_" + theadNum;
            producer.send(new KeyedMessage<>(topic,message));
            System.out.println("send message:" + message);
            theadNum ++;

            try {
                Thread.sleep(2000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
