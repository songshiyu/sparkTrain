package com.song.spark.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author songshiyu
 * @date 2020/2/10 23:33
 *
 * TODO Consumer如何消费历史数据？
 **/
public class KafkaConsumer extends Thread{

    private String topic;

    public KafkaConsumer(String topic){
        this.topic = topic;
    }

    private ConsumerConnector createConsumer(){
        Properties properties = new Properties();
        properties.put("zookeeper.connect",KafkaProperties.ZK);
        properties.put("group.id",KafkaProperties.GROUP_ID);
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
        return consumer;
    }

    @Override
    public void run() {
        ConsumerConnector consumer = createConsumer();
        Map<String,Integer> map = new HashMap<>();
        map.put(topic,1);
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(map);
        KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);

        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
        while (iterator.hasNext()){
            MessageAndMetadata<byte[], byte[]> next = iterator.next();
            String message = new String(next.message());
            System.out.println("received:" + message);
        }

    }
}
