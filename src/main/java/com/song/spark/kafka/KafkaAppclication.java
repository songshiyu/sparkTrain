package com.song.spark.kafka;

/**
 * @author songshiyu
 * @date 2020/2/10 21:29
 **/
public class KafkaAppclication {
    public static void main(String[] args) {
        new Thread(new KafkaProducer(KafkaProperties.TOPIC)).start();

        new Thread(new KafkaConsumer(KafkaProperties.TOPIC)).start();
    }
}
