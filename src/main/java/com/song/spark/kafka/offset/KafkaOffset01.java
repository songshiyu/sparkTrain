package com.song.spark.kafka.offset;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author songshiyu
 * @date 2020/2/16 16:42
 * 从最早的offset开始进行消费，应用程序一旦停止，在次启动时，会重复消费
 **/
public class KafkaOffset01 {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("test");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Seconds.apply(5));

        Map<String,String> kafkaParams = new HashMap<String,String>(){
            {
                put("metadata.broker.list", "192.168.137.129:9092");
                put("auto.offset.reset","smallest");
            }
        };

        Set<String> topics = new HashSet<>();
        topics.add("song");

        KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics
        ).foreachRDD(rdd -> {
            if (!rdd.isEmpty()){
                System.out.println("宋时雨:" + rdd.count());
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }
}
