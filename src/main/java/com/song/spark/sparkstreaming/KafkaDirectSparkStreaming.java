package com.song.spark.sparkstreaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * @author songshiyu
 * @date 2020/2/13 22:40
 **/
public class KafkaDirectSparkStreaming {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName(KafkaDirectSparkStreaming.class.getSimpleName()).setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Seconds.apply(5));

        Map<String,String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "192.168.137.129:9092");

        Set<String> topic = new HashSet<>();
        topic.add("streaming_song");

        JavaPairInputDStream<String, String> directStream = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topic
        );

        JavaPairDStream<String, Integer> pairDStream = directStream.map(tuple -> tuple._2).
                flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(key -> new Tuple2<>(key, 1))
                .reduceByKey((v1, v2) -> v1 + v2);

        pairDStream.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
