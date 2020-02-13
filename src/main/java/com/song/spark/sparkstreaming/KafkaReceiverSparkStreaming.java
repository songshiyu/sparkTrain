package com.song.spark.sparkstreaming;

import com.song.spark.kafka.KafkaProperties;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author songshiyu
 * @date 2020/2/13 22:00
 **/
public class KafkaReceiverSparkStreaming {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName(KafkaReceiverSparkStreaming.class.getSimpleName()).setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Seconds.apply(5));

        Map<String,Integer> topicMap = new HashMap<>();
        topicMap.put("kafka_streaming_song",1);
        JavaPairReceiverInputDStream<String, String> receiverInputDStream = KafkaUtils.createStream(jssc, KafkaProperties.ZK, KafkaProperties.GROUP_ID, topicMap);

        JavaPairDStream<String, Integer> pairDStream = receiverInputDStream
                .map(tuple2 -> tuple2._2)
                .flatMap(line -> Arrays.asList(line.split("\\,")).iterator())
                .mapToPair(key -> new Tuple2<>(key, 1))
                .reduceByKey((v1, v2) -> v1 + v2);

        pairDStream.print();


        jssc.start();
        jssc.awaitTermination();
    }
}
