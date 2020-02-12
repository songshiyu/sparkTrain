package com.song.spark.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author songshiyu
 * @date 2020/2/12 18:31
 **/
public class WordCountByWindow {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("WordCountByWindow");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Seconds.apply(2));

        jssc.checkpoint(".");

        JavaReceiverInputDStream<String> receiverInputDStream = jssc.socketTextStream("192.168.137.129", 9999);

        JavaPairDStream<String, Integer> pairDStream = receiverInputDStream.flatMap(line -> Arrays.asList(line.split("\\ ")).iterator())
                .mapToPair(key -> new Tuple2<>(key, 1))
                .reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer + integer2;
                    }
                }, new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer + integer2;
                    }
                }, Seconds.apply(10), Seconds.apply(4));

        pairDStream.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
