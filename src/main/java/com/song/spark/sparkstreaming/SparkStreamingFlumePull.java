package com.song.spark.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author songshiyu
 * @date 2020/2/12 22:20
 * Spark Streaming 基于push方式整合flume
 **/
public class SparkStreamingFlumePull {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingFlumePush");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Seconds.apply(5));

        JavaReceiverInputDStream<SparkFlumeEvent> flumeStream = FlumeUtils.createPollingStream(jssc, "192.168.137.129", 41414);
        flumeStream.flatMap(new FlatMapFunction<SparkFlumeEvent,String>() {
            @Override
            public Iterator<String> call(SparkFlumeEvent sparkFlumeEvent) throws Exception {
                String line = new String(sparkFlumeEvent.event().getBody().array()).replace("\r","");
                return Arrays.asList(line).iterator();
            }
        }).mapToPair(key -> new Tuple2<>(key,1)).reduceByKey((v1,v2) -> v1 + v2).print(10);

        jssc.start();
        jssc.awaitTermination();
    }
}
