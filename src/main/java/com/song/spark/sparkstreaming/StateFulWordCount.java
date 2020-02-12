package com.song.spark.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author songshiyu
 * @date 2020/2/11 23:40
 * 使用spark Streaming完成有状态统计
 **/
public class StateFulWordCount {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[n]").setAppName("StateFulWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Seconds.apply(5));
        //jssc.checkpoint("/home/hadoop/app/spark-2.2.0-bin-2.6.0-cdh5.7.0/data/song/checkpoint\n");
        jssc.checkpoint("file:///E:\\工作软件\\data\\checkpoint\\");

        JavaReceiverInputDStream<String> receiverInputDStream = jssc.socketTextStream("192.168.137.129", 9999);

        JavaPairDStream<String, Integer> pairDStream = receiverInputDStream.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(value -> new Tuple2<>(value, 1))
                .updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
                    @Override
                    public Optional<Integer> call(List<Integer> newCountList, Optional<Integer> oldCount) throws Exception {
                        Integer newCountSum = newCountList.stream().reduce(Integer::sum).orElse(0);
                        Integer oldSum = oldCount.orElse(0);
                        return Optional.of(newCountSum + oldSum);
                    }
                });

        pairDStream.print();

        jssc.start();
        jssc.awaitTermination();
    }


}
