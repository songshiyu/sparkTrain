package com.song.spark.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author songshiyu
 * @date 2020/2/12 20:30
 **/
public class BlackFilterJob {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Seconds.apply(5));

        JavaPairRDD<String, Boolean> blackRDD = jssc.sparkContext()
                .parallelize(Arrays.asList("zhangsan", "lisi"))
                .mapToPair(key -> new Tuple2<>(key, true));

        JavaReceiverInputDStream<String> receiverInputDStream = jssc.socketTextStream("192.168.137.129", 9999);

        JavaDStream<Tuple2<String, String>> finalRDD = receiverInputDStream.transform(new Function<JavaRDD<String>, JavaRDD<Tuple2<String, String>>>() {
            @Override
            public JavaRDD<Tuple2<String, String>> call(JavaRDD<String> rdd) throws Exception {
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinRDD = rdd.flatMapToPair(new PairFlatMapFunction<String, String, String>() {
                    @Override
                    public Iterator<Tuple2<String, String>> call(String s) throws Exception {
                        List<Tuple2<String, String>> list = new ArrayList<>();
                        String[] splits = s.split("\\,", -1);
                        if (splits.length >= 2) {
                            list.add(new Tuple2<String, String>(splits[0], splits[1]));
                        }
                        return list.iterator();
                    }
                }).leftOuterJoin(blackRDD);

                JavaRDD<Tuple2<String, String>> resultRDD = joinRDD.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple2) throws Exception {
                        if (tuple2._2._2.orElse(false)){
                            return false;
                        }
                        return true;
                    }
                }).mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,Tuple2<String,Optional<Boolean>>>>, Tuple2<String,String>>() {
                    @Override
                    public Iterator<Tuple2<String, String>> call(Iterator<Tuple2<String, Tuple2<String, Optional<Boolean>>>> tuple2) throws Exception {
                        List<Tuple2<String,String>> list = new ArrayList<>();
                        while (tuple2.hasNext()){
                            Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple21 = tuple2.next();
                            list.add(new Tuple2<>(tuple21._1,tuple21._2._1));
                        }
                        return list.iterator();
                    }
                });
                return resultRDD;
            }
        });

        finalRDD.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
