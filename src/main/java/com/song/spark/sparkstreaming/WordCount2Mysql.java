package com.song.spark.sparkstreaming;

import com.song.spark.util.MysqlDBPoolUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @author songshiyu
 * @date 2020/2/12 9:53
 **/
public class WordCount2Mysql {
    public static void main(String[] args) throws InterruptedException{
        SparkConf conf = new SparkConf().setAppName("WordCount2Mysql").setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Seconds.apply(5));

        JavaReceiverInputDStream<String> receiverInputDStream = jssc.socketTextStream("192.168.137.129", 9999);

        JavaPairDStream<String, Integer> pairDStream = receiverInputDStream.flatMap(line -> Arrays.asList(line.split("\\ ")).iterator())
                .mapToPair(key -> new Tuple2<>(key, 1))
                .reduceByKey((v1, v2) -> v1 + v2);

        pairDStream.foreachRDD(new VoidFunction2<JavaPairRDD<String, Integer>, Time>() {
            @Override
            public void call(JavaPairRDD<String, Integer> pairRDD, Time time) throws Exception {
                pairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> tuple2Iterator) throws Exception {
                        Connection connection = MysqlDBPoolUtil.getConnection();
                        while (tuple2Iterator.hasNext()){
                            Tuple2<String, Integer> tuple2 = tuple2Iterator.next();
                            String sql = "insert into word_count(word,count) values ('" + tuple2._1 + "'," + tuple2._2 + ")";
                            connection.createStatement().execute(sql);
                        }
                    }
                });
            }
        });
        pairDStream.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
