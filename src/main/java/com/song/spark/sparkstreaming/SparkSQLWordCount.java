package com.song.spark.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author songshiyu
 * @date 2020/2/11 23:01
 *
 * sparkStreaming集成sparksql
 **/
public class SparkSQLWordCount {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Seconds.apply(5));

        JavaReceiverInputDStream<String> receiverInputDStream = jssc.socketTextStream("192.168.137.129", 9999);

        JavaDStream<String> dStream = receiverInputDStream.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        dStream.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
            @Override
            public void call(JavaRDD<String> pairRDD, Time time) throws Exception {
                SparkSession sparkSession = SparkSession.builder().config(pairRDD.rdd().sparkContext().getConf()).getOrCreate();

                JavaRDD<JavaRow> javaRDD = pairRDD.map(new Function<String, JavaRow>() {
                    @Override
                    public JavaRow call(String records) throws Exception {
                        JavaRow javaRow = new JavaRow();
                        javaRow.setWord(records);
                        return javaRow;
                    }
                });

                Dataset<Row> dataFrame = sparkSession.createDataFrame(javaRDD, JavaRow.class);
                dataFrame.createOrReplaceTempView("words");

                sparkSession.sql("select word,count(1) as total from words group by word").show();

            }
        });

        jssc.start();
        jssc.awaitTermination();
    }
}

