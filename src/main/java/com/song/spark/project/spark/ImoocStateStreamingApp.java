package com.song.spark.project.spark;

import com.song.spark.kafka.KafkaProperties;
import com.song.spark.project.dao.CourseClickCountDAO;
import com.song.spark.project.domain.ClickLog;
import com.song.spark.project.domain.CourseClickCount;
import com.song.spark.project.utils.DateUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * @author songshiyu
 * @date 2020/2/15 11:23
 **/
public class ImoocStateStreamingApp {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf();//.setAppName(ImoocStateStreamingApp.class.getSimpleName()).setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Seconds.apply(60));

        Map<String,String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", KafkaProperties.BROKER_LIST);

        Set<String> topics = new HashSet<>();
        topics.add("streaming_song");

        JavaPairInputDStream<String, String> messageStream = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics
        );

        JavaDStream<String> logsStream = messageStream.map(tuple2 -> tuple2._2);

        JavaDStream<ClickLog> clickDStream = logsStream.map(line -> {
            String[] infos = line.split("\t");

            //infos[2] "GET /class/128.html HTTP/1.1"	200
            String url = infos[2].split(" ")[1];
            Integer courseId = 0;

            if (url.startsWith("/class")) {
                String courseHTML = url.split("/")[2];
                courseId = Integer.valueOf(courseHTML.substring(0, courseHTML.lastIndexOf(".")));
            }

            ClickLog clickLog = new ClickLog(infos[0], DateUtils.formatDate(infos[1]), courseId, Integer.parseInt(infos[3]), infos[4]);
            return clickLog;
        }).filter(clickLog -> clickLog.getCourseId() != 0);

        clickDStream.mapToPair(clickLog -> {
            String dayCourse = clickLog.getTime().substring(0, 8) + "_" + clickLog.getCourseId();
            return new Tuple2<>(dayCourse,1L);
        }).reduceByKey((v1,v2) -> v1 + v2)
                .foreachRDD(new VoidFunction2<JavaPairRDD<String, Long>, Time>() {
                    @Override
                    public void call(JavaPairRDD<String, Long> pairRDD, Time time) throws Exception {
                        pairRDD.foreachPartition(partitions -> {
                            List<CourseClickCount> list = new ArrayList<>();
                            while (partitions.hasNext()){
                                Tuple2<String, Long> tuple2 = partitions.next();
                                list.add(new CourseClickCount(tuple2._1,tuple2._2));
                            }
                            CourseClickCountDAO.saveDataToHabse(list);
                        });
                    }
                });

        jssc.start();
        jssc.awaitTermination();
    }
}

