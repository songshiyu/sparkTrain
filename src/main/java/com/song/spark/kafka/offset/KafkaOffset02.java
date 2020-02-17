package com.song.spark.kafka.offset;

import java.io.File;
import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Pattern;

import kafka.serializer.StringDecoder;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import com.google.common.io.Files;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.util.LongAccumulator;

/**
 * Use this singleton to get or register a Broadcast variable.
 */
class JavaWordBlacklist {

    private static volatile Broadcast<List<String>> instance = null;

    public static Broadcast<List<String>> getInstance(JavaSparkContext jsc) {
        if (instance == null) {
            synchronized (JavaWordBlacklist.class) {
                if (instance == null) {
                    List<String> wordBlacklist = Arrays.asList("a", "b", "c");
                    instance = jsc.broadcast(wordBlacklist);
                }
            }
        }
        return instance;
    }
}

class JavaDroppedWordsCounter {

    private static volatile LongAccumulator instance = null;

    public static LongAccumulator getInstance(JavaSparkContext jsc) {
        if (instance == null) {
            synchronized (JavaDroppedWordsCounter.class) {
                if (instance == null) {
                    instance = jsc.sc().longAccumulator("WordsInBlacklistCounter");
                }
            }
        }
        return instance;
    }
}

public final class KafkaOffset02 {
    private static final Pattern SPACE = Pattern.compile(" ");

    private static JavaStreamingContext createContext(String checkpointDirectory,String outputPath) {

        System.out.println("Creating new context");
        File outputFile = new File(outputPath);
        if (outputFile.exists()) {
            outputFile.delete();
        }
        SparkConf sparkConf = new SparkConf().setAppName("KafkaOffset02").setMaster("local[*]");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        ssc.checkpoint(checkpointDirectory);

        Map<String,String> kafkaParams = new HashMap<String,String>(){
            {
                put("metadata.broker.list","hadoop000:9092");
            }
        };

        Set<String> topics = new HashSet<>();
        topics.add("song");

        JavaPairInputDStream<String, String> directStream = KafkaUtils.createDirectStream(
                ssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics
        );

        JavaPairDStream<String, Integer> wordCounts = directStream.map(tuple2 -> tuple2._2)
                .flatMap(x -> Arrays.asList(SPACE.split(x)).iterator())
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);

        wordCounts.foreachRDD((rdd, time) -> {
            Broadcast<List<String>> blacklist =
                    JavaWordBlacklist.getInstance(new JavaSparkContext(rdd.context()));

            LongAccumulator droppedWordsCounter =
                    JavaDroppedWordsCounter.getInstance(new JavaSparkContext(rdd.context()));

            String counts = rdd.filter(wordCount -> {
                if (blacklist.value().contains(wordCount._1())) {
                    droppedWordsCounter.add(wordCount._2());
                    return false;
                } else {
                    return true;
                }
            }).collect().toString();
            String output = "Counts at time " + time + " " + counts;
            System.out.println(output);
            System.out.println("Dropped " + droppedWordsCounter.value() + " word(s) totally");
            System.out.println("Appending to " + outputFile.getAbsolutePath());
            Files.append(output + "\n", outputFile, Charset.defaultCharset());
        });

        return ssc;
    }

    public static void main(String[] args) throws Exception {
        String checkpointDirectory = "E:\\工作软件\\data\\checkpoint\\";
        String outputPath = "E:\\工作软件\\data\\output\\";

        Function0<JavaStreamingContext> createContextFunc =
                () -> createContext(checkpointDirectory, outputPath);

        JavaStreamingContext ssc =
                JavaStreamingContext.getOrCreate(checkpointDirectory, createContextFunc);
        ssc.start();
        ssc.awaitTermination();
    }
}