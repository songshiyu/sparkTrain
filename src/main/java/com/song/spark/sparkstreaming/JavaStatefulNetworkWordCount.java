package com.song.spark.sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author songshiyu
 * @date 2020/2/12 0:44
 **/
public class JavaStatefulNetworkWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("JavaStatefulNetworkWordCount").setMaster("local[*]");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        ssc.checkpoint(".");

        List<Tuple2<String, Integer>> tuples =
                Arrays.asList(new Tuple2<>("hello", 1), new Tuple2<>("world", 1));
        JavaPairRDD<String, Integer> initialRDD = ssc.sparkContext().parallelizePairs(tuples);

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
                "192.168.137.129", 9999, StorageLevels.MEMORY_AND_DISK_SER_2);

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());

        JavaPairDStream<String, Integer> wordsDstream = words.mapToPair(s -> new Tuple2<>(s, 1));

        // Update the cumulative count function
        Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc =
                (word, one, state) -> {
                    int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
                    Tuple2<String, Integer> output = new Tuple2<>(word, sum);
                    state.update(sum);
                    return output;
                };

        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateDstream =
                wordsDstream.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD));

        stateDstream.print();
        ssc.start();
        ssc.awaitTermination();
    }
}
