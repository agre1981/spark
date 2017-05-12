package com.pgs.spark.crimes;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by ogrechanov on 5/11/2017.
 */
public class WordCount {

    public static void main(String[] args) throws Exception {

        JavaSparkContext spark = new JavaSparkContext("local[4]", "JavaWordCount");
        try {

            JavaRDD<String> linesRDD = spark.parallelize(Arrays.asList(new String[]{"aaa bbb", "aaa ccc", "aaa bbb"}));

            JavaRDD<String> wordsRDD = linesRDD.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")));

            JavaPairRDD<String, Integer> ones = wordsRDD.mapToPair(
                    (PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));

            JavaPairRDD<String, Integer> counts = ones.reduceByKey(
                    (Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2);

            List<Tuple2<String, Integer>> output = counts.collect();
            for (Tuple2<?, ?> tuple : output) {
                System.out.println(tuple._1() + ": " + tuple._2());
            }
        }
        finally {
            spark.stop();
        }
    }

}
