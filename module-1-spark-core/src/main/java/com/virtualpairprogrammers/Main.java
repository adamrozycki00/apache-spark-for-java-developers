package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Main {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String filePath = Main.class.getClassLoader().getResource("subtitles/input.txt").getFile();

        sc.textFile(filePath)
                .flatMap(str -> Arrays.asList(str.split(" ")).iterator())
                .map(word -> word.replaceAll("[^a-zA-Z]", ""))
                .map(String::toLowerCase)
                .filter(word -> word.length() > 1)
                .filter(Util::isNotBoring)
                .mapToPair(word -> new Tuple2<>(word, 1L))
                .reduceByKey(Long::sum)
                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                .sortByKey(false)
                .take(10)
                .forEach(System.out::println);

    }

}
