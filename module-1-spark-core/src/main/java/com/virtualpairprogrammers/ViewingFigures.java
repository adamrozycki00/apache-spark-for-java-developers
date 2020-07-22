package com.virtualpairprogrammers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import static com.config.GlobalSettings.setHadoopAndLogger;

/**
 * This class is used in the chapter late in the course where we analyse viewing figures.
 * You can ignore until then.
 */
public class ViewingFigures {

    public static void main(String[] args) {
        setHadoopAndLogger();

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Use true to use hardcoded data identical to that in the PDF guide.
        boolean testMode = false;

        JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
        JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
        JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);

        JavaPairRDD<Integer, Long> chapterCount = chapterData
                .mapToPair(tuple -> new Tuple2<>(tuple._2, 1L))
                .reduceByKey(Long::sum);

        System.out.println("Top 10 courses");

        viewData
                .distinct()
                .mapToPair(tpl -> new Tuple2<>(tpl._2, tpl._1))
                .join(chapterData)
                .mapToPair(tpl -> new Tuple2<>(tpl._2, 1L))
                .reduceByKey(Long::sum)
                .mapToPair(tpl -> new Tuple2<>(tpl._1._2, tpl._2))
                .join(chapterCount)
                .mapToPair(tpl -> new Tuple2<>(tpl._1, (double) tpl._2._1 / tpl._2._2))
                .mapToPair(tpl -> {
                    int points = tpl._2 < .25 ?
                            0 : tpl._2 < .5 ?
                            2 : tpl._2 < .9 ?
                            4 : 10;
                    return new Tuple2<>(tpl._1, points);
                })
                .reduceByKey(Integer::sum)
                .join(titlesData)
                .mapToPair(tpl -> tpl._2)
                .sortByKey(false)
                .map(tpl -> String.format("%-38s %4d", tpl._2 + ":", tpl._1))
                .take(10)
                .forEach(System.out::println);
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
    }

    private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {
        if (testMode) {
            // (chapterId, title)
            List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
            rawTitles.add(new Tuple2<>(1, "How to find a better job"));
            rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
            rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
            return sc.parallelizePairs(rawTitles);
        }
        return sc.textFile("module-1-spark-core/src/main/resources/viewing figures/titles.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] cols = commaSeparatedLine.split(",");
                    return new Tuple2<>(Integer.valueOf(cols[0]), cols[1]);
                });
    }

    private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {
        if (testMode) {
            // (chapterId, courseId)
            List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
            rawChapterData.add(new Tuple2<>(96, 1));
            rawChapterData.add(new Tuple2<>(97, 1));
            rawChapterData.add(new Tuple2<>(98, 1));
            rawChapterData.add(new Tuple2<>(99, 2));
            rawChapterData.add(new Tuple2<>(100, 3));
            rawChapterData.add(new Tuple2<>(101, 3));
            rawChapterData.add(new Tuple2<>(102, 3));
            rawChapterData.add(new Tuple2<>(103, 3));
            rawChapterData.add(new Tuple2<>(104, 3));
            rawChapterData.add(new Tuple2<>(105, 3));
            rawChapterData.add(new Tuple2<>(106, 3));
            rawChapterData.add(new Tuple2<>(107, 3));
            rawChapterData.add(new Tuple2<>(108, 3));
            rawChapterData.add(new Tuple2<>(109, 3));
            return sc.parallelizePairs(rawChapterData);
        }
        return sc.textFile("module-1-spark-core/src/main/resources/viewing figures/chapters.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] cols = commaSeparatedLine.split(",");
                    return new Tuple2<>(Integer.valueOf(cols[0]), Integer.valueOf(cols[1]));
                });
    }

    private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {
        if (testMode) {
            // Chapter views - (userId, chapterId)
            List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
            rawViewData.add(new Tuple2<>(14, 96));
            rawViewData.add(new Tuple2<>(14, 97));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(14, 99));
            rawViewData.add(new Tuple2<>(13, 100));
            return sc.parallelizePairs(rawViewData);
        }
        return sc.textFile("module-1-spark-core/src/main/resources/viewing figures/views-*.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] columns = commaSeparatedLine.split(",");
                    return new Tuple2<>(Integer.valueOf(columns[0]), Integer.valueOf(columns[1]));
                });
    }

}
