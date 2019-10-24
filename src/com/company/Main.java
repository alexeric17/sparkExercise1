package com.company;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class Main {

    public static void main(String[] args) {
        //Config spark
        SparkConf conf = new SparkConf().setAppName("FirstTest").setMaster("local[2]");
        //Start spark conext
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> distFile = sc.textFile("C:\\Users\\Alexander Eric\\Desktop\\Skola augsburg\\JavaSpark\\untitled\\filehash.csv");
        //Find pairs - Map??
        JavaPairRDD<String, Integer> pairs = distFile.mapToPair(s -> new Tuple2(s,1));
        //Reduce
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a,b) -> a + b);
        //counts.sortByKey();
        //Function for sorting tuples seen more than once.
        Function<Tuple2<String, Integer>, Boolean> filterFunction = w -> (w._2 == 2);
        //Need a new JavaPairRDD to hold the 'result'.
        JavaPairRDD<String, Integer> result = counts.filter(filterFunction);

        //counts.filter(filterFunction);
        result.foreach(element -> { System.out.println(element._1 + " " + element._2);});

    }
}
