package com.ragu.spark.demo.main;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import java.util.regex.Pattern;

public class WordCountApp {
	  private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {

		SparkConf sConf = new SparkConf().setAppName("Work Count");
		
		JavaSparkContext spark = new JavaSparkContext(sConf);
		
		String dataFilePath = "D:/output/data.csv";
		String testFilePath = "D:/output/testinput.txt";
		String testOutFilePath = "testoutput";
		
		JavaRDD<String> lines = spark.textFile(testFilePath);
		
	    JavaRDD<String> words = lines.flatMap(s -> Arrays.asList((SPACE.split(s))).iterator());

	    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

	    JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

	    List<Tuple2<String, Integer>> output = counts.collect();
	    for (Tuple2<?,?> tuple : output) {
	      System.out.println(tuple._1() + ": " + tuple._2());
	    }
	    
	    spark.stop();
		
		
	}

}
