package com.ragu.spark.demo.main;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class App {

	public static void main(String[] args) {

		SparkConf sConf = new SparkConf().setAppName("Work Count");
		
		JavaSparkContext sc = new JavaSparkContext(sConf);
		
		String dataFilePath = "D:/output/data.csv";
		String testFilePath = "D:/output/testinput.txt";
		String testOutFilePath = "testoutput";
		
		JavaRDD<String> textFile = sc.textFile(testFilePath);
		
		JavaPairRDD<String, Integer> counts = textFile
			    .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
			    .mapToPair(word -> new Tuple2<>(word, 1))
			    .reduceByKey((a, b) -> a + b);
		
		counts.saveAsTextFile(testOutFilePath);
		
		System.out.println("Count of the word is : " + counts.count());
		
		sc.stop();
		
	}

}
