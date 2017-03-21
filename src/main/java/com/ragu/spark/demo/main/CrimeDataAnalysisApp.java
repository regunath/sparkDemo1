package com.ragu.spark.demo.main;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class CrimeDataAnalysisApp {
	  private static final Pattern SPACE = Pattern.compile(" ");
	  private static final String DATA_FILE_PATH = "src/main/resources/data/data1.csv";
	  private static final String LOG_LEVEL = "WARN";
	public static void main(String[] args) {

		SparkConf sConf = new SparkConf().setAppName("Work Count");
		JavaSparkContext spark = new JavaSparkContext(sConf);
		spark.setLogLevel(LOG_LEVEL);
		
		JavaRDD<String> data = spark.textFile(DATA_FILE_PATH);
		
		String header = data.first();
		
		String[] headerArr = splitBySeperator(header);
		JavaRDD<String> dataWoHeader = data.filter(x -> !x.equalsIgnoreCase(header));
		
		System.out.println();
		System.out.println();
		System.out.println("*****************************************************");	    

//		List<HashMap<String, String>> rowMapDataLst = dataWoHeader.map(x -> x.split(",")).map(x -> {
//			HashMap<String, String> rowMapData = new HashMap<>();
//			for (int i=0; i<headerArr.length; i++){
//				rowMapData.put(headerArr[i],x[i]);
//			}
//			return rowMapData;
//		}).filter(x -> (Long.parseLong(x.get("OccurrenceYear")) > 2014)).map(x -> x.get("OccurrenceYear")).countByValue();

		JavaRDD<HashMap<String, String>> rowMapDataLst = dataWoHeader.map(x -> x.split(",")).map(x -> {
			HashMap<String, String> rowMapData = new HashMap<>();
			for (int i=0; i<headerArr.length; i++){
				rowMapData.put(headerArr[i],x[i]);
			}
			return rowMapData;
		});

		
		JavaRDD<HashMap<String, String>> filteredRowMapDataLst = rowMapDataLst.filter(x -> (Long.parseLong(x.get("OccurrenceYear")) >= 2000));
		
		JavaPairRDD<Double, Double> globalPosition = filteredRowMapDataLst
				.mapToPair(t -> new Tuple2<Double, Double>(Double.parseDouble(t
						.get("Latitude")),
						Double.parseDouble(t.get("Longitude"))));
		
		System.out.println("Tuples " + globalPosition.collect());
		
		//JavaPairRDD<Long, Long> globalPosition = filteredRowMapDataLst.map(x -> new Tuple2<Long, Long>(Long.parseLong(x.get("Latitude")), Long.parseLong(x.get("Longitude"))));
			
//		System.out.println("DDDDDDDDDDDDDDDDDD");
//		for (HashMap<String, String> hashMap : data1) {
//			System.out.println(hashMap);
//		}
//		System.out.println("EEEEEEEEEEEEEEEEEE");
		
		Map<String, Long> groupByOffense = dataWoHeader.map(x -> x.split(",")).map(x -> {
			HashMap<String, String> rowMapData = new HashMap<>();
			for (int i=0; i<headerArr.length; i++){
				rowMapData.put(headerArr[i],x[i]);
			}
			return rowMapData;
		}).map(x -> x.get("OccurrenceYear")).countByValue();
		
		System.out.println("OccurrenceYear " + groupByOffense);
		
		if(!dataWoHeader.isEmpty()){
			System.out.println("First of data : " + dataWoHeader.first());
		}else {
			System.out.println("EMPTY!");
		}
		
		System.out.println("*****************************************************");
		System.out.println();
		System.out.println();
		
	    spark.stop();
		
	}

	static public String[] splitBySeperator(String line){
		return line.replace(" ", "_").replace("/", "_").split(",");
	}
	
	static public void printer(String str){
		System.out.println(str);
	}
}
