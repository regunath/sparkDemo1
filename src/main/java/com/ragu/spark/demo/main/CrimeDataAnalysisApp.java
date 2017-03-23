package com.ragu.spark.demo.main;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

import com.google.gson.Gson;

public class CrimeDataAnalysisApp {
	
	  private static final int MAX_COORDINATES = 5000;
	  private static final String APP_NAME = "SPARK ETL DEMO";
	  private static final String DATA_FILE_PATH = "src/main/resources/data/data.csv";
	  private static final String GPS_OUT_DATA_JSON = "src/main/resources/html/gpsCoordinates.json";
	  
	  private static final String LOG_LEVEL = "WARN";
	  
	public static void main(String[] args) {

		SparkConf sConf = new SparkConf().setAppName(APP_NAME);
		JavaSparkContext spark = new JavaSparkContext(sConf);
		spark.setLogLevel(LOG_LEVEL);
		
		JavaRDD<String> data = spark.textFile(DATA_FILE_PATH);
		
		String header = data.first();
		
		String[] headerArr = GenUtil.splitBySeperator(header);
		JavaRDD<String> dataWoHeader = data.filter(x -> !x.equalsIgnoreCase(header));
		
		GenUtil.printSeperator();	    

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
		
		System.out.println("GPS(10) :" + globalPosition.take(10));
		
		Tuple2<Double, Double> reducedGlobalMinPosition = globalPosition.reduce(new Function2<Tuple2<Double,Double>, Tuple2<Double,Double>, Tuple2<Double,Double>>() {
			
			@Override
			public Tuple2<Double, Double> call(Tuple2<Double, Double> value1,
					Tuple2<Double, Double> value2) throws Exception {
				return new Tuple2<Double, Double>(Math.min(value1._1, value2._1), Math.min(value1._2, value2._2));
			}
		});
		
		System.out.println("Min position : " + reducedGlobalMinPosition.toString());

		
		Tuple2<Double, Double> reducedGlobalMaxPosition = globalPosition.reduce(new Function2<Tuple2<Double,Double>, Tuple2<Double,Double>, Tuple2<Double,Double>>() {
			
			@Override
			public Tuple2<Double, Double> call(Tuple2<Double, Double> value1,
					Tuple2<Double, Double> value2) throws Exception {
				return new Tuple2<Double, Double>(Math.max(value1._1, value2._1), Math.max(value1._2, value2._2));
			}
		});
		
		System.out.println("Max position : " + reducedGlobalMaxPosition.toString());
		
		String gpsCoordinatesJson = new Gson().toJson(filteredRowMapDataLst
				.map(x -> new GPScoordinates(Double.parseDouble(x.get("Latitude")), Double
						.parseDouble(x.get("Longitude")))).take(MAX_COORDINATES));
		
		try(PrintWriter out = new PrintWriter(GPS_OUT_DATA_JSON)){
		    out.println( gpsCoordinatesJson );
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		
		Map<String, Long> groupByOffense = dataWoHeader.map(x -> x.split(",")).map(x -> {
			HashMap<String, String> rowMapData = new HashMap<>();
			for (int i=0; i<headerArr.length; i++){
				rowMapData.put(headerArr[i],x[i]);
			}
			return rowMapData;
		}).map(x -> x.get("OccurrenceYear")).countByValue();
		
		System.out.println("OccurrenceYear " + groupByOffense);
		
		GenUtil.printSeperator();
		
	    spark.stop();
	    spark.close();
		
	}
	
}