# sparkDemo1
Apache spark demo

This is a Apache Spark demo project loading New york crime data and extract some information after filtering with some conditions. 

To run this project spark should be installed in the local machine along with Java 1.7+


# To execute the project 
cd to Apache_spark_demo1 directory 
mvn clean package
spark-submit --class com.ragu.spark.demo.main.CrimeDataAnalysisApp --master local[2] target\Apache_spark_demo1-0.0.1-SNAPSHOT.jar

# Verifying the output
Other than various SYSOUT, GPS locations of filtered crime details are extracted and saved in the format of json under the file name src\main\resources\html\gpsCoordinates.json

Above data will be used in the dataplot.html for visualizing the same. 

Open the HTML in a browser to see the output. 

Note : Google map might take approximately 10 seconds to plot the points.    