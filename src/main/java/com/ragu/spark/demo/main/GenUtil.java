package com.ragu.spark.demo.main;

public final class GenUtil {
	
	static public String[] splitBySeperator(String line){
		return line.replace(" ", "_").replace("/", "_").split(",");
	}
	
	static public void printSeperator(){
		System.out.println("*****************************************************");
		System.out.println();
		System.out.println();
		System.out.println("*****************************************************");
	}

}
