package com.ragu.spark.demo.main;

import java.io.Serializable;

public class GPScoordinates implements Serializable {
	public double lat;
	public double lng;

	public GPScoordinates(double lat, double lng) {
		super();
		this.lat = lat;
		this.lng = lng;
	}

	@Override
	public String toString() {
		return "lat : " + lat + ", long :" + lng;
	}
}
