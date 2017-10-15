package com.yelp.analysis1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritable implements Writable,WritableComparable<CompositeKeyWritable> {

	
	private String city;
	private String hood;
	
	
	
	public CompositeKeyWritable(){
		
	}
	
	
	public CompositeKeyWritable(String d,String l){
		this.city=d;
		this.hood=l;
		
		
	}
	
	public int compareTo(CompositeKeyWritable o) {
		int result =city.compareTo(o.city);
		if (result==0){
			result=hood.compareTo(o.hood);
		}
		
		return result;
		
	}

	public void write(DataOutput d) throws IOException {
		WritableUtils.writeString(d, city);
		WritableUtils.writeString(d, hood);
		
	}

	public void readFields(DataInput di) throws IOException {
		city=WritableUtils.readString(di);
	hood=	WritableUtils.readString(di);
		
	}


	
	public String getCity() {
		return city;
	}


	public void setCity(String city) {
		this.city = city;
	}


	public String getHood() {
		return hood;
	}


	public void setHood(String hood) {
		this.hood = hood;
	}


	public String toString(){
		return (new StringBuilder().append(city).append("\t").append(hood).toString());
	}
	
	
	

}
