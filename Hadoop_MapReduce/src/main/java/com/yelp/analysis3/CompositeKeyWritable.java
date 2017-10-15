package com.yelp.analysis3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritable implements Writable,WritableComparable<CompositeKeyWritable> {


	private String city;
	private String crusine;



	public CompositeKeyWritable(){

	}


	public CompositeKeyWritable(String d,String l){
		this.city=d;
		this.crusine=l;


	}

	public int compareTo(CompositeKeyWritable o) {
		int result =city.compareTo(o.city);
		if (result==0){
			result=crusine.compareTo(o.crusine);
		}

		return result;

	}

	public void write(DataOutput d) throws IOException {
		WritableUtils.writeString(d, city);
		WritableUtils.writeString(d, crusine);
	}

	public void readFields(DataInput di) throws IOException {
		city=WritableUtils.readString(di);
		crusine=	WritableUtils.readString(di);

	}



	public String getCity() {
		return city;
	}


	public void setCity(String city) {
		this.city = city;
	}




	public String getCrusine() {
		return crusine;
	}


	public void setCrusine(String crusine) {
		this.crusine = crusine;
	}


	public String toString(){
		return (new StringBuilder().append(city).append("\t").append(crusine).toString());
	}




}
