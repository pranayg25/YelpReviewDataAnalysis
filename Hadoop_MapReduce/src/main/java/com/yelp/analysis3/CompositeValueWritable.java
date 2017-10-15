package com.yelp.analysis3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeValueWritable implements Writable,WritableComparable<CompositeValueWritable> {


	private String city;
	private String count;



	public CompositeValueWritable(){

	}


	public CompositeValueWritable(String d,String l){
		this.city=d;
		this.count=l;


	}

	public int compareTo(CompositeValueWritable o) {
		int result =city.compareTo(o.city);
		if (result==0){
			result=count.compareTo(o.count);
		}

		return result;

	}

	public void write(DataOutput d) throws IOException {
		WritableUtils.writeString(d, city);
		WritableUtils.writeString(d, count);
	}

	public void readFields(DataInput di) throws IOException {
		//try {
			city= WritableUtils.readString(di);
			count=	WritableUtils.readString(di);						
		/*} catch (IOException e) {
			System.out.println("wooow");
		}*/

	}



	public String getCity() {
		return city;
	}


	public void setCity(String city) {
		this.city = city;
	}

	public String getCount() {
		return count;
	}


	public void setCount(String count) {
		this.count = count;
	}


	public String toString(){
		return (new StringBuilder().append(city).append("\t").append(count).toString());
	}




}
