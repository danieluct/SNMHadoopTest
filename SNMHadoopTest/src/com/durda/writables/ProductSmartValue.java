package com.durda.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ProductSmartValue implements Writable {

	private String firstVal;
	private double secndVal;

	public ProductSmartValue() {
		firstVal = "";
		secndVal = 0.0;
	}

	public ProductSmartValue(String a, double b) {
		super();
		firstVal = a;
		secndVal = b;
	}

	public void readFields(DataInput in) throws IOException {
		firstVal = Text.readString(in);
		secndVal = in.readDouble();

	}

	public void write(DataOutput out) throws IOException {
		Text.writeString(out, firstVal);
		out.writeDouble(secndVal);

	}

	public String getFirstVal() {
		return firstVal;
	}

	public void setFirstVal(String firstVal) {
		this.firstVal = firstVal;
	}

	public double getSecndVal() {
		return secndVal;
	}

	public void setSecndVal(double secndVal) {
		this.secndVal = secndVal;
	}

	public void set(String a, double b) {
		this.firstVal = a;
		this.secndVal = b;
	}

}
