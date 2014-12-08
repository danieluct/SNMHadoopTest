package com.durda.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class UserCatValue implements Writable {

	private int total;
	private double[] qrts = { 0.0, 0.0, 0.0, 0.0 };

	public UserCatValue() {
		total = 0;
	}

	public void readFields(DataInput in) throws IOException {
		total = in.readInt();
		for (int i = 0; i < 4; i++) {
			qrts[i] = in.readDouble();
		}

	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(total);
		for (int i = 0; i < 4; i++) {
			out.writeDouble(qrts[i]);
		}
	}

	public void set(int total, double[] qrts) {
		this.total = total;
		for (int i = 0; i < 4; i++) {
			this.qrts[i] = qrts[i];
		}

	}

	public int getTotal() {
		return total;
	}

	public void setTotal(int total) {
		this.total = total;
	}

	public double getQrt(int i) {
		return qrts[i];
	}

	public void setQrt(int i, double qrt) {
		qrts[i] = qrt;
	}

}
