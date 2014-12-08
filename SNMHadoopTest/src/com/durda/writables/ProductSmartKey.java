package com.durda.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class ProductSmartKey implements Writable,
		WritableComparable<ProductSmartKey> {

	private IntWritable id;
	private int userId;

	public ProductSmartKey() {
		id = new IntWritable();
		userId = 0;
	}

	public ProductSmartKey(int id, int userId) {
		super();
		this.id.set(id);
		this.userId = userId;
	}

	public int compareTo(ProductSmartKey o) {
		int compareVal = this.id.compareTo(o.id);

		if (compareVal == 0)
			return Double.compare(userId, o.getUserId());

		return compareVal;
	}

	public void readFields(DataInput in) throws IOException {
		id.readFields(in);
		userId = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		id.write(out);
		out.writeInt(userId);
	}

	public IntWritable getId() {
		return id;
	}

	public void setId(int id) {
		this.id.set(id);
	}

	public int getUserId() {
		return userId;
	}

	public void setUserId(int userId) {
		this.userId = userId;
	}

	public void set(int id, int userId) {
		this.id.set(id);
		this.userId = userId;
	}

}
