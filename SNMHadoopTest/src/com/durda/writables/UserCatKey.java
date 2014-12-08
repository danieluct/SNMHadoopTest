package com.durda.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class UserCatKey implements Writable, WritableComparable<UserCatKey> {

	private IntWritable userId;
	private String category;

	public UserCatKey() {
		userId = new IntWritable();
		category = "";
	}

	public UserCatKey(int userId, String cat) {
		super();
		this.userId.set(userId);
		this.category = cat;
	}

	public int compareTo(UserCatKey o) {
		int compareVal = this.userId.compareTo(o.userId);

		if (compareVal == 0)
			return this.category.compareTo(o.category);

		return compareVal;
	}

	public void readFields(DataInput in) throws IOException {
		userId.readFields(in);
		category = Text.readString(in);
	}

	public void write(DataOutput out) throws IOException {
		userId.write(out);
		Text.writeString(out, category);
	}

	public IntWritable getUserId() {
		return userId;
	}

	public String getCategory() {
		return category;
	}

	public void setUserId(int userId) {
		this.userId.set(userId);
	}

	public void set(int userId, String category) {
		this.userId.set(userId);
		this.category = category;
	}

}
