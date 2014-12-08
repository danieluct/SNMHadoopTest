package com.durda.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class ProductDBWritable implements Writable, DBWritable {

	private int id;
	private String name;
	private String category;
	private double price;

	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		name = Text.readString(in);
		category = Text.readString(in);
		price = in.readDouble();
	}

	public void readFields(ResultSet rs) throws SQLException {
		id = rs.getInt(1);
		name = rs.getString(2);
		category = rs.getString(3);
		price = rs.getDouble(4);
	}

	public void write(PreparedStatement arg0) throws SQLException {
		// TODO Auto-generated method stub

	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		Text.writeString(out, name);
		Text.writeString(out, category);
		out.writeDouble(price);

	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		return sb.append(id).append("þ").append(name).append("þ")
				.append(category).append("þ").append(price).toString();
	}

}
