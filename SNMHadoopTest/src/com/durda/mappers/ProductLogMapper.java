package com.durda.mappers;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.durda.writables.ProductSmartKey;
import com.durda.writables.ProductSmartValue;

public class ProductLogMapper extends
		Mapper<LongWritable, Text, ProductSmartKey, ProductSmartValue> {

	private static final SimpleDateFormat format = new SimpleDateFormat(
			"yyyy-MM-dd");

	private static String getQuarter(final Calendar date) {
		// could have used comparison, but I've chosen to list months for
		// clarity
		switch (date.get(Calendar.MONTH)) {
		case Calendar.JANUARY:
		case Calendar.FEBRUARY:
		case Calendar.MARCH:
			return "q1";
		case Calendar.APRIL:
		case Calendar.MAY:
		case Calendar.JUNE:
			return "q2";
		case Calendar.JULY:
		case Calendar.AUGUST:
		case Calendar.SEPTEMBER:
			return "q3";
		case Calendar.OCTOBER:
		case Calendar.NOVEMBER:
		case Calendar.DECEMBER:
			return "q4";
		default:
			return "q4";
		}
	}

	private ProductSmartKey psK = new ProductSmartKey();
	private ProductSmartValue psV = new ProductSmartValue();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] elems = value.toString().split("þ");

		// something's wrong, ignore line
		if (elems.length != 6) {
			return;
		}

		try {
			// get interesting fields
			Calendar cal = Calendar.getInstance();
			cal.setTime(format.parse(elems[0]));
			int prodId = Integer.parseInt(elems[2]);
			int userId = Integer.parseInt(elems[3]);
			int quantity = Integer.parseInt(elems[5]);

			String quarter = "q2";//getQuarter(cal);

			psK.set(prodId, userId);
			psV.set(quarter, quantity);
			context.write(psK, psV);

		}
		// ignore table header
		catch (ParseException e) {
			return;
		}
	}

}
